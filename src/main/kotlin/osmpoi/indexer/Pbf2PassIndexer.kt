package osmpoi.indexer

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import crosby.binary.Osmformat
import crosby.binary.file.BlockInputStream
import mu.KotlinLogging
import osmpoi.core.Elastic
import osmpoi.models.OsmItem
import osmpoi.models.OsmItemType
import java.io.File
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

class Pbf2PassIndexerMain {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            try {
                val indexer = Pbf2PassIndexer()
                indexer.main(args)
                exitProcess(indexer.errorCode)
            } catch (t: Throwable) {
                t.printStackTrace()
                exitProcess(-1)
            }
        }
    }
}

class Pbf2PassIndexer: CliktCommand() {
    private val batchSize: Int by option(
        "-b",
        "--batchSize",
        help = "The batch size used for ElasticSearch")
        .int()
        .default(200)
    private val elasticUrl: String by option(
        "-e",
        "--elasticUrl",
        help = "The URl for ElasticSearch")
        .required()
    private val inputFolderName: String by option(
        "-p",
        "--pbfFolder",
        help = "The folder with the OSM PBF file(s) to parse")
        .required()
    private val item: String? by option(
        "-i",
        "--item",
        help = "The id of the relation (as '1234'")
    private val rocksDbPath: String by option(
        "-r",
        "--rocksDBPath",
        help = "The RocksDB path. If not given, an in-memory store is used.")
        .required()
    private val nodesOnDisk: Boolean by option(
        "-n",
        "--nodesOnDisk",
        help="Store node ids on disk rather than in memory")
        .flag()

    var errorCode = 0

    override fun run() {
        Elastic.url = elasticUrl
        Elastic.socketTimeout = 3 * 60000
        Elastic.initIndex()

        File(OsmItemRepository.failedIdFilename).delete()
        File(ResolveNotable.indexFailuresFilename).delete()
        File(ResolveNotable.skippedPoisFilename).delete()
        File(ResolveNotable.badGeometriesFilename).delete()
        File(RelationBuilder.missingWaysFilename).delete()
        File(RelationBuilder.noWaysFilename).delete()
        File(RelationBuilder.ignoredDataFilename).delete()
        File(RelationBuilder.patchingFilename).delete()
        File(RelationBuilder.noConnectedWay).delete()

        val inputFolder = File(inputFolderName)
        if (!inputFolder.exists()) {
            logger.error { "No such folder/file: $inputFolderName" }
            errorCode = -1
            return
        }
        if (!inputFolder.isFile) {
            logger.info { "Processing PBF files in $inputFolderName" }
        }

        if (nodesOnDisk) { logger.info { "Storing nodes on disk" } }
        inputFolder.walk().sortedBy { it.name.lowercase() }.forEach { file ->
            if (file.isFile && file.extension == "pbf") {
                logger.info { "Parsing ${file.name} "}
                OsmItemRepository.initialize(rocksDbPath, !nodesOnDisk)
                BlockInputStream(File(file.absolutePath).inputStream(), WaysAndRelationsPbfParser(item)).process()
                OsmItemRepository.flush()
                BlockInputStream(File(file.absolutePath).inputStream(), NodesPbfParser()).process()
                OsmItemRepository.flush()
                logger.info { "Resolving & storing OSM items" }
                ResolveNotable().run(batchSize)
                OsmItemRepository.close()
            }
        }
    }
}

// Ignore nodes and keep track of interesting Ways & Relations
// Retain Ways as some of them are needed to resolve relations (but the relations are parsed after ways)
class WaysAndRelationsPbfParser(private val singleItemId: String?): BasePbfParser() {
    private var totalRelations = 0L
    private var retainedRelations = 0L

    private var totalWays = 0L
    private var retainedWays = 0L

    private var dependentNodes = 0L

    override fun parseRelations(rels: MutableList<Osmformat.Relation>?) {
        val items = mutableListOf<OsmItem>()
        val retained = mutableListOf<String>()
        val allNodeIds = mutableListOf<Long>()
        rels?.let { relationsList ->
            totalRelations += relationsList.size
            relationsList.forEach { relation ->
                val id = OsmItem.relationId(relation.id)

                val allTags = getTags(relation.keysList, relation.valsList)
                val filteredPair = OsmTags.filterTags(allTags, OsmItemType.RELATION)
                val name = OsmTags.nameFromTags(allTags)
                var isRetained = false
                singleItemId?.let {
                    if (relation.id == it.toLong()) {
//if (filteredPair.second == PoiLevel.ADMIN) { println("Admin: ${id}: ${filteredPair.first}") }
                        retained.add(id)
                        isRetained = true
                        retainedRelations += 1
                    }
                } ?: run {
                    if (filteredPair.second != PoiLevel.ZERO && name.isNotEmpty()) {
//if (filteredPair.second == PoiLevel.ADMIN) { println("Admin: ${id}: ${filteredPair.first}") }
                        retained.add(id)
                        isRetained = true
                        retainedRelations += 1
                    }
                }

                if (isRetained) {
                    var adminCenterNodeId: Long? = null
                    val nodeIds = mutableListOf<Long>()
                    val wayIds = mutableListOf<Long>()
                    val relationIds = mutableListOf<Long>()
                    var lastMemberId = 0L
                    for (memIndex in relation.memidsList.indices) {
                        lastMemberId += relation.memidsList[memIndex]
                        val role = getStringById(relation.rolesSidList[memIndex])
                        when (relation.typesList[memIndex]) {
                            Osmformat.Relation.MemberType.NODE -> {
                                nodeIds.add(lastMemberId)
                                if (role == "admin_centre") {
                                    adminCenterNodeId = lastMemberId
                                    OsmItemRepository.storeNodeIds(listOf(lastMemberId))
                                }
                            }
                            Osmformat.Relation.MemberType.WAY -> {
                                wayIds.add(lastMemberId)
                            }
                            Osmformat.Relation.MemberType.RELATION -> {
                                relationIds.add(lastMemberId)
                            }
                            null -> {}
                        }
                    }
                    items.add(
                        OsmItem(
                            id = id,
                            poiLevel = filteredPair.second,
                            name = name.ifEmpty { null },
                            tags = filteredPair.first.ifEmpty { null },
                            nodeIds = nodeIds,
                            wayIds = wayIds,
                            relationIds = relationIds,
                            rolesSidList = relation.rolesSidList.map { getStringById(it) },
                            adminCenterNodeId = adminCenterNodeId
                        )
                    )
                    allNodeIds.addAll(nodeIds)
                }
            }
        }
        OsmItemRepository.store(items)
        OsmItemRepository.storeRetained(retained)
        OsmItemRepository.storeNodeIds(allNodeIds)
        dependentNodes += allNodeIds.size
    }

    override fun parseWays(ways: MutableList<Osmformat.Way>?) {
        val items = mutableListOf<OsmItem>()
        val retained = mutableListOf<String>()
        val allNodeIds = mutableListOf<Long>()
        ways?.let { wayList ->
            totalWays += wayList.size
            wayList.forEach { way ->
                val id = OsmItem.wayId(way.id)
                val allTags = getTags(way.keysList, way.valsList)
                val filteredPair = OsmTags.filterTags(allTags, OsmItemType.WAY)
                val name = OsmTags.nameFromTags(allTags)
                var isRetained = false
                if (filteredPair.second != PoiLevel.ZERO && name.isNotEmpty()) {
//if (filteredPair.second == PoiLevel.ADMIN) { println("Admin: ${id}: ${filteredPair.first}") }
                    retained.add(id)
                    retainedWays += 1
                    isRetained = true
                }

                if (way.latList.isNotEmpty()) {
                    TODO("Handle direct lat/lon values in way")
                }

                val nodeIds = mutableListOf<Long>()
                var lastNodeId = 0L
                for (refIndex in way.refsList.indices) {
                    lastNodeId += way.refsList[refIndex]
                    nodeIds.add(lastNodeId)
                }

                if (isRetained && singleItemId == null) {
                    allNodeIds.addAll(nodeIds)
                }

//if (way.id == 4755066L) {
//    logger.info { "Parsing $id, allTags=$allTags filteredTags=$filteredTags" }
//}
                items.add(OsmItem(
                    id = id,
                    poiLevel = if (filteredPair.second == PoiLevel.ZERO) null else filteredPair.second,
                    name = name.ifEmpty { null },
                    tags = filteredPair.first.ifEmpty { null },
                    nodeIds = nodeIds
                ))
            }
        }

        OsmItemRepository.storeRetained(retained)
        OsmItemRepository.store(items)
        OsmItemRepository.storeNodeIds(allNodeIds)
        dependentNodes += allNodeIds.size
    }

    override fun complete() {
        // Walk each way list of each relation and store the node ids in the way
        OsmItemRepository.flush()
        OsmItemRepository.startRetrieveRetained()
        do {
            val ids = OsmItemRepository.nextRetained()
            ids.forEach { id ->
                if (OsmItem.type(id) == OsmItemType.RELATION) {
                    OsmItemRepository.retrieve(id)?.let { item ->
                        item.wayIds?.let { wayIdList ->
                            wayIdList.forEach { wayId ->
                                OsmItemRepository.retrieve(OsmItem.wayId(wayId))?.let { wayItem ->
                                    wayItem.nodeIds?.let { nodeIdList ->
                                        OsmItemRepository.storeNodeIds(nodeIdList)
                                        dependentNodes += nodeIdList.size
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } while (ids.isNotEmpty())

        logger.info { "First pass complete, kept ${retainedRelations} of $totalRelations relations " +
                "and ${retainedWays} of $totalWays ways. There are $dependentNodes dependent nodes."
        }
    }

    override fun parse(header: Osmformat.HeaderBlock?) {
    }

    override fun parseDense(nodes: Osmformat.DenseNodes?) {
    }

    override fun parseNodes(nodes: MutableList<Osmformat.Node>?) {
    }
}

// Only retain nodes that are (1) interesting or (2) a dependent of an interesting way/relation (all nodes for ways
// & relations were stored during the first pass)
class NodesPbfParser: BasePbfParser() {
    private var totalNodes = 0L
    private var retainedNodes = 0L
    private var dependentNodes = 0L

    override fun complete() {
        OsmItemRepository.flush()
        logger.info { "Second pass complete, kept $retainedNodes nodes and " +
            "$dependentNodes dependent nodes of $totalNodes nodes. " }
    }

    override fun parse(header: Osmformat.HeaderBlock?) {
    }

    override fun parseRelations(rels: MutableList<Osmformat.Relation>?) {
    }

    override fun parseDense(nodes: Osmformat.DenseNodes?) {
        val items = mutableListOf<OsmItem>()
        val retained = mutableListOf<String>()
        nodes?.let { dn ->
//logger.info { "Processing ${dn.idCount} dense nodes" }
            totalNodes += dn.idCount
            var lastId = 0L
            var lastLat = 0L
            var lastLon = 0L
            val allTags = getDenseTags(dn.idList, dn.keysValsList)
            for (index in 0 until dn.idCount) {
                lastId += dn.getId(index)
                lastLat += dn.getLat(index)
                lastLon += dn.getLon(index)

                val id = OsmItem.nodeId(lastId)
                val tags = allTags[index]
                val filteredPair = OsmTags.filterTags(tags, OsmItemType.NODE)
                val name = OsmTags.nameFromTags(tags)
                if (filteredPair.second != PoiLevel.ZERO && name.isNotEmpty()) {
                    retainedNodes += 1
                    retained.add(id)
                }

                val lat = parseLat(lastLat)
                val lon = parseLon(lastLon)
                items.add(
                    OsmItem(
                        id = id,
                        poiLevel = filteredPair.second,
                        latitude = lat,
                        longitude = lon,
                        name = name.ifEmpty { null },
                        tags = filteredPair.first.ifEmpty { null },
                    )
                )
            }
        }

        OsmItemRepository.storeRetained(retained)
        OsmItemRepository.store(items.filter { retained.contains(it.id) })

        val allNodeIds = items.map { it.toLongId() }
        val desiredNodeIds = OsmItemRepository.containsNodeIds(allNodeIds)
        val storedNodes = items.filter { desiredNodeIds.contains(it.toLongId()) }
        dependentNodes += storedNodes.size
        OsmItemRepository.store(storedNodes)
    }

    override fun parseNodes(nodes: MutableList<Osmformat.Node>?) {
    }

    override fun parseWays(ways: MutableList<Osmformat.Way>?) {
    }
}
