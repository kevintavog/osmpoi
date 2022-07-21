package osmpoi.indexer

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import crosby.binary.Osmformat
import crosby.binary.Osmformat.Relation.MemberType
import crosby.binary.file.BlockInputStream
import osmpoi.core.Elastic
import osmpoi.models.OsmItem
import java.io.File
import kotlin.system.exitProcess
import mu.KotlinLogging
import osmpoi.models.OsmItemType

private val logger = KotlinLogging.logger {}

class IndexPbfMain {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            try {
                IndexerPbf().main(args)
                exitProcess(0)
            } catch (t: Throwable) {
                t.printStackTrace()
                exitProcess(-1)
            }
        }
    }
}

class IndexerPbf: CliktCommand() {
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
    private val inputFolder: String by option(
        "-p",
        "--pbfFolder",
        help = "The folder with the OSM PBF file(s) to parse")
        .required()
    private val rocksDbPath: String? by option(
        "-r",
        "--rocksDBPath",
        help = "The RocksDB path. If not given, an in-memory store is used.")

    override fun run() {
        Elastic.url = elasticUrl
        Elastic.socketTimeout = 3 * 60000
        Elastic.initIndex()

        logger.info { "Processing PBF files in $inputFolder" }
        rocksDbPath?.let {
            logger.info { " -> Using Rocks DB at $it" }
        }

        File(OsmItemRepository.failedIdFilename).delete()
        File(ResolveNotable.indexFailuresFilename).delete()
        File(ResolveNotable.skippedPoisFilename).delete()
        File(ResolveNotable.badGeometriesFilename).delete()
        File(RelationBuilder.missingWaysFilename).delete()
        File(RelationBuilder.noWaysFilename).delete()
        File(RelationBuilder.ignoredDataFilename).delete()

        File(inputFolder).walk().sortedBy { it.name.lowercase() }.forEach { file ->
            if (file.isFile && file.extension == "pbf") {
                OsmItemRepository.initialize(rocksDbPath)
                logger.info { "Parsing OSM items in ${file.name}" }
                BlockInputStream(File(file.absolutePath).inputStream(), OsmPbfParser()).process()
                logger.info { "Resolving & storing OSM items" }
                ResolveNotable().run(batchSize)
                OsmItemRepository.close()
            }
        }
    }
}

class OsmPbfParser: BasePbfParser() {
    private var totalRelations = 0L
    private var notableRelations = 0L
    private var ordinaryRelations = 0L

    private var totalWays = 0L
    private var notableWays = 0L
    private var ordinaryWays = 0L

    private var totalNodes = 0L
    private var notableNodes = 0L
    private var ordinaryNodes = 0L

    private var leftBound: Double = 0.0
    private var rightBound: Double = 0.0
    private var topBound: Double = 0.0
    private var bottomBound: Double = 0.0


    override fun complete() {
        OsmItemRepository.flush()
//        logger.info { "Input bounding box: $leftBound,$bottomBound,$rightBound,$topBound" }
        logger.info { "Found ${notableRelations + notableWays + notableNodes}/${ordinaryRelations + ordinaryWays + ordinaryNodes} notable/ordinary items " +
                "($notableRelations/$ordinaryRelations relations, $notableWays/$ordinaryWays ways " +
                "& $notableNodes/$ordinaryNodes nodes). " +
                "Parsed $totalRelations relations, $totalWays ways, $totalNodes nodes for ${totalRelations + totalWays + totalNodes} items" }
    }

    override fun parse(header: Osmformat.HeaderBlock?) {
        header?.bbox?.let {
            leftBound = it.left * .000000001
            rightBound = it.right * .000000001
            topBound = it.top * .000000001
            bottomBound = it.bottom * .000000001
        }
    }

    override fun parseDense(nodes: Osmformat.DenseNodes?) {
        val items = mutableListOf<OsmItem>()
        val retained = mutableListOf<String>()
        nodes?.let { dn ->
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
                if (filteredPair.second == PoiLevel.NOTABLE && name.isNotEmpty()) {
                    notableNodes += 1
                    retained.add(id)
                } else if (filteredPair.second == PoiLevel.ORDINARY && name.isNotEmpty()) {
                    retained.add(id)
                    ordinaryNodes += 1
                }

                val lat = parseLat(lastLat)
                val lon = parseLon(lastLon)
                items.add(OsmItem(
                    id = id,
                    poiLevel = if (filteredPair.second == PoiLevel.ZERO) null else filteredPair.second,
                    latitude = lat,
                    longitude = lon,
                    name = name.ifEmpty { null },
                    tags = filteredPair.first.ifEmpty { null },
                ))
            }
        }
        OsmItemRepository.storeRetained(retained)
        OsmItemRepository.store(items)
    }

    override fun parseNodes(nodes: MutableList<Osmformat.Node>?) {
        nodes?.let { nodeList ->
            totalNodes += nodeList.size
            if (nodeList.isNotEmpty()) {
                TODO("Handle nodes")
            }
        }
    }

    override fun parseRelations(rels: MutableList<Osmformat.Relation>?) {
        val items = mutableListOf<OsmItem>()
        val retained = mutableListOf<String>()
        rels?.let { relationsList ->
            totalRelations += relationsList.size
            relationsList.forEach { relation ->
                val id = OsmItem.relationId(relation.id)

                val allTags = getTags(relation.keysList, relation.valsList)
                val filteredPair = OsmTags.filterTags(allTags, OsmItemType.RELATION)
                val name = OsmTags.nameFromTags(allTags)
                val isNotable = filteredPair.second == PoiLevel.NOTABLE && name.isNotEmpty()
                if (isNotable) {
                    retained.add(id)
                    notableRelations += 1
//if (listOf("relation/4002410", "relation/10173587").contains(id)) {
//    val roles = relation.rolesSidList.map { getStringById(it) }
//    println("$id -> roles: ${relation.rolesSidList} ($roles)")
//}
                }
                if (filteredPair.second == PoiLevel.ORDINARY) {
                    retained.add(id)
                    ordinaryRelations += 1
                }

                val nodeIds = mutableListOf<Long>()
                val wayIds = mutableListOf<Long>()
                val relationIds = mutableListOf<Long>()
                var lastMemberId = 0L
                for (memIndex in relation.memidsList.indices) {
                    lastMemberId += relation.memidsList[memIndex]
                    when (relation.typesList[memIndex]) {
                        MemberType.NODE -> {
                            nodeIds.add(lastMemberId)
                        }
                        MemberType.WAY -> {
                            wayIds.add(lastMemberId)
                        }
                        MemberType.RELATION -> {
                            relationIds.add(lastMemberId)
                        }
                        null -> { }
                    }
                }
//if (relation.id == 812603L) {
//    println("$id: allTags=$allTags filteredTags=$filteredTags")
//}
                items.add(OsmItem(
                    id = id,
                    poiLevel = if (filteredPair.second == PoiLevel.ZERO) null else filteredPair.second,
                    name = name.ifEmpty { null },
                    tags = filteredPair.first.ifEmpty { null },
                    nodeIds = nodeIds,
                    wayIds = wayIds,
                    relationIds = relationIds,
                    rolesSidList = relation.rolesSidList.map { getStringById(it) }
                ))
            }
        }
        OsmItemRepository.storeRetained(retained)
        OsmItemRepository.store(items)
    }

    override fun parseWays(ways: MutableList<Osmformat.Way>?) {
        val items = mutableListOf<OsmItem>()
        val retained = mutableListOf<String>()
        ways?.let { wayList ->
            totalWays += wayList.size
            wayList.forEach { way ->
                val id = OsmItem.wayId(way.id)
                val allTags = getTags(way.keysList, way.valsList)
                val filteredPair = OsmTags.filterTags(allTags, OsmItemType.WAY)
                val name = OsmTags.nameFromTags(allTags)
                val isNotable = filteredPair.second == PoiLevel.NOTABLE && name.isNotEmpty()
                if (isNotable) {
                    retained.add(id)
                    notableWays += 1
                }
                if (filteredPair.second == PoiLevel.ORDINARY) {
                    retained.add(id)
                    ordinaryWays += 1
                }

                if (way.latList.isNotEmpty()) {
                    TODO("Handle direct lat/lon values in way")
                }

                val nodeIds =  mutableListOf<Long>()
                var lastNodeId = 0L
                for (refIndex in way.refsList.indices) {
                    lastNodeId += way.refsList[refIndex]
                    nodeIds.add(lastNodeId)
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
    }
}
