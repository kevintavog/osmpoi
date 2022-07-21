package osmpoi.indexer

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
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

class CountryIndexerMain {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            try {
                val indexer = CountryIndexer()
                indexer.main(args)
                exitProcess(indexer.errorCode)
            } catch (t: Throwable) {
                t.printStackTrace()
                exitProcess(-1)
            }
        }
    }
}

class CountryIndexer: CliktCommand() {
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
    private val inputName: String by option(
        "-p",
        "--pbfFolder",
        help = "The folder with the OSM PBF file(s) to parse, or a PBF file")
        .required()
    private val rocksDbPath: String by option(
        "-r",
        "--rocksDBPath",
        help = "The RocksDB path. If not given, an in-memory store is used.")
        .required()
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

        val inputFolder = File(inputName)
        if (!inputFolder.exists()) {
            logger.error { "No such folder/file: $inputName" }
            errorCode = -1
            return
        }
        if (!inputFolder.isFile) {
            logger.info { "Processing PBF files in $inputName" }
        }

        inputFolder.walk().sortedBy { it.name.lowercase() }.forEach { file ->
            if (file.isFile && file.extension == "pbf") {
                logger.info { "Parsing ${file.name} "}
                OsmItemRepository.initialize(rocksDbPath, false)
                val countryRelations = mutableListOf<OsmItem>()
                BlockInputStream(File(file.absolutePath).inputStream(), CountryParser(countryRelations)).process()
                val dependentWays = countryRelations
                    .mapNotNull { it.wayIds }
                    .flatMap { it.asIterable() }
                    .toSet()
println("There are ${dependentWays.size} dependent ways")
                val nodes = mutableSetOf<Long>()
                val wayItems = mutableMapOf<String, OsmItem>()
                BlockInputStream(File(file.absolutePath).inputStream(), DependentWayParser(dependentWays, wayItems, nodes)).process()
println("There are ${nodes.size} needed nodes")
                val nodeItems = mutableMapOf<String, OsmItem>()
                BlockInputStream(File(file.absolutePath).inputStream(), DependentNodeParser(nodes, nodeItems)).process()
println("Found ${nodeItems.size} of the ${nodes.size} needed nodes")
                val lookup = CountryOsmItemLookup(wayItems, nodeItems)
                countryRelations.forEach { countryItem ->
                    val builder = RelationBuilder(countryItem, lookup)
println("${countryItem.id} ${countryItem.name} valid=${builder.isValid}")
                }
            }
        }
    }
}

class CountryOsmItemLookup(val ways: Map<String, OsmItem>, val nodes: Map<String, OsmItem>): OsmItemLookup {
    override fun retrieve(id: String): OsmItem? {
        if (ways.containsKey(id)) {
            return ways[id]
        }
        return nodes[id]
    }
}

class CountryParser(val countries: MutableList<OsmItem>): BasePbfParser() {
    override fun complete() {
    }

    override fun parse(header: Osmformat.HeaderBlock?) {
    }

    override fun parseRelations(rels: MutableList<Osmformat.Relation>?) {
        rels?.let { relationsList ->
            relationsList.forEach { relation ->
                val id = OsmItem.relationId(relation.id)
                val allTags = getTags(relation.keysList, relation.valsList)
                if (allTags["admin_level"] == "2") {
                    val name = OsmTags.nameFromTags(allTags)
                    val filteredPair = OsmTags.filterTags(allTags, OsmItemType.RELATION)
                    val nodeIds = mutableListOf<Long>()
                    val wayIds = mutableListOf<Long>()
                    val relationIds = mutableListOf<Long>()
                    var lastMemberId = 0L
                    for (memIndex in relation.memidsList.indices) {
                        lastMemberId += relation.memidsList[memIndex]
                        when (relation.typesList[memIndex]) {
                            Osmformat.Relation.MemberType.NODE -> {
                                nodeIds.add(lastMemberId)
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
                    countries.add(OsmItem(
                            id = id,
                            poiLevel = filteredPair.second,
                            name = name.ifEmpty { null },
                            tags = filteredPair.first.ifEmpty { null },
                            nodeIds = nodeIds,
                            wayIds = wayIds,
                            relationIds = relationIds,
                            rolesSidList = relation.rolesSidList.map { getStringById(it) }
                        )
                    )

                    val last = countries.last()
println("Country! ${last.id} ${last.name} relations=${relationIds.size} ways=${wayIds.size} nodes=${nodeIds.size} tags=${last.tags}")
                }
            }
        }
    }

    override fun parseDense(nodes: Osmformat.DenseNodes?) {
    }

    override fun parseNodes(nodes: MutableList<Osmformat.Node>?) {
    }

    override fun parseWays(ways: MutableList<Osmformat.Way>?) {
    }
}

class DependentWayParser(val dependentWays: Set<Long>, val ways: MutableMap<String, OsmItem>, val nodes: MutableSet<Long>): BasePbfParser() {
    override fun complete() {
    }

    override fun parse(header: Osmformat.HeaderBlock?) {
    }

    override fun parseRelations(rels: MutableList<Osmformat.Relation>?) {
    }

    override fun parseDense(nodes: Osmformat.DenseNodes?) {
    }

    override fun parseNodes(nodes: MutableList<Osmformat.Node>?) {
    }

    override fun parseWays(incomingWays: MutableList<Osmformat.Way>?) {
        incomingWays?.let { wayList ->
            wayList.forEach { way ->
                if (dependentWays.contains(way.id)) {
                    val nodeIds = mutableListOf<Long>()
                    var lastNodeId = 0L
                    for (refIndex in way.refsList.indices) {
                        lastNodeId += way.refsList[refIndex]
                        nodeIds.add(lastNodeId)
                    }
                    nodes.addAll(nodeIds)
                    val id = OsmItem.wayId(way.id)
                    ways[id] = OsmItem(
                        id = id,
                        nodeIds = nodeIds
                    )
                }
            }
        }
    }
}

class DependentNodeParser(val dependentNodes: Set<Long>, val items: MutableMap<String, OsmItem>): BasePbfParser() {
    override fun complete() {
    }

    override fun parse(header: Osmformat.HeaderBlock?) {
    }

    override fun parseRelations(rels: MutableList<Osmformat.Relation>?) {
    }

    override fun parseDense(nodes: Osmformat.DenseNodes?) {
        nodes?.let { dn ->
            var lastId = 0L
            var lastLat = 0L
            var lastLon = 0L
            for (index in 0 until dn.idCount) {
                lastId += dn.getId(index)
                lastLat += dn.getLat(index)
                lastLon += dn.getLon(index)
                if (dependentNodes.contains(lastId)) {
                    val id = OsmItem.nodeId(lastId)
                    items[id] = OsmItem(
                        id = id,
                        latitude = parseLat(lastLat),
                        longitude = parseLon(lastLon)
                    )
                }
            }
        }
    }

    override fun parseNodes(nodes: MutableList<Osmformat.Node>?) {
    }

    override fun parseWays(ways: MutableList<Osmformat.Way>?) {
    }
}

