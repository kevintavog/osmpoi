package osmpoi.indexer

import de.topobyte.osm4j.geometry.GeometryBuilder
import de.topobyte.osm4j.geometry.MissingEntitiesStrategy
import mu.KotlinLogging
import osmpoi.models.GeoPoint
import osmpoi.models.OsmItem
import java.io.File

private val logger = KotlinLogging.logger {}

class RelationBuilder(private val item: OsmItem, private val itemLookup: OsmItemLookup) {
    companion object {
        const val missingWaysFilename = "MissingWays.log"
        const val noWaysFilename = "NoWays.log"
        const val ignoredDataFilename = "IgnoredData.log"
        const val patchingFilename = "PatchingWay.log"
        const val noConnectedWay = "NoConnectedWay.log"
        var missingWaysCount = 0L
        var noWaysCount = 0L
    }
    val isValid: Boolean
    val location: String
    var isMissingWays = false
    lateinit var center: GeoPoint
    var area: Double

    private val polygons = mutableListOf<String>()
    private val remainingWays = mutableListOf<Pair<OsmItem, MutableList<OsmItem>>>()
    private var debug = false
    private val missingWaysList = mutableListOf<Long>()
    private var adminCenter: OsmItem? = null

    init {
        if (initialize()) {
            if ((item.wayIds?.size ?: 0) > 0) {
                isValid = true
                location = wkt()
                val osm = OsmItemRepository.getRelation(item.toLongId())
                val builder = GeometryBuilder()
                builder.missingEntitiesStrategy = MissingEntitiesStrategy.BUILD_PARTIAL
                val geometry = builder.build(osm, OsmItemRepository)
                area = geometry.envelope.area

                // Migrate a few valuable tags from the admin center to the relation to better match cities
                adminCenter?.let {
                    center = GeoPoint(it.latitude!!, it.longitude!!)
                } ?: run {
                    val bounds = geometry.envelopeInternal
                    center = GeoPoint(bounds.minX + (bounds.maxX - bounds.minX) / 2,
                        bounds.minY + (bounds.maxY - bounds.minY) / 2)
                }
            } else {
                isValid = false
                location = ""
                center = GeoPoint()
                area = 0.0
            }
        } else {
            isValid = false
            location = ""
            center = GeoPoint()
            area = 0.0
        }
    }

    // Return Well Known Text (WKT), a text description for shapes, for the given relation.
    // A relation comprises nodes, ways & relations - 0 to many.
    // Ways can be in any order; and nodes in a way can be either clockwise or counter-clockwise
    // Ways are stitched together in proper order by matching ends
    //
    // NOTE: This code ignores nodes & relations
    private fun wkt(): String {
debug = listOf("relation/62149").contains(item.id)
if (debug) { println("resolving ${item.id} ${item.name}, ${item.rolesSidList} (${item.nodeIds?.size} nodes, ${item.wayIds?.size} ways, ${item.relationIds?.size} relations)") }

        if (0 == (item.wayIds?.size ?: 0)) {
            noWaysCount += 1
            File(noWaysFilename).appendText(
                "${item.id}: ${item.name} ${item.tags} ways=${item.wayIds} relations=${item.relationIds} " +
                "nodes=${item.nodeIds}\n")
            return ""
        }

        while (remainingWays.isNotEmpty()) {
            val pair = remainingWays.first()
            remainingWays.remove(pair)
            val way = pair.first
            val nodes = pair.second
if (debug) { println(" handling ${way.id}") }

            while (nodes.first().id != nodes.last().id) {
                val first = nodes.first()
                val last = nodes.last()
//println("Starting with ${nodes.size}, f=${first.id} l=${last.id}")
                val connectedPair = findConnected(first, last)
                val connectedWay = connectedPair.first
                val connectedNodes = connectedPair.second
                if (connectedNodes.isEmpty()) {
                    // multipolygon & boundary are MULTIPOLYGON
                    //  They require that the ways connect to each other, which means a missing one is a problem
                    val type = item.tags?.get("type") ?: ""
                    if (type == "multipolygon" || type == "boundary") {
                        // Force a closed polygon
                        File(patchingFilename).appendText(
                            "${item.id} ${item.name} way=${way.id} type=$type\n")
                        if (nodes.first().id != nodes.last().id) {
                            nodes.add(nodes.first())
                        }
                        continue
//                        if (isMissingWays) {
//                            missingWaysCount += 1
//                            File(missingWaysFilename).appendText(
//                                "${item.id}: ${item.name} ${item.tags} type=$type currentWay=${way.id} f=${first.id} " +
//                                    "l=${last.id} missing=$missingWaysList ways=${item.wayIds} " +
//                                    "found=${remainingWays.map { it.first.id}}\n")
//if (debug) { println("-> Cannot find connected way for way=${way.id} missing=$missingWaysList (ignoring data)") }
//if (debug) { println("Current WKT: (${nodes.joinToString(",") { "${it.longitude} ${it.latitude}" }})") }
//                            break
//                        }
//                        println("ERROR - unable to find connected way for ${item.id} ${way.id}, ${first.id} & ${last.id}")
//                        return ""
                    }

                    // The remaining types are LINESTRING, though there's uncertainty in that some relations
                    // don't specify what their shape is
                    if (remainingWays.isNotEmpty()) {
                        // TODO: Does this mean this is a MULTILINESTRING?
                        File(noConnectedWay).appendText(
                            "${item.id} ${item.name} ${way.id}, ${first.id} & ${last.id}; type=$type\n")
                        return ""
                    }
if (type == "multilinestring") { logger.warn { "${item.id} ${item.name}, type='$type' (using a linestring)" } }
                    return "LINESTRING(${nodes.joinToString(",") { "${it.longitude} ${it.latitude}" }})"
                }
                val newFirst = connectedNodes.first()
                val newLast = connectedNodes.last()
                if (first.id == newLast.id) {
if (debug) { println(" -> inserted ${connectedWay.id} at the start") }
                    nodes.addAll(0, connectedNodes.dropLast(1))
                } else if (last.id == newFirst.id) {
if (debug) { println(" -> added ${connectedWay.id} to the end") }
                    nodes.addAll(connectedNodes.drop(1))
                } else if (first.id == newFirst.id && last.id == newLast.id) {
if (debug) { println(" -> added ${connectedWay.id} to the end and it's fully connected") }
                    nodes.addAll(connectedNodes.dropLast(1).reversed())
                } else if (last.id == newLast.id) {
if (debug) { println(" -> added a reversed ${connectedWay.id} to the end (ends are equal)") }
                    nodes.addAll(connectedNodes.dropLast(1).reversed())
                } else if (first.id == newFirst.id) {
if (debug) { println(" -> inserted a reversed ${connectedWay.id} at the start (starts are equal)") }
                    nodes.addAll(0, connectedNodes.drop(1).reversed())
                } else {
                    logger.error { "UNHANDLED combine for ${item.id} (${item.name}) - ${way.id}, points (${nodes.size}: ${first.id} & ${last.id}) and way $connectedWay (${connectedNodes.size}: ${newFirst.id} & ${newLast.id})" }
                }
//println("ended with ${nodes.size} nodes, f=${nodes.first().id} l=${nodes.last().id}")
            }

            if (nodes.first().id == nodes.last().id) {
                addPolygon(nodes, item, way)
            } else {
                File(ignoredDataFilename).appendText(
                    "${item.id}: ${item.name} throwing data away, node list does not connect " +
                            "size=${nodes.size} first=${nodes.firstOrNull()} last=${nodes.lastOrNull()}\n")
            }
        }

        if (polygons.isEmpty()) { return "" }
        return "MULTIPOLYGON(${polygons.joinToString(",")})"
    }

    private fun findConnected(first: OsmItem, last: OsmItem): Pair<OsmItem,List<OsmItem>> {
//if (debug) { println("findConnected: ${first.id},${last.id} ${remainingWays.map { "${it.first.id}, f=${it.second.first().id}, l=${it.second.last().id}" }}") }
        for (pair in remainingWays) {
            val nodes = pair.second
//if (debug) { println(" -> Checking ${pair.first.id} (${nodes.first().id}, ${nodes.last().id})") }
            if (nodes.first().id == first.id || nodes.first().id == last.id ||
                    nodes.last().id == first.id || nodes.last().id == last.id) {
                remainingWays.remove(pair)
//if (debug) { println(" -> Matched ${pair.first.id}") }
                return pair
            }
        }

        return Pair(OsmItem(""), emptyList())
    }

    private fun addPolygon(list: List<OsmItem>, item: OsmItem, way: OsmItem) {
        if (list.size < 3) {
            logger.warn { "WARNING: ${item.id} way ${way.id} has too few points: ${list.size}, SKIPPING" }
        } else {
            polygons.add("((${list.joinToString(",") { "${it.longitude} ${it.latitude}" }}))")
        }
    }

    private fun initialize(): Boolean {
        if (item.relationIds != null && item.relationIds.isNotEmpty()) {
            if (item.wayIds == null || item.wayIds.isEmpty()) {
                logger.info { "IGNORING relation list for ${item.id} ${item.name} has relations, but no ways: ${item.relationIds}" }
            }
            // At least in Hungary, a spot check indicates the relations are redundant
//            println("IGNORING relation list for ${item.id}: ${item.relationIds}")
        }

        item.wayIds?.let { wayIdList ->
            wayIdList.forEach { id ->
                itemLookup.retrieve(OsmItem.wayId(id))?.let { wayItem ->
                    if (wayItem.relationIds != null && wayItem.relationIds.isNotEmpty()) {
                        logger.warn { "WARNING: way ${wayItem.id} has relations -> ${wayItem.relationIds}" }
                    }
                    if (wayItem.wayIds != null && wayItem.wayIds.isNotEmpty()) {
                        logger.warn { "WARNING: way ${wayItem.id} has ways -> ${wayItem.wayIds}" }
                    }

                    val nodeList = mutableListOf<OsmItem>()
                    wayItem.nodeIds?.let { nodeIdList ->
                        nodeIdList.forEach { nodeId ->
                            itemLookup.retrieve(OsmItem.nodeId(nodeId))?.let {
                                nodeList.add(it)
                            } ?: run {
                                logger.warn { "WARNING: Unable to find node $nodeId" }
                            }
                        }
                    }
                    remainingWays.add(Pair(wayItem, nodeList))
                } ?: run {
                    missingWaysList.add(id)
                }
            }

            // If we fail getting a way, we can't resolve this relation
            if (wayIdList.size != remainingWays.size) {
                isMissingWays = true
            }
        }

        item.adminCenterNodeId?.let { nodeId ->
            itemLookup.retrieve(OsmItem.nodeId(nodeId))?.let { nodeItem ->
                adminCenter = nodeItem
            } ?: run {
println("Missing admin center node id: $nodeId")
            }
        }

        return true
    }
}
