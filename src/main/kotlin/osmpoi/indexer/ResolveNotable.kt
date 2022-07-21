package osmpoi.indexer

import co.elastic.clients.elasticsearch._types.ErrorCause
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation
import de.topobyte.osm4j.geometry.GeometryBuilder
import de.topobyte.osm4j.geometry.MissingEntitiesStrategy
import mu.KotlinLogging
import org.elasticsearch.client.ResponseException
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.Point
import org.locationtech.jts.geom.Polygon
import osmpoi.core.Elastic
import osmpoi.models.GeoPoint
import osmpoi.models.OsmPoi
import osmpoi.models.OsmTagValue
import java.io.File

private val logger = KotlinLogging.logger {}

class ResolveNotable {
    companion object {
        const val indexFailuresFilename = "IndexFailures.log"
        const val skippedPoisFilename = "SkippedPOIs.log"
        const val badGeometriesFilename = "BadGeometries.log"
    }

    private val poiList = mutableListOf<OsmPoi>()

    private val maxPoiDependencySize = 5000L

    private val totalNumRetries = 2
    private val emitCount = 100_000L
    private var nextEmitLineCount = emitCount
    private var totalPOIsProcessed = 0L
    private var largePOIsSkipped = 0L
    private var badGeometryCount = 0L

    var itemsIndexed = 0L
    var indexErrors = 0L
    var indexExceptions = 0L
    var countRetryAttempts = 0L

    fun run(batchSize: Int) {
        OsmItemRepository.startRetrieveRetained()
        do {
            val ids = OsmItemRepository.nextRetained()
            ids.forEach { id ->
                OsmItemRepository.retrieve(id)?.let { item ->
                    totalPOIsProcessed += 1
                    val totalDependents = (item.relationIds?.size ?: 0) + (item.wayIds?.size ?: 0) + (item.nodeIds?.size ?: 0)
                    if (totalDependents > maxPoiDependencySize) {
                        largePOIsSkipped += 1
                        File(skippedPoisFilename).appendText(
                            "Skipping $id: ${item.name} ${item.tags} ${item.relationIds?.size} relations, " +
                                "${item.wayIds?.size} ways and ${item.nodeIds?.size} nodes\n")
                    } else {
                        var location = ""
                        var point = GeoPoint(item.latitude ?: 0.0, item.longitude ?: 0.0)
                        var area = 0.0
                        var skipItem = false
                        val debug = id == "XXXX relation/3886755"
                        when (item.type) {
                            "node" -> {
                                location = "POINT (${item.longitude} ${item.latitude})"
                            }
                            "way" -> {
                                val osm = OsmItemRepository.getWay(item.toLongId())
                                val geometry = builder().build(osm, OsmItemRepository)
                                location = toWkt(geometry, true, debug)
                                val bounds = geometry.envelopeInternal
                                point = GeoPoint(bounds.minX + (bounds.maxX - bounds.minX) / 2,
                                    bounds.minY + (bounds.maxY - bounds.minY) / 2)
                                area = geometry.envelope.area
                            }
                            "relation" -> {
                                val builder = RelationBuilder(item, OsmItemRepository)
                                skipItem = !builder.isValid
                                location = builder.location
                                point = builder.center
                                area = builder.area
                            }
                        }

                        if (!skipItem) {
                            if (location.isNotEmpty()) {
                                val poi = OsmPoi(
                                    id = item.id,
                                    name = item.name ?: "",
                                    point = point,
                                    location = location,
                                    tags = (item.tags ?: emptyMap()).map { OsmTagValue(it.key, it.value) },
                                    poiLevel = item.poiLevel ?: PoiLevel.ZERO,
                                    area = area
                                )
//if (id.contains("4755066")) {
//    println("$id -> $poi")
//}
                                poiList.add(poi)
                                if (poiList.size >= batchSize) {
                                    indexDocuments(poiList, totalNumRetries)
                                    poiList.clear()
                                }
                            } else {
                                badGeometryCount += 1
                                File(badGeometriesFilename).appendText(
                                    "$id: ${item.name} ${item.tags} relations=${item.relationIds} " +
                                            "ways=${item.wayIds} nodes=${item.nodeIds}\n"
                                )
                            }
                        }
                        emitInfo(false)
                    }
                }
            }
        } while (ids.isNotEmpty())
        indexDocuments(poiList, totalNumRetries)
        emitInfo(true)
    }

    private fun indexDocuments(docs: List<OsmPoi>, numRetries: Int) {
        if (docs.isEmpty()) {
            return
        }

        val req: List<BulkOperation> = docs.map { poi ->
            BulkOperation.of { r ->
                r.index<OsmPoi> { s -> s
                    .index(Elastic.poiIndexName)
                    .id(poi.id)
                    .document(poi)
                }
            }
        }

        try {
            val response = Elastic.client.bulk { b ->
                b.operations(req)
            }
            response.items().forEachIndexed { index, item ->
                item.error()?.let {
                    indexErrors += 1
                    var message = ""
                    var error: ErrorCause? = it
                    while (error != null) {
                        message += "type=${error.type()} reason=${error.reason()}; "
                        error.rootCause().forEach { er ->
                            message += "root-type: ${er.type()} root-reason: ${er.reason()}"
                        }
                        error = error.causedBy()
                    }

                    File(indexFailuresFilename).appendText("$message\n --> ${docs[index]}\n")
                } ?: run {
                    itemsIndexed += 1
                }
            }
        } catch (t: Throwable) {
            File(indexFailuresFilename).appendText("Index exception: $t\n")

            // Elastic exceptions don't have much useful info - but we are not going to retry the entire list.
            // Instead, we will try one item at a time to find which documents are problematic
            if (t is ResponseException) {
                if (docs.size == 1) { throw t }
                logger.warn("Elastic exception, trying one doc at a time")
                for (d in docs) {
                    try {
                        indexDocuments(listOf(d), numRetries - 1)
                    } catch (innerT: Throwable) {
                        indexExceptions += 1
                        // This document cannot be indexed, give up on it
                        indexErrors += 1
                        d.location = ""
                        File(indexFailuresFilename).appendText("${t.message}\n --> $d\n")
                    }
                }
            } else {
                indexExceptions += 1
                if (numRetries > 0) {
                    countRetryAttempts += 1
                    indexDocuments(docs, numRetries - 1)
                } else {
                    throw t
                }
            }
        }
    }

    private fun toWkt(geometry: Geometry, addType: Boolean, debug: Boolean): String {
        if (debug) { logger.debug {"DEBUG -> to WKT $geometry" }}
        when(geometry.geometryType) {
            "GeometryCollection" -> {
                val text = mutableListOf<String>()
                for (index in 0 until  geometry.numGeometries) {
                    text.add(toWkt(geometry.getGeometryN(index), true, debug))
                }
                if (debug) { logger.debug {"returning GEOMETRYCOLLECTION (${text.joinToString(", ")})" } }
                if (debug) { logger.debug {"toText=${geometry.toText()}" } }
                return "${if (addType) "GEOMETRYCOLLECTION" else ""} (${text.joinToString(", ")})"
            }
            "LinearRing" -> {
                if (geometry.coordinates.first().equals2D(geometry.coordinates.last())) {
                    return "${if (addType) "POLYGON" else ""} ((${wktCoordinates(geometry.coordinates)}))"
                }
            }
            "LineString" -> {
                return "${if (addType) "LINESTRING" else ""} (${wktCoordinates(geometry.coordinates)})"
            }
            "MultiLineString" -> {
                val text = mutableListOf<String>()
                for (index in 0 until  geometry.numGeometries) {
                    val inner = geometry.getGeometryN(index)
                    if (inner.geometryType != "LineString") {
                        logger.warn {"LineString, inner type is ${inner.geometryType}" }
                    }
                    text.add("(${wktCoordinates(inner.coordinates)})")
                }
                return "${if (addType) "MULTILINESTRING"  else ""} (${text.joinToString(", ")})"
            }
            "MultiPoint" -> {
                return "${if (addType) "MULTIPOINT" else ""} (${wktCoordinates(geometry.coordinates)})"
            }
            "MultiPolygon" -> {
                if (geometry.numGeometries == 0) { return "${if (addType) "MULTIPOLYGON" else ""} EMPTY" }
                val text = mutableListOf<String>()
                for (index in 0 until  geometry.numGeometries) {
                    val inner = geometry.getGeometryN(index)
                    text.add(toWkt(inner, false, debug))
                }
                if (debug) { logger.debug {"returning MULTIPOLYGON (${text.joinToString(", ")})" } }
                if (debug) { logger.debug {"toText=${geometry.toText()}" } }
                return "${if (addType) "MULTIPOLYGON" else ""} (${text.joinToString(", ")})"
            }
            "Point" -> {
                val point = geometry as Point
                return "${if (addType) "POINT" else ""} (${point.y} ${point.x})"
            }
            "Polygon" -> {
                val polygon = geometry as Polygon
                val output = StringBuilder(256)
                output.append("(")
                output.append("(${wktCoordinates(polygon.exteriorRing.coordinates)})")

                val text = mutableListOf<String>()
                for (index in 0 until polygon.numInteriorRing) {
                    text.add("(${wktCoordinates(polygon.getInteriorRingN(index).coordinates)})")
                }
                output.append(")")
                return if (addType) { return "POLYGON $output" } else output.toString()
            }
            else -> {
                logger.debug {"geometry count: ${geometry.numGeometries}" }
                logger.debug {"$geometry" }
                throw Exception("unhandled type: ${geometry.geometryType}")
            }
        }
        return geometry.toString()
    }

    private fun wktCoordinates(coordinates: Array<Coordinate>): String {
        return coordinates.joinToString(",") { "${it.y} ${it.x}" }
    }

    private fun builder(): GeometryBuilder {
        val builder = GeometryBuilder()
        builder.missingEntitiesStrategy = MissingEntitiesStrategy.BUILD_PARTIAL
        return builder
    }

    private fun emitInfo(force: Boolean) {
        if (force || nextEmitLineCount <= totalPOIsProcessed) {
            logger.info {
                "Resolved $totalPOIsProcessed POIs, $badGeometryCount had bad geometries, " +
                    "${RelationBuilder.missingWaysCount} were missing ways, ${RelationBuilder.noWaysCount} had no ways. " +
                    "Indexed $itemsIndexed with $indexErrors errors. $indexExceptions exceptions " +
                    "($countRetryAttempts retry attempts) and ${OsmItemRepository.failedLookupCount} failed lookups. " +
                    "Skipped $largePOIsSkipped POIs due to large geometries"
            }

            nextEmitLineCount += emitCount
        }
    }

/*
    // Need WellKnownText, area and center point/centroid
    private fun resolveItem(item: OsmItem): String {
val debug = listOf("relation/2583105").contains(item.id)
if (debug) { println("resolving ${item.id} ${item.name}, ${item.rolesSidList} (${item.nodeIds?.size} nodes, ${item.wayIds?.size} ways, ${item.relationIds?.size} relations)") }

        val wayList = mutableListOf<OsmItem>()
        item.wayIds?.let { idList ->
            idList.forEach { id ->
                OsmItemRepository.retrieve(OsmItem.wayId(id))?.let { wayItem ->
                    wayList.add(wayItem)
                }
            }
        }
        if (item.relationIds != null && item.relationIds.isNotEmpty()) {
            println("IGNORING relation list for ${item.id}: ${item.relationIds}")
        }

        val wktList = mutableListOf<String>()
        if (wayList.isNotEmpty()) {
            // It's a multipolygon
            // Each way should have nodes (more than 2), NO relations & NO ways
            // Points are really nodes, which have valid latitude/longitude fields
            val points = mutableListOf<OsmItem>()
            wayList.forEach { wayItem ->
                val wayPoints = mutableListOf<OsmItem>()
                if (wayItem.relationIds != null && wayItem.relationIds.isNotEmpty()) {
                    println("WARNING: way ${wayItem.id} has relations -> ${wayItem.relationIds}")
                }
                if (wayItem.wayIds != null && wayItem.wayIds.isNotEmpty()) {
                    println("WARNING: way ${wayItem.id} has ways -> ${wayItem.wayIds}")
                }
                wayItem.nodeIds?.let { idList ->
                    idList.forEach { nodeId ->
                        OsmItemRepository.retrieve(OsmItem.nodeId(nodeId))?.let {
                            wayPoints.add(it)
                        }
                    }
                }
//if (debug) { println(" ${wayItem.id} has ${wayPoints.size} way points; there are ${points.size} in points") }
                if (wayPoints.size < 2) {
                    println("WARNING: way ${wayItem.id} has too few points: ${wayPoints.size}")
                } else {
                    if (wayPoints.first().id == wayPoints.last().id) {
//                        if (points.isNotEmpty()) {
//                            println("closed way ${wayItem.id}, but points is not empty (${item.id})")
//                        }
//if (debug) {
//    println("points: ${points}\nsorted: ${sortPointsCounterClockwise(points)}")
//}
if (debug) {
    println(" -> ${wayItem.id} first/last are equal: ${wayPoints.first()} && ${wayPoints.last()}; ${wayPoints.size} points in way")
}
                        // Sorting is likely to change the first/last items, ensure the first/last are the same
                        // *after* sorting
//                        wayPoints.removeLast()
                        val geoPoints = wayPoints.map { GeoPoint(it.latitude!!, it.longitude!!) }
//                        val sorted = sortPointsCounterClockwise(geoPoints).toMutableList()
//                        sorted.add(sorted.first())
                        wktList.add("((${geoPoints.joinToString(",") { "${it.lon} ${it.lat}" }}))")
                    } else {
                        // Either add the (not closed) wayPoints to the start or end of points
                        if (points.isEmpty()) {
                            points.addAll(wayPoints)
if (debug) {
    println("Added all ${wayItem.id} points, now have ${points.first().id} & ${points.last().id}")
}
                        } else {
if (debug) {
    println("before combining points (${points.size}: ${points.first().id} & ${points.last().id}) and way (${wayPoints.size}: ${wayPoints.first().id} & ${wayPoints.last().id})")
}
                            if (points.first().id == wayPoints.last().id) {
if (debug) { println("inserted ${wayItem.id} at the start") }
                                points.addAll(0, wayPoints.dropLast(1))
                            } else if (points.last().id == wayPoints.first().id) {
if (debug) { println("added ${wayItem.id} at the end") }
                                points.addAll(wayPoints.drop(1))
                            } else if (points.first().id == wayPoints.first().id && points.last().id == wayPoints.last().id) {
if (debug) { println("added ${wayItem.id} at the start (start & end are equal)") }
                                points.addAll(wayPoints.drop(1).dropLast(1))
                            } else if (points.last().id == wayPoints.last().id) {
if (debug) { println("added a reversed ${wayItem.id} at the end (ends are equal)") }
                                points.addAll(wayPoints.dropLast(1).reversed())
                            } else if (points.first().id == wayPoints.first().id) {
if (debug) { println("added a reversed ${wayItem.id} at the start (starts are equal)") }
                                points.addAll(0, wayPoints.drop(1).reversed())
                            } else {
//println("UNHANDLED combine for ${item.id} (${item.name}) - ${wayItem.id}, points (${points.size}: ${points.first().id} & ${points.last().id}) and way (${wayPoints.size}: ${wayPoints.first().id} & ${wayPoints.last().id})")
//println(" > POINTS: ${points.joinToString(",") { it.id } }")
//println(" > WAY: ${wayPoints.joinToString(",") { it.id } }")
                            }
if (debug) {
//    println("after (${points.size}): ${points.joinToString(",") { it.id } }")
    println("after (${points.size})")
}
                        }
                    }
                }
            }

            if (points.isNotEmpty()) {
//if (debug) {
//    println("points: ${pointsString(points)}\nsorted: ${pointsString(sortPointsCounterClockwise(points))}")
//}
//                if (points.first().id == points.last().id) {
//                    points.removeLast()
//                }
                val geoPoints = points.map { GeoPoint(it.latitude!!, it.longitude!!) }.toMutableList()
                if (geoPoints.first() != geoPoints.last()) {
                    geoPoints.add(geoPoints.first())
                }
//                val sorted = sortPointsCounterClockwise(geoPoints).toMutableList()
//                if (sorted.first() != sorted.last()) {
//if (debug) { println("Adding point to make first/last match") }
//                    sorted.add(sorted.first())
//                }
//if (debug) { println("sorted => ${pointsString(sorted)}") }
                wktList.add("((${geoPoints.joinToString(",") { "${it.lon} ${it.lat}" }}))")
            }
        }
        if (wktList.isEmpty()) { return "" }
        val wkt = "MULTIPOLYGON(${wktList.joinToString(",")})"
if (debug) { println(" => $wkt") }
        return wkt
    }

    private fun pointsString(points: List<GeoPoint>): String {
        return points.joinToString(",") { "${it.lon} ${it.lat}" }
    }

    // Determine the mean center
    fun calculateCenter(list: List<GeoPoint>): GeoPoint {
        var centerTotalLatitude = 0.0
        var centerTotalLongitude = 0.0
        list.forEach {
            centerTotalLatitude += it.lat
            centerTotalLongitude += it.lon
        }

        return GeoPoint(centerTotalLatitude / list.size, centerTotalLongitude / list.size)
    }

    fun sortPointsCounterClockwise(list: List<GeoPoint>): List<GeoPoint> {
        val center = calculateCenter(list)
        return list.sortedWith(Comparator { a, b ->
            when {
                a.lat - center.lat >= 0 && b.lat - center.lat < 0 -> -1
                a.lat - center.lat < 0 && b.lat - center.lat >= 0 -> 1
                a.lat - center.lat == 0.0 && b.lat - center.lat == 0.0 -> {
                    if (a.lon - center.lon >= 0 || b.lon - center.lon >= 0) { (a.lon - b.lon).toInt() }
                    (b.lon - a.lon).toInt()
                }
                else -> {
                    val xprod = (a.lat - center.lat) * (b.lon - center.lon) - (b.lat - center.lat) * (a.lon - center.lon)
                    if (xprod < 0) { return@Comparator -1 }
                    if (xprod > 0) { return@Comparator 1 }
                    val d1 = (a.lat - center.lat) * (a.lat - center.lat) + (a.lon - center.lon) * (a.lon - center.lon)
                    val d2 = (b.lat - center.lat) * (b.lat - center.lat) + (b.lon - center.lon) * (b.lon - center.lon)
                    (d2 - d1).toInt()
                }
            }
        })
    }
 */
}
