package osmpoi.indexer

import GeoJsonGeometry
import GeoJsonGeometryMultiPolygon
import GeoJsonGeometryPoint
import GeoJsonGeometryPolygon

object GeoJsonToWkt {
    fun convert(geometry: GeoJsonGeometry, addType: Boolean): String {
        when(geometry.type) {
//            "GeometryCollection" -> {
//                val text = mutableListOf<String>()
//                for (index in 0 until geometry.numGeometries) {
//                    text.add(toWkt(geometry.getGeometryN(index), true, debug))
//                }
//                if (debug) { logger.debug {"returning GEOMETRYCOLLECTION (${text.joinToString(", ")})" } }
//                if (debug) { logger.debug {"toText=${geometry.toText()}" } }
//                return "${if (addType) "GEOMETRYCOLLECTION" else ""} (${text.joinToString(", ")})"
//            }
//            "LinearRing" -> {
//                if (geometry.coordinates.first().equals2D(geometry.coordinates.last())) {
//                    return "${if (addType) "POLYGON" else ""} ((${wktCoordinates(geometry.coordinates)}))"
//                }
//            }
//            "LineString" -> {
//                return "${if (addType) "LINESTRING" else ""} (${wktCoordinates(geometry.coordinates)})"
//            }
//            "MultiLineString" -> {
//                val text = mutableListOf<String>()
//                for (index in 0 until  geometry.numGeometries) {
//                    val inner = geometry.getGeometryN(index)
//                    if (inner.geometryType != "LineString") {
//                        logger.warn {"LineString, inner type is ${inner.geometryType}" }
//                    }
//                    text.add("(${wktCoordinates(inner.coordinates)})")
//                }
//                return "${if (addType) "MULTILINESTRING"  else ""} (${text.joinToString(", ")})"
//            }
//            "MultiPoint" -> {
//                return "${if (addType) "MULTIPOINT" else ""} (${wktCoordinates(geometry.coordinates)})"
//            }
            "MultiPolygon" -> {
                val multiPolygon = geometry as GeoJsonGeometryMultiPolygon
                return "${if (addType) "MULTIPOLYGON" else ""} ${surfaceList(multiPolygon.coordinates)}"
            }
            "Point" -> {
                val point = geometry as GeoJsonGeometryPoint
                return "${if (addType) "POINT" else ""} (${point(point.coordinates)})"
            }
            "Polygon" -> {
                val polygon = geometry as GeoJsonGeometryPolygon
                return "${if (addType) "POLYGON" else ""} ${surface(polygon.coordinates)}"
            }
            else -> {
                throw Exception("unhandled GeoJson type when converting to WKT: ${geometry.type}")
            }
        }
    }

    private fun surfaceList(surfaceList: Array<Array<Array<Array<Double>>>>): String {
        return "(${surfaceList.joinToString(",") { surface(it) }})"
    }

    private fun surface(surface: Array<Array<Array<Double>>>): String {
        return "(${surface.joinToString(",") { line(it) }})"
    }

    private fun line(line: Array<Array<Double>>): String {
        return "(${line.joinToString(",") { point(it) }})"
    }

    private fun point(point: Array<Double>): String {
        return "${point[0]} ${point[1]}"
    }
}
