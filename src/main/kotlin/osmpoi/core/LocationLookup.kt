package osmpoi.core

import co.elastic.clients.elasticsearch._types.DistanceUnit
import co.elastic.clients.elasticsearch._types.ErrorResponse
import co.elastic.clients.elasticsearch._types.SortOrder
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery
import co.elastic.clients.elasticsearch.core.MsearchRequest
import co.elastic.clients.elasticsearch.core.msearch.MultiSearchResponseItem
import co.elastic.clients.elasticsearch.core.msearch.MultisearchBody
import co.elastic.clients.elasticsearch.core.msearch.RequestItem
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import osmpoi.indexer.PoiLevel
import osmpoi.models.*
import osmpoi.service.CityAndDistance
import osmpoi.service.CitySelector
import osmpoi.service.PoiFilter
import java.io.StringReader
import kotlin.system.measureTimeMillis

object LocationLookup {
    private fun poiFromMap(map: Map<*, *>): OsmPoi {
        val point = map["point"] as Map<*,*>
        val tags = (map["tags"] as List<*>).filterIsInstance<Map<String, String>>()
        return OsmPoi(
            id = map["id"] as String,
            name = map["name"] as String,
            point = GeoPoint(point["lat"] as Double, point["lon"] as Double),
            tags = tags.map { OsmTagValue(it["key"] as String, it["value"] as String) },
            area = map["area"] as Double,
            poiLevel = PoiLevel.valueOf(map["poiLevel"] as String)
        )
    }

    private fun cityFromMap(map: Map<*, *>): WofEntity {
        val point = map["point"] as Map<*,*>
        return WofEntity(
            id = (map["id"] as Int).toLong(),
            name = map["name"] as String,
            placeType = map["placeType"] as String,
            point = GeoPoint(point["lat"] as Double, point["lon"] as Double),
            areaMeters = map["areaMeters"] as Double,
            population = (map["population"] as Int?)?.toLong(),
            populationRank = map["populationRank"] as Int?,
            country = map["country"] as String,
            countryCode = map["countryCode"] as String,
            state = map["state"] as String?,
            stateCode = map["stateCode"] as String?,
            shape = map["shape"] as String? ?: "",
        )
    }

    fun lookupTest(points: List<GeoPoint>, includeOrdinary: Boolean, poiRadiusMeters: Int, cityRadiusMeters: Int): List<LocationSearchResponse> {
        val searches = poiRequest(points, includeOrdinary, poiRadiusMeters) + cityRequest(points, cityRadiusMeters)
        val searchResponses = Elastic.client.msearch(
            MsearchRequest.Builder()
                .searches(searches)
                .build(),
            Any::class.java
        ).responses()

        if (searchResponses.size != 4 * points.size) {
            throw Exception("Wrong number of search responses: ${searchResponses.size}")
        }

        // Due to adding the city requests after all the poi requests, all the POI searches are together at the
        // start of 'searchResponses' and all the city requests are grouped together in the second half
        val response = mutableListOf<LocationSearchResponse>()
        val cityIndexOffset = searchResponses.size / 2
        for (idx in 0 until searchResponses.size/2 step 2) {
            val poiInside = searchResponses[idx]
            val poiNearby = searchResponses[idx + 1]
            val cityInside = searchResponses[cityIndexOffset + idx]
            val cityNearby = searchResponses[cityIndexOffset + idx + 1]

            if (poiInside.isFailure || poiNearby.isFailure ||
                cityInside.isFailure || cityNearby.isFailure
            ) {
                response.add(
                    LocationSearchResponse(
                        failuresString(
                            listOf(
                                if (poiInside.isFailure) poiInside.failure() else null,
                                if (poiNearby.isFailure) poiNearby.failure() else null,
                                if (cityInside.isFailure) cityInside.failure() else null,
                                if (cityNearby.isFailure) cityNearby.failure() else null
                            )
                        ),
                        null
                    )
                )
            } else {
                val poiInsideCollection = mutableMapOf<String, OsmPoi>()
                poiInside.result().hits().hits().forEach { hit ->
                    val poi = poiFromMap(hit.source() as Map<*, *>)
                    poiInsideCollection[poi.id] = poi
                }

                val poiNearbyCollection = mutableListOf<OsmPoi>()
                poiNearby.result().hits().hits().forEach { hit ->
                    hit.source()?.let { source ->
                        val poi = poiFromMap(source as Map<*, *>)
                        if (!poiInsideCollection.containsKey(poi.id)) {
                            poiNearbyCollection.add(poi)
                        }
                    }
                }

                val cityInsideCollection =
                    cityInside.result().hits().hits().mapNotNull { cityFromMap(it.source() as Map<*, *>) }
                val cityNearbyCollection = cityNearby.result().hits().hits()
                    .mapNotNull {
                        CityAndDistance(
                            cityFromMap(it.source()!! as Map<*, *>),
                            it.sort().firstOrNull()?.toDouble() ?: (cityRadiusMeters * 1000.0)
                        )
                    }
                val cityResult = CitySelector.apply(cityInsideCollection, cityNearbyCollection)

                response.add(
                    LocationSearchResponse(
                        null,
                        LocationResponse(
                            inside = PoiFilter.run(poiInsideCollection.map { it.value }.sortedBy { it.area }),
                            nearby = PoiFilter.run(poiNearbyCollection),
                            countryCode = cityResult.countryCode,
                            countryName = cityResult.countryName,
                            stateName = cityResult.stateName,
                            cityName = cityResult.cityName
                        )
                    )
                )
            }
        }

        return response
    }

    // There are four searches
    //      1. Inside - return all POI items
    //      2. Nearby POI items (Notable, Ordinary)
    //      3. Inside - return all city/state/country items
    //      4. Nearby city items
    // The two nearby queries have the weakness that they are sorted by distance from the POI/Cities major point,
    // which isn't always the center. This is a particular problem for cities, as they're larger and the major
    // point is rarely the center.
    // I'd prefer to have the distance from the closest point of the shape instead.
    //
    // A possible solution is to use the "Who's on First" 'bbox' (bounding box) and find the nearest
    // bbox (the point may be inside the bbox but not the shape)
    //
    // Scoring distance from a point: https://www.factweavers.com/blog/introduction-of-geo-queries-in-elasticsearch/
    // Perhaps it could be used to score distance from a shape?
    @OptIn(DelicateCoroutinesApi::class)
    suspend fun lookup(points: List<GeoPoint>, includeOrdinary: Boolean, poiRadiusMeters: Int, cityRadiusMeters: Int): List<LocationSearchResponse> {
        val tasks = mutableListOf<Deferred<Unit>>()
        val semaphore = Semaphore(2, 2)

        val poiRequests = poiRequest(points, includeOrdinary, poiRadiusMeters)
        val citySearches = cityRequest(points, cityRadiusMeters)

        var poiSearchResponse = listOf<MultiSearchResponseItem<OsmPoi>>()
        var citySearchResponse = listOf<MultiSearchResponseItem<WofEntity>>()
        GlobalScope.launch {
            tasks.add(async {
                semaphore.release()
                poiSearchResponse = Elastic.client.msearch(
                    MsearchRequest.Builder()
                        .searches(poiRequests)
                        .build(),
                    OsmPoi::class.java
                ).responses()
            })

            tasks.add(async {
                semaphore.release()
                citySearchResponse = Elastic.client.msearch(
                    MsearchRequest.Builder()
                        .searches(citySearches)
                        .build(),
                    WofEntity::class.java
                ).responses()
            })
        }

        semaphore.acquire()
        semaphore.acquire()
        tasks.forEach { it.await() }
        if (poiSearchResponse.size != 2 * points.size) {
            throw Exception("Wrong number of POI responses: ${poiSearchResponse.size}")
        }
        if (citySearchResponse.size != 2 * points.size) {
            throw Exception("Wrong number of city responses: ${citySearchResponse.size}")
        }

        val response = mutableListOf<LocationSearchResponse>()
        for (idx in poiSearchResponse.indices step 2) {
            val poiInsideResponse = poiSearchResponse[idx + 0]
            val poiNearbyResponse = poiSearchResponse[idx + 1]
            val cityInsideResponse = citySearchResponse[idx + 0]
            val cityNearbyResponse = citySearchResponse[idx + 1]

            if (poiInsideResponse.isFailure || poiNearbyResponse.isFailure ||
                cityInsideResponse.isFailure || cityNearbyResponse.isFailure
            ) {
                response.add(
                    LocationSearchResponse(
                        failuresString(
                            listOf(
                                if (poiInsideResponse.isFailure) poiInsideResponse.failure() else null,
                                if (poiNearbyResponse.isFailure) poiNearbyResponse.failure() else null,
                                if (cityInsideResponse.isFailure) cityInsideResponse.failure() else null,
                                if (cityNearbyResponse.isFailure) cityNearbyResponse.failure() else null
                            )
                        ),
                        null
                    )
                )
            } else {
                val poiInsideCollection = mutableMapOf<String, OsmPoi>()
                poiInsideResponse.result().hits().hits().forEach { hit ->
                    hit.source()?.let { source ->
                        poiInsideCollection[source.id] = source
                    }
                }

                // Nearby POIs picked up by the inside query are filtered out here - no need for duplicates
                val poiNearbyCollection = mutableListOf<OsmPoi>()
                poiNearbyResponse.result().hits().hits().forEach { hit ->
                    hit.source()?.let { source ->
                        if (!poiInsideCollection.containsKey(source.id)) {
                            poiNearbyCollection.add(source)
                        }
                    }
                }

                val cityInsideCollection = cityInsideResponse.result().hits().hits().mapNotNull { it.source() }
                val cityNearbyCollection = cityNearbyResponse.result().hits().hits()
                    .mapNotNull {
                        CityAndDistance(
                            it.source()!!,
                            it.sort().firstOrNull()?.toDouble() ?: (cityRadiusMeters * 1000.0)
                        )
                    }
                val cityResult = CitySelector.apply(
                    cityInsideCollection,
                    cityNearbyCollection
                )

                response.add(
                    LocationSearchResponse(
                        null,
                        LocationResponse(
                            inside = PoiFilter.run(poiInsideCollection.map { it.value }.sortedBy { it.area }),
                            nearby = PoiFilter.run(poiNearbyCollection),
                            countryCode = cityResult.countryCode,
                            countryName = cityResult.countryName,
                            stateName = cityResult.stateName,
                            cityName = cityResult.cityName
                        )
                    )
                )
            }
        }

//println("${cityInsideResponse.result().hits().hits().mapNotNull { hit -> "name=${hit.source()?.name} type=${hit.source()?.placeType}" }}")
//val poiMsg = "poi inside=${poiInsideCollection.values.map { it.name } } nearby=${poiNearbyResponse.result().hits().hits().map { it.source()?.name} }"
//val cityMsg = "city inside=${cityInsideCollection.map { "${it.name} (${it.placeType} ${it.id})" } } nearby=${cityNearbyResponse.result().hits().hits().map { "${it.source()?.name} (${it.source()?.id} pop=${it.source()?.populationRank}/${it.source()?.population} dist=${it.sort()})" } }"
//println("$poiMsg\n -> $cityMsg")
        return response
    }

    private fun failuresString(errors: List<ErrorResponse?>): String {
        var message = ""
        errors.forEach {
            it?.let { err ->
                val cause = err.error()
                message += "type=${cause.type()} reason=${cause.reason()};"
                cause.rootCause().forEach { rootCause ->
                    message += "root-type: ${rootCause.type()} root-reason: ${rootCause.reason()}"
                }
            }
        }
        return message
    }

    private fun poiRequest(points: List<GeoPoint>, includeOrdinary: Boolean, radiusMeters: Int): List<RequestItem> {
        val requestItems = mutableListOf<RequestItem>()
        points.forEach { point ->
            val poiInsideQuery = baseIntersectQuery(point.lat, point.lon, "location")
            poiInsideQuery.mustNot { m -> m
                .term { tm -> tm
                    .field("poiLevel.keyword")
                    .value(PoiLevel.ADMIN.toString())
                }
            }
            if (!includeOrdinary) {
                poiInsideQuery.mustNot { m -> m
                    .term { tm -> tm
                        .field("poiLevel.keyword")
                        .value(PoiLevel.ORDINARY.toString())
                    }
                }
            }

            requestItems.add(
                RequestItem.Builder()
                .header { h -> h.index(Elastic.poiIndexName) }
                .body { MultisearchBody.Builder()
                    .query { q -> q
                        .bool(poiInsideQuery.build())
                    }
                    .source { s -> s
                        .filter { fil -> fil
                            .excludes("location")
                        }
                    }
                    .sort { s -> s
                        .field { f -> f.field("area").order(SortOrder.Asc) }
                    }
                    .size(30)
                }
                .build()
            )

            val poiNearbyQuery = BoolQuery.Builder()
                .must { m -> m
                    .geoDistance { gd -> gd
                        .distance("${radiusMeters}m")
                        .field("location")
                        .location { loc -> loc
                            .latlon { ll -> ll
                                .lat(point.lat)
                                .lon(point.lon)
                            }
                        }
                    }
                }
                .mustNot { mn -> mn
                    .term { tm -> tm
                        .field("poiLevel.keyword")
                        .value(PoiLevel.ADMIN.toString())
                    }
                }
            if (!includeOrdinary) {
                poiNearbyQuery.mustNot { mn -> mn
                    .term { tm -> tm
                        .field("poiLevel.keyword")
                        .value(PoiLevel.ORDINARY.toString())
                    }
                }
            }

            requestItems.add(
                RequestItem.Builder()
                .header { h -> h.index(Elastic.poiIndexName) }
                .body { MultisearchBody.Builder()
                    .query { q -> q
                        .bool(poiNearbyQuery.build())
                    }
                    .source { s -> s
                        .filter { fil -> fil
                            .excludes("location")
                        }
                    }
                    .sort { s -> s
                        .geoDistance { gs -> gs
                            .field("point")
                            .location { lo -> lo
                                .latlon { ll -> ll
                                    .lat(point.lat)
                                    .lon(point.lon)
                                }
                            }
                            .order(SortOrder.Asc)
                            .unit(DistanceUnit.Meters)
                        }
                    }
                    .size(30)
                }
                .build()
            )
        }

        return requestItems
    }

    private fun cityRequest(points: List<GeoPoint>, radiusMeters: Int): List<RequestItem> {
        val requestItems = mutableListOf<RequestItem>()
        points.forEach { point ->
            val cityInsideQuery = baseIntersectQuery(point.lat, point.lon, "shape")
            requestItems.add(
                RequestItem.Builder()
                .header { h -> h.index(Elastic.wofIndexName) }
                .body {
                    MultisearchBody.Builder()
                        .query { q -> q.bool(cityInsideQuery.build()) }
                        .source { s -> s
                            .filter { fil -> fil
                                .excludes("shape")
                            }
                        }
                        .sort { s -> s
                            .field { f -> f.field("areaMeters").order(SortOrder.Asc) }
                        }
                        .size(30)
                }
                .build()
            )

            val cityNearbyQuery = BoolQuery.Builder()
                .must { m -> m
                    .geoDistance { gd -> gd
                        .distance("${radiusMeters}m")
                        .field("shape")
                        .location { loc -> loc
                            .latlon { ll -> ll
                                .lat(point.lat)
                                .lon(point.lon)
                            }
                        }
                    }
                }
                .must { mn -> mn
                    .term { tm -> tm
                        .field("placeType.keyword")
                        .value("locality")
                    }
                }
            requestItems.add(
                RequestItem.Builder()
                .header { h -> h.index(Elastic.wofIndexName) }
                .body {
                    MultisearchBody.Builder()
                        .query { q -> q
                            .bool(cityNearbyQuery.build())
                        }
                        .source { s -> s
                            .filter { fil -> fil
                                .excludes("shape")
                            }
                        }
                        .sort { s -> s
                            .geoDistance { gs -> gs
                                .field("point")
                                .location { lo -> lo
                                    .latlon { ll -> ll
                                        .lat(point.lat)
                                        .lon(point.lon)
                                    }
                                }
                                .order(SortOrder.Asc)
                                .unit(DistanceUnit.Meters)
                            }
                        }
                        .size(50)
                }
                .build()
            )
        }

        return requestItems
    }

    private fun baseIntersectQuery(lat: Double, lon: Double, field: String): BoolQuery.Builder {
        return BoolQuery.Builder()
            .must { m -> m
                .geoShape { gs -> gs
                    .withJson(
                        StringReader(
                        """
                        {
                            "$field": {
                                "shape": {
                                    "type": "envelope",
                                    "coordinates": [
                                        [
                                            $lon,
                                            $lat
                                        ],
                                        [
                                            $lon,
                                            $lat
                                        ]
                                    ]
                                },
                                "relation": "intersects"
                            }
                        }                
                        """.trimIndent()
                    )
                    )
                }
            }
    }
}
