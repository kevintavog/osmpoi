package osmpoi.service

import co.elastic.clients.elasticsearch._types.DistanceUnit
import co.elastic.clients.elasticsearch._types.SortOptions
import co.elastic.clients.elasticsearch._types.SortOrder
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery
import co.elastic.clients.elasticsearch._types.query_dsl.Query
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery
import co.elastic.clients.elasticsearch.core.SearchRequest
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import osmpoi.models.OsmPoi
import osmpoi.core.Elastic
import osmpoi.models.PoiResponse

fun Route.textSearch() {
    get("/search") {
        val q = context.parameters["q"]
        if (q == null) {
            call.respond(HttpStatusCode.BadRequest, "Missing 'q', the text to search for")
            return@get
        }

        var searchQuery = BoolQuery.Builder()
            .must { m -> m
                .queryString { s -> s
                    .fields("name")
                    .query(q)
                }
            }

        var sort: SortOptions? = null

        val latParam = context.parameters["lat"]?.toDoubleOrNull()
        val lonParam = context.parameters["lon"]?.toDoubleOrNull()
        val includeLocation = context.parameters["location"]?.toBoolean() ?: false
        val includeOrdinary = context.parameters["ordinary"]?.toBoolean() ?: false
        val includeAdmin = context.parameters["admin"]?.toBoolean() ?: false
        val distanceKilometers = 1000.0

        // If we are searching from a point, sort by distance from the point as well
        if (latParam != null && lonParam != null) {
            searchQuery = BoolQuery.Builder()
                .must { m -> m
                    .queryString { s -> s
                        .fields("city", "country", "name")
                        .query(q)
                    }
                }
                .filter { f -> f
                    .geoDistance { gd -> gd
                        .distance("$distanceKilometers km")
                        .location { l -> l
                            .latlon { ll -> ll
                                .lat(latParam)
                                .lon(lonParam)
                            }
                        }
                        .field("point")
                    }
                }

            sort = SortOptions.Builder()
                .geoDistance { gd -> gd
                        .field("point")
                        .location { l -> l
                            .latlon { ll -> ll
                                .lat(latParam)
                                .lon(lonParam)
                            }
                        }
                        .order(SortOrder.Asc)
                        .unit(DistanceUnit.Meters)
                }
                .build()
        }

        val mustNotList = mutableListOf<Query>()
        if (!includeAdmin) {
            mustNotList.add(
                TermQuery.Builder()
                .field("tags.key.keyword")
                .value("admin_level")
                .build()
                ._toQuery()
            )
        }
        if (!includeOrdinary) {
            mustNotList.add(
                TermQuery.Builder()
                .field("poiLevel.keyword")
                .value("ORDINARY")
                .build()
                ._toQuery()
            )
        }
        if (mustNotList.isNotEmpty()) {
            searchQuery.mustNot(mustNotList)
        }

        val requestBuilder = SearchRequest.Builder()
            .index(Elastic.poiIndexName)
            .query(searchQuery.build()._toQuery())
            .size(100)
            .from(0)
        if (!includeLocation) {
            requestBuilder.source { src -> src
                .filter { fil -> fil
                    .excludes("location")
                }
            }
        }

        sort?.let {
            requestBuilder.sort(it)
        }
        val searchRequest = requestBuilder.build()
        try {
            val searchResponse = Elastic.client.search(searchRequest, OsmPoi::class.java)
            call.respond(
                PoiResponse(
                    searchResponse.hits().total()?.value() ?: 0,
                    PoiFilter.apply(searchResponse.hits().hits().mapNotNull { it.source() }).poiList
                )
            )
        } catch (t: Throwable) {
            call.respond(HttpStatusCode.InternalServerError, "Server exception: ${Elastic.toReadable(t)}")
            logException(t)
        }
    }
}