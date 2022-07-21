package osmpoi.service

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery
import co.elastic.clients.elasticsearch._types.query_dsl.Query
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.SearchResponse
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import osmpoi.models.OsmPoi
import osmpoi.core.Elastic
import osmpoi.models.PoiResponse
import java.lang.Double.max
import java.lang.Double.min

fun Route.area() {
    get("/area") {
        val boundsParam = context.parameters["bounds"]
        if (boundsParam == null) {
            call.respond(HttpStatusCode.BadRequest, "Missing 'bounds' (min-lat,min-lon,max-lat,max-lon)")
            return@get
        }
        val tokens = boundsParam.split(",").map { it.trim() }
        val boundsValue = tokens.map { it.toDoubleOrNull() }
        val allNumbers = -1 == boundsValue.indexOf(null)
        if (tokens.size != 4 || !allNumbers) {
            call.respond(HttpStatusCode.BadRequest, "'bounds' must be 4 comma-separated numbers (min-lat,min-lon,max-lat,max-lon)")
            return@get
        }

        val includeLocation = context.parameters["location"]?.toBoolean() ?: false
        val includeOrdinary = context.parameters["ordinary"]?.toBoolean() ?: false
        val includeAdmin = context.parameters["admin"]?.toBoolean() ?: false

        try {
            val boolQuery = BoolQuery.Builder()
                .must { m -> m
                    .matchAll { ma -> ma }
                }
                .filter { f -> f
                    .geoBoundingBox { gbb -> gbb
                        .field("location")
                        .boundingBox { bb -> bb
                            .coords { c -> c
                                .top(max(boundsValue[0]!!, boundsValue[2]!!))
                                .left(min(boundsValue[1]!!, boundsValue[3]!!))
                                .bottom(min(boundsValue[0]!!, boundsValue[2]!!))
                                .right(max(boundsValue[1]!!, boundsValue[3]!!))
                            }
                        }
                    }
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
                boolQuery.mustNot(mustNotList)
            }

            val searchRequest = SearchRequest.Builder()
                .index(Elastic.poiIndexName)
                .size(500)
                .query { q -> q
                    .bool(boolQuery.build())
                }
            if (!includeLocation) {
                searchRequest.source { src -> src
                    .filter { fil -> fil
                        .excludes("location")
                    }
                }
            }

            val searchResponse: SearchResponse<OsmPoi> = Elastic.client.search(searchRequest.build(), OsmPoi::class.java)
            call.respond(PoiResponse(
                searchResponse.hits().total()?.value() ?: 0,
                PoiFilter.apply(searchResponse.hits().hits().mapNotNull { it.source() }).poiList)
            )
        } catch (t: Throwable) {
            call.respond(HttpStatusCode.InternalServerError, "Server exception: ${Elastic.toReadable(t)}")
            logException(t)
        }
    }
}
