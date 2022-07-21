package osmpoi.service

import co.elastic.clients.elasticsearch.core.MgetRequest
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import osmpoi.core.Elastic
import osmpoi.models.OsmPoi

@kotlinx.serialization.Serializable
data class ShapesRequest(
    val ids: List<String>
)

@kotlinx.serialization.Serializable
data class ShapeItemResponse(
    val id: String,
    val shape: String?,
    val error: String?
)

@kotlinx.serialization.Serializable
data class ShapesResponse(
    val shapes: List<ShapeItemResponse>
)

fun Route.shapes() {
    post("/shapes") {
        val apiRequest = call.receive<ShapesRequest>()
        if (apiRequest.ids.isEmpty()) {
            call.respond(HttpStatusCode.OK, ShapesResponse(emptyList()))
        }

        val request = MgetRequest.Builder()
            .index(Elastic.poiIndexName)
            .ids(apiRequest.ids)
        val response = Elastic.client.mget(request.build(), OsmPoi::class.java)
            .docs().map { d ->
                if (d.isFailure) { ShapeItemResponse(
                    id = d.failure().id(),
                    shape = null,
                    error = d.failure().error().reason()) }
                ShapeItemResponse(
                    id = d.result().id(),
                    shape = d.result().source()?.location,
                    error = null)
            }
        call.respond(HttpStatusCode.OK, ShapesResponse(response))
    }
}
