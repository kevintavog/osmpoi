package osmpoi.service

import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.serialization.Serializable
import osmpoi.core.Elastic
import osmpoi.core.LocationLookup
import osmpoi.models.GeoPoint
import osmpoi.models.LocationSearchResponse

const val defaultPoiRadiusMeters = 10
const val defaultCityRadiusMeters = (15 * 1000)

@Serializable
data class ApiLocationRequest(
    val items: List<GeoPoint>? = null,
    val includeOrdinary: Boolean? = null,
    val poiRadiusMeters: Int? = null,
    val cityRadiusMeters: Int? = null
)

@Serializable
data class ApiLocationResponse(
    val items: List<LocationSearchResponse>
)

fun Route.pois() {
    post("/pois") {
        val params = call.receive<ApiLocationRequest>()
        if (params.items == null) {
            call.respond(HttpStatusCode.BadRequest, "Missing 'items' with 'lat' and 'lon' fields")
        }
        try {
            call.respond(
                ApiLocationResponse(
                    LocationLookup.lookup(
                        params.items!!,
                        params.includeOrdinary ?: false,
                        params.poiRadiusMeters ?: defaultPoiRadiusMeters,
                        params.cityRadiusMeters ?: defaultCityRadiusMeters
                    )
                )
            )
        } catch (t: Throwable) {
            call.respond(HttpStatusCode.InternalServerError, "Server exception: ${Elastic.toReadable(t)}")
            logException(t)
        }
    }

    post("/poistest") {
        val params = call.receive<ApiLocationRequest>()
        if (params.items == null) {
            call.respond(HttpStatusCode.BadRequest, "Missing 'items' with 'lat' and 'lon' fields")
        }
        try {
            call.respond(
                ApiLocationResponse(
                    LocationLookup.lookupTest(
                        params.items!!,
                        params.includeOrdinary ?: false,
                        params.poiRadiusMeters ?: defaultPoiRadiusMeters,
                        params.cityRadiusMeters ?: defaultCityRadiusMeters
                    )
                )
            )
        } catch (t: Throwable) {
t.printStackTrace()
            call.respond(HttpStatusCode.InternalServerError, "Server exception: ${Elastic.toReadable(t)}")
            logException(t)
        }

    }

    get("/pois") {
        val lat = context.parameters["lat"]
        val latNum = lat?.toDoubleOrNull()
        if (lat == null || latNum == null) {
            call.respond(HttpStatusCode.BadRequest, "Missing 'lat' or not a valid number")
            return@get
        }
        val lon = context.parameters["lon"]
        val lonNum = lon?.toDoubleOrNull()
        if (lon == null || lonNum == null) {
            call.respond(HttpStatusCode.BadRequest, "Missing 'lon' or not a valid number")
            return@get
        }

        val includeOrdinary = context.parameters["ordinary"]?.toBoolean() ?: false
        val poiRadiusMeters = context.parameters["poi-radius"]?.toInt() ?: defaultPoiRadiusMeters
        val cityRadiusMeters = context.parameters["city-radius"]?.toInt() ?: defaultCityRadiusMeters

        try {
            val response = LocationLookup.lookup(
                listOf(GeoPoint(latNum, lonNum)),
                includeOrdinary,
                poiRadiusMeters,
                cityRadiusMeters
            )
                .first()
            if (response.error != null) {
                call.respond(HttpStatusCode.InternalServerError, it)
            } else if (response.location == null) {
                call.respond(HttpStatusCode.InternalServerError, "Unknown error; neither location nor error set")
            } else {
                call.respond(response.location)
            }
        } catch (t: Throwable) {
            call.respond(HttpStatusCode.InternalServerError, "Server exception: ${Elastic.toReadable(t)}")
            logException(t)
        }
    }
}
