package osmpoi.models

import kotlinx.serialization.Serializable

@Serializable
data class LocationResponse(
    val inside: List<OsmPoi>,
    val nearby: List<OsmPoi>,
    val countryCode: String?,
    val countryName: String?,
    val stateName: String?,
    val cityName: String?
)

@Serializable
data class LocationSearchResponse(
    val error: String? = null,
    val location: LocationResponse? = null
)