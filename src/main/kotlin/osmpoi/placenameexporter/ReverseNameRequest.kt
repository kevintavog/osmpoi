package osmpoi.placenameexporter

import kotlinx.serialization.Serializable

@Serializable
data class NameRequest(val lat: Double, val lon: Double)

@Serializable
data class NameResponse(
    val placename: Placename?,
    val error: String? = null,
    val azureResults: ResolverFields? = null,
    val foursquareResults: ResolverFields? = null,
    val openCageDataResults: ResolverFields? = null,
    val overpassResults: ResolverFields? = null
)

@Serializable
data class Placename(
    val fullDescription: String,
    val sites: List<String>? = null,
    val city: String? = null,
    val state: String? = null,
    val countryName: String? = null,
    val countryCode: String? = null,
    val location: PlacenameLocation? = null,
    val dateCreated: String? = null
)

@Serializable
data class PlacenameLocation(val lat: Double, val lon: Double)

@Serializable
data class ResolverFields(
    val countryCode: String? = null,
    val countryName: String? = null,
    val stateName: String? = null,
    val cityName: String? = null,
    val sites: List<String>? = null,
    val description: String? = null,

    val location: PlacenameLocation? = null,

    val secondaryStateName: String? = null,
    val secondarySites: List<String>? = null
)

@Serializable
data class ReverseNameRequest(
    val items: List<NameRequest>,
    val radiusMeters: Double?
)

@Serializable
data class ReverseNamesResponse(
    val items: List<NameResponse>,
    val hasErrors: Boolean
)
