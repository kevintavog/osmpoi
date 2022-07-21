package osmpoi.models

import kotlinx.serialization.Serializable

@Serializable
data class GeoPoint(val lat: Double = 0.0, val lon: Double = 0.0)
