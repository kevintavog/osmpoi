package osmpoi.csvexporter

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import osmpoi.models.GeoPoint

@JsonIgnoreProperties(ignoreUnknown = true)
data class Placename(
    val sites: List<String> = emptyList(),
    val location: GeoPoint = GeoPoint(0.0, 0.0)
)
