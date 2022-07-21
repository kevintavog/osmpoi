package osmpoi.csvexporter

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class Overpass(val location: OverpassLocation = OverpassLocation())

data class OverpassLocation(
    val lat: Double = 0.0,
    val lon: Double = 0.0
)
