package osmpoi.models

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import kotlinx.serialization.Serializable
import osmpoi.indexer.PoiLevel
import java.time.Clock
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

// NOTE: The default values are to make the jackson deserializer work, as it requires all fields to be initialized
val utcFormatter: DateTimeFormatter by lazy {
    DateTimeFormatter
        .ofPattern("yyyy-MM-dd'T'HH:mm:ssZZZZZ")
}

@JsonIgnoreProperties(ignoreUnknown = true)
@Serializable
data class OsmPoi(
    val id: String = "",
    var name: String = "",
    val point: GeoPoint = GeoPoint(0.0, 0.0),
    var location: String = "",
    var tags: List<OsmTagValue> = emptyList(),
    val area: Double = 0.0,
    var poiLevel: PoiLevel = PoiLevel.ZERO,
    var dateCreated: String = ZonedDateTime.now(Clock.systemUTC()).format(utcFormatter)
) {
    fun tagsAsMap() = tags.associate { Pair(it.key, it.value) }
}

@Serializable
data class OsmTagValue(val key: String = "", val value: String = "") {
    override fun toString(): String {
        return "$key=$value"
    }
}
