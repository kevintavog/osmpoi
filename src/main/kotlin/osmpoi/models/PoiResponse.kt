package osmpoi.models

@kotlinx.serialization.Serializable
data class PoiResponse(
    val totalCount: Long,
    val pois: List<OsmPoi>
)
