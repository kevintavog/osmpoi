package osmpoi.models

import kotlinx.serialization.Serializable
import osmpoi.indexer.PoiLevel

enum class OsmItemType {
    NODE,
    WAY,
    RELATION
}

@Serializable
data class OsmItem(
    val id: String,
    val poiLevel: PoiLevel? = null,
    val latitude: Double? = null,
    val longitude: Double? = null,
    val name: String? = null,
    val tags: Map<String, String>? = null,
    val nodeIds: List<Long>? = null,
    val wayIds: List<Long>? = null,
    val relationIds: List<Long>? = null,
    val rolesSidList: List<String>? = null,
    val adminCenterNodeId: Long? = null
) {
    val type get() = id.substring(0, id.indexOf('/'))

    companion object {
        fun nodeId(id: Long) = "node/$id"
        fun wayId(id: Long) = "way/$id"
        fun relationId(id: Long) = "relation/$id"
        fun type(id: String) = enumValueOf<OsmItemType>(id.substring(0, id.indexOf("/")).uppercase())
    }

    fun toLongId() = id.substring(id.indexOf("/") + 1).toLong()
}
