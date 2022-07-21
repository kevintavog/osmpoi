package osmpoi.models

import kotlinx.serialization.Serializable
import java.time.Clock
import java.time.ZonedDateTime

@Serializable
data class WofEntity(
    val id: Long = 0L,
    val name: String = "",               // To determine a name, see: https://github.com/whosonfirst/whosonfirst-names
    val placeType: String = "",
    val point: GeoPoint = GeoPoint(0.0, 0.0),
    val areaMeters: Double = 0.0,
    val population: Long? = 0,
    val populationRank: Int? = 0,
    val country: String = "",
    val countryCode: String = "",
    val state: String? = null,
    val stateCode: String? = null,
    val shape: String = "",
    val dateIndexed: String = ZonedDateTime.now(Clock.systemUTC()).format(utcFormatter)
) {
    override fun toString(): String {
        return "id=$id name=$name type=$placeType point=${point.lat},${point.lon} area=$areaMeters pop=$population " +
                "country=$country/$countryCode state=$state/$stateCode"
    }
}
