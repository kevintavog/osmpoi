package osmpoi.models

import GeoJsonGeometry
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class WofJsonItem(
    val id: Long,
    val properties: WofJsonItemProperties,
    val geometry: GeoJsonGeometry
){
//    var resolvedName: String = properties.nameEnglishPreferred?.first()
//        ?: properties.nameEnglishVariant?.first()
//        ?: properties.wofName
    var resolvedName: String = properties.wofName
}

@Serializable
data class WofJsonItemProperties(
//    @SerialName("name:eng_x_preferred")
//    val nameEnglishPreferred: List<String>? = null,
//    @SerialName("name:eng_x_variant")
//    val nameEnglishVariant: List<String>? = null,
    @SerialName("wof:id")
    val wofId: Long? = null,
    @SerialName("geom:area_square_m")
    val geomAreaMeters: Double? = null,
    @SerialName("geom:bbox")
    val geomBbox: String?,
    @SerialName("geom:latitude")
    val geomLatitude: Double,
    @SerialName("geom:longitude")
    val geomLongitude: Double,
    @SerialName("wof:lastmodified")
    val wofLastModified: Long,
    @SerialName("wof:name")
    val wofName: String,
    @SerialName("wof:placetype")
    val placeType: String,
    @SerialName("wof:population")
    val wofPopulation: Long? = null,
    @SerialName("wof:population_rank")
    val wofPopulationRank: Int? = null,

    @SerialName("mz:is_current")
    val mzIsCurrent: Int? = null,

    @SerialName("wof:country")
    val wofCountry: String? = null,
    @SerialName("wof:country_alpha3")
    val wofCountryCode: String? = null,
    @SerialName("wof:shortcode")
    val wofShortCode: String? = null,

    @SerialName("wof:hierarchy")
    val hierarchy: List<WofJsonHierarchy>,
)

@Serializable
data class WofJsonHierarchy(
    @SerialName("country_id")
    val countryId: Long? = null,
    @SerialName("macroregion_id")
    val macroRegionId: Long? = null,       // State/Province
    @SerialName("region_id")
    val regionId: Long? = null,            // State/Province
)
