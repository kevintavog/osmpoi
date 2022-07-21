package osmpoi.indexer

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import kotlinx.serialization.Serializable

@JsonIgnoreProperties(ignoreUnknown = true)
@Serializable
data class PhotonResponse(
    val features: List<PhotonFeature>
)

@JsonIgnoreProperties(ignoreUnknown = true)
@Serializable
data class PhotonFeature(
    val properties: PhotonProperties
)

@JsonIgnoreProperties(ignoreUnknown = true)
@Serializable
data class PhotonProperties(
    val countryCode: String? = null,
    val country: String? = null,
    val state: String? = null,
    val city: String? = null
)
