package osmpoi.cvscomparer

import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.SearchResponse
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import io.ktor.utils.io.errors.*
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import okhttp3.OkHttpClient
import okhttp3.Request
import osmpoi.core.Elastic
import osmpoi.indexer.PhotonResponse
import osmpoi.models.LocationResponse
import osmpoi.models.OsmPoi
import java.io.StringReader
import kotlin.system.exitProcess

class ComparerMain {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            try {
                Comparer().main(args)
                exitProcess(0)
            } catch (t: Throwable) {
                t.printStackTrace()
                exitProcess(-1)
            }
        }
    }
}

class Comparer: CliktCommand() {
    private val elasticUrl: String? by option(
        "-e",
        "--elasticUrl",
        help="The URl for ElasticSearch")
    private val osmPoiUrl: String? by option(
        "-o",
        "--osmpoiUrl",
        help="The URl for the OSM POI service")
    private val photonUrl: String? by option(
        "-p",
        "--photonUrl",
        help="The URl for the Photon service - used to resolve city & country when using the OSM POI service")
    private val inputFilename: String by option(
        "-f",
        "--file",
        help="The name of the CVS file that will be read")
        .required()
    private val approvedFilename: String? by option(
        "-a",
        "--approved",
        help="The name of the CVS file that contains the approved OSM POI - these will be excluded from mismatches if they match these")
    private val mismatchDetails: Boolean by option(
        "-d",
        "--details",
        help="Provide details for mismatches")
        .flag()

    val osmClient = OkHttpClient()
    val photonClient = OkHttpClient()
    val json = Json { ignoreUnknownKeys = true }

    override fun run() {
        val isElastic = elasticUrl != null
        if (isElastic) {
            Elastic.url = elasticUrl!!
            Elastic.initIndex()
//        } else {
//            if (photonUrl == null || osmPoiUrl == null) {
//                throw Exception("Must specify OSM POI and Photon URL together")
//            }
        }

        val approved = mutableMapOf<String,String>()
        approvedFilename?.let { filename ->
            println("Reading approved file $filename")
            csvReader {
                quoteChar = '\''
                escapeChar = '|'
            }.open(filename) {
               var approvedRow = readNext()
                while (approvedRow != null) {
                    val oldNames = approvedRow[0]
                    val newNames = approvedRow[1]
                    approved[oldNames] = newNames
                    approvedRow = readNext()
                }
            }
        }

        var approvedCount = 0
        var matched = 0
        var failed = 0
        var uniqueFailed = 0
        var totalRows = 0
//        val mismatched = mutableMapOf<String,String>()
        csvReader().open(inputFilename) {
            var row = readNext()
            while (row != null) {
                totalRows += 1
                val lat = row[0]
                val lon = row[1]

//                val oldSites = row.drop(2)
                val singleOldName = row[2]    // Country code, country name, state, city, sites - nulls removed

//                val missingOld = mutableListOf<String>()
                val newNames = if (isElastic) elastic(lat, lon) else osmPoiService(lat, lon)
                val singleNewName = newNames.reversed().joinToString(",")
//                val newSites = if (isElastic) elastic(lat, lon) else osmPoiService(lat, lon)
                val approvedNewName = approved[singleOldName]
                if (approvedNewName == singleNewName) {
                    approvedCount += 1
                } else {
                    if (singleOldName != singleNewName) {
                        println("Mismatch $lat,$lon old='$singleOldName' new='$singleNewName'")
                        if (!isElastic && mismatchDetails) {
                            poiDetails(lat, lon).forEach { poi ->
                                println(" -> ${poi.id} name=${poi.name}")
                                println(" ---> tags=${poi.tags.map { "${it.key}=${it.value}" }}")
                            }
                        }
                        failed += 1
                        uniqueFailed += 1
                    } else {
                        matched += 1
                    }
//                    newSites.forEach { site ->
//                        site?.let {
//                            if (!oldSites.contains(it)) {
//                                missingOld.add(it)
//                            }
//                        }
//                    }
//                    val missingNew = mutableListOf<String>()
//                    oldSites.forEach { site ->
//                        if (!newSites.contains(site)) {
//                            missingNew.add(site)
//                        }
//                    }
//
//                    if (missingNew.isNotEmpty() || missingOld.isNotEmpty()) {
//                        val key = oldSites.joinToString(",") + ":" + newSites.joinToString(",")
//                        if (!mismatched.containsKey(key)) {
//                            mismatched[key] = "$lat,$lon"
//                            println("Mismatch $lat,$lon - old=$oldSites, new=$newSites")
//                            uniqueFailed += 1
//                        }
//                        failed += 1
//                    } else {
//                        matched += 1
//                    }
                }

                row = readNext()
            }
        }

        println("Read $totalRows, approved $approvedCount, matched $matched and $uniqueFailed failed ($failed total failed)")
    }

    private fun photonOsmPoiService(lat: String, lon: String): List<String?> {
        val list = photonService(lat, lon).filterNotNull().reversed()
        return osmPoiService(lat, lon) + list
    }

    private fun osmPoiService(lat: String, lon: String): List<String?> {
        val request = Request.Builder()
            .url("${osmPoiUrl}/pois?lat=$lat&lon=$lon")
            .build()
        osmClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) throw IOException("Unexpected code $response")
            val osmResponse = Json.decodeFromString<LocationResponse>(response.body!!.string())
            val names: MutableList<String?> = osmResponse.inside.map { it.name }.toMutableList()
//            names.addAll(osmResponse.nearby.map { it.name })
            names.addAll(listOf(
                osmResponse.cityName,
                normalizeStateName(osmResponse.stateName),
                normalizeCountryName(osmResponse.countryName)))
            return names.filterNotNull()
        }
    }

    private fun normalizeCountryName(name: String?): String? {
        return name?.let {
            when(it) {
                "United Kingdom" -> "UK"
                "United States", "United States of America (Kaua'i, Ni'ihau, Ka'ula)" -> "USA"
                else -> it
            }
        }
    }

    private fun normalizeStateName(name: String?): String? {
        return name?.let {
            when(it) {
                "Alberta" -> "AB"
                "British Columbia" -> "BC"
                else -> it
            }
        }
    }

    // Return countryCode, country, state & city. null is used for missing components
    private fun photonService(lat: String, lon: String): List<String?> {
        val request = Request.Builder()
            .url("${photonUrl}/reverse?lang=en&lat=$lat&lon=$lon")
            .build()
        photonClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) throw IOException("Unexpected Photon code $response")
            val photonResponse = json.decodeFromString<PhotonResponse>(response.body!!.string())
            photonResponse.features.firstOrNull()?.let { feature ->
                val state = when(feature.properties.countryCode) {
                    "CAN", "GBR", "USA" -> feature.properties.state
                    else -> null
                }
                return transformNullNames(listOf(feature.properties.countryCode, feature.properties.country,
                    state, feature.properties.city))
            }

println("No Photon features returned for $lat,$lon")
            return listOf(null, null, null, null)
        }
    }

    private fun poiDetails(lat: String, lon: String): List<OsmPoi> {
        val request = Request.Builder()
            .url("${osmPoiUrl}/location?lat=$lat&lon=$lon&admin=true&no_admin_filter=true")
            .build()
        osmClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) throw IOException("Unexpected code $response")
            val osmResponse = Json.decodeFromString<LocationResponse>(response.body!!.string())
            return osmResponse.inside
        }
    }

    private fun elastic(lat: String, lon: String): List<String?> {
        val intersectsRequest = SearchRequest.Builder()
            .index(Elastic.poiIndexName)
            .query { q -> q
                .geoShape { gs -> gs
                    .withJson(
                        StringReader(
                            """
                                {
                                    "location": {
                                        "shape": {
                                            "type": "envelope",
                                            "coordinates": [
                                                [
                                                    $lon,
                                                    $lat
                                                ],
                                                [
                                                    $lon,
                                                    $lat
                                                ]
                                            ]
                                        },
                                        "relation": "intersects"
                                    }
                                }                
                                """.trimIndent()
                        )
                    )
                }
            }
            .build()
        val response: SearchResponse<OsmPoi> = Elastic.client.search(intersectsRequest, OsmPoi::class.java)
        return response.hits().hits().map { hit ->
            hit.source()?.name
        }
    }

    private fun transformNullNames(list: List<String?>): List<String?> {
        return list.map { if (it == null) null else transformName(it) }
    }

    private fun transformName(name: String): String {
        return when(name) {
            "Alberta" -> "AB"
            "British Columbia" -> "BC"

            "California" -> "CA"
            "Maine" -> "ME"
            "New York" -> "NY"
            "Washington" -> "WA"

            "United Kingdom" -> "UK"
            "United States" -> "USA"
            else -> name
        }
    }
}
