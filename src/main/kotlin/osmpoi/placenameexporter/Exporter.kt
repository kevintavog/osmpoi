package osmpoi.placenameexporter

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import io.ktor.utils.io.errors.*
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import osmpoi.core.Elastic
import kotlin.system.exitProcess

class ExporterMain {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            try {
                Exporter().main(args)
                exitProcess(0)
            } catch (t: Throwable) {
                println("Exception: ${Elastic.toReadable(t)}")
                t.printStackTrace()
                exitProcess(-1)
            }
        }
    }
}

class Exporter: CliktCommand() {
    private val reverseNameUrl: String by option(
        "-r",
        "--reversename",
        help="The URl for the ReverseName service")
        .required()
    private val inputFilename: String by option(
        "-i",
        "--input",
        help="The name of the input file, a CSV of latitude,longitude values")
        .required()
    private val outputFilename: String by option(
        "-o",
        "--output",
        help="The name of the output file that will be written")
        .required()

    val batchSize = 100
    val reverseNameClient = OkHttpClient()
    val json = Json { ignoreUnknownKeys = true }

    override fun run() {
        var locationsUsed = 0
        val placenamesToLocation = mutableMapOf<String,PlacenameLocation>()
        csvReader().open(inputFilename) {
            var row = readNext()
            val reverseNameRequests = mutableListOf<NameRequest>()
            while (row != null) {
                reverseNameRequests.add(NameRequest(row[0].toDouble(), row[1].toDouble()))
                locationsUsed += 1
                if (reverseNameRequests.size > batchSize) {
                    reverseNameCache(reverseNameRequests).forEach { r ->
                        if (r?.location != null) {
                            val list = listOfNotNull(r.countryName, r.state, r.city) + (r.sites?.toList() ?: emptyList())
                            val key = list.joinToString(",")
                            placenamesToLocation[key] = r.location
                        }
                    }
                    reverseNameRequests.clear()
                }
                row = readNext()
            }
            reverseNameCache(reverseNameRequests).forEach { r ->
                if (r?.location != null) {
                    val list = listOfNotNull(r.countryName, r.state, r.city) + (r.sites?.reversed()?.toList() ?: emptyList())
                    val key = list.joinToString { "," }
                    placenamesToLocation[key] = r.location
                }
            }
        }

        println("Of $locationsUsed locations, there are ${placenamesToLocation.size} unique place names")
        // Read the input,
        //  For each row (in bulk)
        //      invoke ReverseName
        //      add sites, city, state, country (code, name or both?) as key to lat,lon
        //  Write each item in the map to a CSV

        csvWriter().open(outputFilename, false) {
            placenamesToLocation.forEach { kv ->
                val row = listOf(kv.value.lat.toString(), kv.value.lon.toString(), kv.key)
                writeRow(row)
            }
        }
    }

    private fun reverseNameCache(list: List<NameRequest>): List<Placename?> {
        if (list.isEmpty()) { return emptyList() }

        val request = ReverseNameRequest(list, 5.0)
        reverseNameClient.newCall(Request.Builder()
            .url("${reverseNameUrl}/cached-names")
            .post(json.encodeToString(request).toRequestBody("application/json; charset=utf-8".toMediaType()))
            .build()).execute().use { it ->
            if (!it.isSuccessful) throw IOException("Unexpected code $it")
            val fullResponse = json.decodeFromString<ReverseNamesResponse>(it.body!!.string())
            return fullResponse.items.map { item -> item.placename }
        }
    }

    private fun reverseNameDiagnostics(list: List<NameRequest>): List<ResolverFields?> {
        if (list.isEmpty()) { return emptyList() }

        val request = ReverseNameRequest(list, 5.0)
        reverseNameClient.newCall(Request.Builder()
            .url("${reverseNameUrl}/diagnostics/names")
            .post(json.encodeToString(request).toRequestBody("application/json; charset=utf-8".toMediaType()))
            .build()).execute().use { it ->
            if (!it.isSuccessful) throw IOException("Unexpected code $it")
            val fullResponse = json.decodeFromString<ReverseNamesResponse>(it.body!!.string())
            return fullResponse.items.map { item -> item.overpassResults }
        }
    }
}
