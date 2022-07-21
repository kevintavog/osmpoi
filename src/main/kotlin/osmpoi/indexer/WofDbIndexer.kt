package osmpoi.indexer

import co.elastic.clients.elasticsearch._types.ErrorCause
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import org.elasticsearch.client.ResponseException
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import osmpoi.core.Elastic
import osmpoi.models.GeoPoint
import osmpoi.models.WofEntity
import osmpoi.models.WofJsonItem
import java.io.File
import java.lang.Integer.min
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

class WofDbIndexerMain {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            try {
                val indexer = WofDbIndexer()
                indexer.main(args)
                exitProcess(indexer.errorCode)
            } catch (t: Throwable) {
                t.printStackTrace()
                exitProcess(-1)
            }
        }
    }
}

class WofDbIndexer: CliktCommand() {
    companion object {
        const val indexFailuresFilename = "IndexFailures.log"
    }

    private val elasticBatchSize: Int by option(
        "-b",
        "--batchSize",
        help = "The batch size used for ElasticSearch")
        .int()
        .default(200)
    private val elasticUrl: String by option(
        "-e",
        "--elasticUrl",
        help = "The URl for ElasticSearch")
        .required()
    private val dbFilename: String by option(
        "-d",
        "--dbFilename",
        help = "The SQLite filename")
        .required()

    var errorCode = 0
    val json = Json { ignoreUnknownKeys = true }

    val databaseBatchSize = 10000

    val countryDtoItems = mutableMapOf<Long,WofEntity>()
    val stateDtoItems = mutableMapOf<Long,WofEntity>()
    val docList = mutableListOf<WofEntity>()

    var indexErrors = 0
    var itemsIndexed = 0
    var indexExceptions = 0
    var countRetryAttempts = 0

    val emitCount = 50_000L
    var nextEmitLineCount = emitCount

    var totalItems = 0L
    var countryCount = 0L
    var stateCount = 0L
    var cityCount = 0L
    var citiesNoPopulation = 0L

    val supportedCountryCodes = setOf(
        "AL", "AT", "AU", "BE", "CA", "CZ", "DE", "DK", "ES", "FI", "FR", "GB", "GG", "GI", "GL", "GR", "HR",
        "HU", "IE", "IS", "IT", "JE", "LI", "LU", "MC", "MX", "NL", "NO", "NZ", "PL", "PT", "SK", "SM", "TR",
        "US", "VA",
    )

    override fun run() {
        val dbFile = File(dbFilename)
        if (!dbFile.exists() || !dbFile.isFile) {
            logger.error { "No such file: $dbFilename" }
            errorCode = -1
            return
        }

        File(indexFailuresFilename).delete()

        Elastic.url = elasticUrl
        Elastic.socketTimeout = 3 * 60000
        Elastic.initIndex()

        val db = Database.connect("jdbc:sqlite:${dbFile.absolutePath}", "org.sqlite.JDBC")
        transaction(db) {
            supportedCountryCodes.forEach { countryCode ->
                logger.info { "Processing $countryCode" }
                // Countries & States are needed for Cities
                val countryIds = mutableListOf<Long>()
                SprTable
                    .select { SprTable.placeType eq "country" and(SprTable.country eq countryCode) }
                    .forEach {
                        if (it[SprTable.isCurrent] > 0 && it[SprTable.isDeprecated] == 0) {
                            countryIds.add(it[SprTable.id])
                            countryCount += 1
                            totalItems += 1
                        }
                    }

                // Transform and index
                getJsonItems(countryIds).forEach {
                    val dto = toDto(it)
                    countryDtoItems[dto.id] = dto
                }

                // States
                var offset = 0L
                do {
                    val stateIds = mutableListOf<Long>()
                    SprTable.select {
                            (SprTable.country eq countryCode)
                            .and (SprTable.isCurrent eq 1)
                            .and (SprTable.isDeprecated eq 0)
                            .and(SprTable.placeType eq "region" or(SprTable.placeType eq "macroregion"))
                        }.limit(databaseBatchSize, offset)
                        .forEach {
                            stateIds.add(it[SprTable.id])
                            stateCount += 1
                            totalItems += 1
                        }

                    // Transform and index
                    getJsonItems(stateIds).forEach {
                        val dto = toDto(it)
                        stateDtoItems[dto.id] = dto
                    }
                    offset += stateIds.size
                } while (stateIds.size > 0)

                // Cities
                offset = 0
                do {
                    var countReturned = 0
                    val cityIds = mutableListOf<Long>()
                    SprTable
                        .select {
                            (SprTable.country eq countryCode)
                            .and(SprTable.isCurrent neq 0)
                            .and(SprTable.isDeprecated neq 1)
                            .and(SprTable.isSuperseded neq 1)
                            .and(SprTable.placeType eq "locality") }
                        .limit(databaseBatchSize, offset)
                        .forEach {
                            countReturned += 1
                            cityIds.add(it[SprTable.id])
                        }

                    // Transform and index
                    getJsonItems(cityIds).forEach {
                        totalItems += 1
                        if (it.properties.mzIsCurrent == 1 || (it.properties.wofPopulation ?: 0) > 1) {
                            cityCount += 1
                            toDto(it)
                        } else {
                            citiesNoPopulation += 1
                        }
                    }

                    offset += countReturned
                } while (countReturned > 0)
            }
        }

        indexDocuments(docList, 3)
        logger.info { "Done, indexed $itemsIndexed, there were $indexErrors index errors. " +
            "$countryCount countries, $stateCount states & $cityCount cities (skipped $citiesNoPopulation with zero population)" }
    }

    private fun toDto(item: WofJsonItem): WofEntity {
        val hierarchy = item.properties.hierarchy.first()

        var country = ""
        var countryCode = ""
        var state: String? = null
        var stateCode: String? = null
        when(item.properties.placeType) {
            "country" -> {
                country = item.properties.wofCountry ?: ""
                countryCode = item.properties.wofCountryCode ?: ""
            }
            "region", "macroregion" -> {
                state = item.resolvedName
                stateCode = item.properties.wofShortCode
            }
        }
        hierarchy.countryId?.let { id ->
            countryDtoItems[id]?.let { dto ->
                country = dto.country
                countryCode = dto.countryCode
            }
        }

        hierarchy.macroRegionId?.let { id ->
            stateDtoItems[id]?.let { dto ->
                state = dto.state
                stateCode = dto.stateCode
            }
        }
        hierarchy.regionId?.let { id ->
            stateDtoItems[id]?.let { dto ->
                state = dto.state
                stateCode = dto.stateCode
            }
        }

        val dto = WofEntity(
            item.id,
            item.resolvedName,
            item.properties.placeType,
            GeoPoint(item.properties.geomLatitude, item.properties.geomLongitude),
            item.properties.geomAreaMeters ?: 0.0,
            item.properties.wofPopulation,
            item.properties.wofPopulationRank,
            country,
            countryCode,
            state,
            stateCode,
            GeoJsonToWkt.convert(item.geometry, true)
        )

        docList.add(dto)
        if (docList.size >= elasticBatchSize) {
            indexDocuments(docList, 3)
            docList.clear()
        }

        if (itemsIndexed >= nextEmitLineCount) {
            nextEmitLineCount += emitCount
            logger.info { "Indexed $itemsIndexed, there were $indexErrors index errors. " +
                    "$countryCount countries, $stateCount states & $cityCount cities" }
        }
        return dto
    }

    private fun getJsonItems(list: List<Long>): List<WofJsonItem> {
        val result = mutableListOf<WofJsonItem>()

        // Limit how many database lookups we do, but do all of them before returning
        val lookupSize = 1000
        var index = 0
        while (index < list.size) {
            val slice = list.subList(index, min(index + lookupSize, list.size))
            index += lookupSize
            result.addAll(GeoJsonTable
                .select { GeoJsonTable.id inList slice and(GeoJsonTable.isAlt eq 0) }
                .map { json.decodeFromString(WofJsonItem.serializer(), it[GeoJsonTable.body]) })
        }
        return result
    }

    private fun indexDocuments(docs: List<WofEntity>, numRetries: Int) {
        if (docs.isEmpty()) {
            return
        }

        val req: List<BulkOperation> = docs.map { wof ->
            BulkOperation.of { r ->
                r.index<WofEntity> { s -> s
                    .index(Elastic.wofIndexName)
                    .id(wof.id.toString())
                    .document(wof)
                }
            }
        }

        try {
            val response = Elastic.client.bulk { b ->
                b.operations(req)
            }
            response.items().forEachIndexed { index, item ->
                item.error()?.let {
                    indexErrors += 1
                    var message = ""
                    var error: ErrorCause? = it
                    while (error != null) {
                        message += "type=${error.type()} reason=${error.reason()}; "
                        error.rootCause().forEach { er ->
                            message += "root-type: ${er.type()} root-reason: ${er.reason()}"
                        }
                        error = error.causedBy()
                    }

                    File(indexFailuresFilename).appendText("$message\n --> ${docs[index]}\n")
                } ?: run {
                    itemsIndexed += 1
                }
            }
        } catch (t: Throwable) {
            File(indexFailuresFilename).appendText("Index exception: $t\n")

            // Elastic exceptions don't have much useful info - but we are not going to retry the entire list.
            // Instead, we will try one item at a time to find which documents are problematic
            if (t is ResponseException) {
                if (docs.size == 1) { throw t }
                logger.warn("Elastic exception, trying one doc at a time")
                for (d in docs) {
                    try {
                        indexDocuments(listOf(d), numRetries - 1)
                    } catch (innerT: Throwable) {
                        indexExceptions += 1
                        // This document cannot be indexed, give up on it
                        indexErrors += 1
                        File(indexFailuresFilename).appendText("${t.message}\n --> $d\n")
                    }
                }
            } else {
                indexExceptions += 1
                if (numRetries > 0) {
                    countRetryAttempts += 1
                    indexDocuments(docs, numRetries - 1)
                } else {
                    throw t
                }
            }
        }
    }
}

object SprTable: Table() {
    val id: Column<Long> = long("id").autoIncrement()
    val name: Column<String> = text("name")
    val placeType: Column<String> = text("placetype")
    val country: Column<String> = text("country")
    val isSuperseded: Column<Int> = integer("is_superseded")
    val isCurrent: Column<Int> = integer("is_current")
    val isDeprecated: Column<Int> = integer("is_deprecated")
}

object GeoJsonTable: Table() {
    val id: Column<Long> = long("id").autoIncrement()
    val body: Column<String> = text("body")
    val isAlt: Column<Int> = integer("is_alt")
}