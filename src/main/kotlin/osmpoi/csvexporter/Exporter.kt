package osmpoi.csvexporter

import co.elastic.clients.elasticsearch._types.Time
import co.elastic.clients.elasticsearch.core.OpenPointInTimeRequest
import co.elastic.clients.elasticsearch.core.SearchRequest
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import osmpoi.core.Elastic
import java.io.StringReader
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
    private val elasticUrl: String by option(
        "-e",
        "--elasticUrl",
        help="The URl for ElasticSearch")
        .required()
    private val outputFilename: String by option(
        "-o",
        "--output",
        help="The name of the output file that will be written")
        .required()

    override fun run() {
        Elastic.url = elasticUrl
        Elastic.initIndex()

        val indexName = "placename_cache"

        var hitsReturned = 0
        var lastDocumentSort = ""
        var lastDocumentShardId = ""
        val lastPitId = Elastic.client.openPointInTime(OpenPointInTimeRequest.Builder()
            .index(indexName)
            .keepAlive(Time.of { b -> b.time("1m")})
            .build()
        ).id()
        csvWriter().open(outputFilename, false) {
            do {
                val request = createSearchRequest()
                request.pit { p -> p.id(lastPitId).keepAlive { k -> k.time("1m") } }
                if (hitsReturned > 0) {
                    request.searchAfter(lastDocumentSort, lastDocumentShardId)
                }
                val response = Elastic.client.search(request.build(), Placename::class.java)
                hitsReturned = response.hits().hits().size
                response.hits().hits().forEach { hit ->
                    hit.source()?.let { source ->
                        val row = mutableListOf(source.location.lat.toString(), source.location.lon.toString())
//                        source.sites.forEach { row.add(it) }
                        writeRow(row)
                        lastDocumentSort = hit.sort().first()
                        lastDocumentShardId = hit.sort()[1]
                    }
                }
            } while (hitsReturned > 0)
        }
    }

    private fun createSearchRequest(): SearchRequest.Builder {
        return SearchRequest.Builder()
            .withJson(StringReader(
                """
                {
                    "size": 100,
                    "sort": [
                        {
                            "dateCreated": {
                                "order": "asc",
                                "format": "strict_date_optional_time_nanos",
                                "numeric_type": "date_nanos"
                            }
                        },
                        {"_shard_doc": "desc"}
                    ],
                    "query": {
                        "match_all": { }
                    }
                }
                """.trimIndent()
            )
        )

    }
}
