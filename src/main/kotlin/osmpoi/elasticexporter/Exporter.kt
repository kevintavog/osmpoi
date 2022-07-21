package osmpoi.elasticexporter

import co.elastic.clients.elasticsearch._types.Time
import co.elastic.clients.elasticsearch.core.OpenPointInTimeRequest
import co.elastic.clients.elasticsearch.core.SearchRequest
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import osmpoi.core.Elastic
import osmpoi.models.OsmPoi
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
                println(Elastic.toReadable(t))
                t.printStackTrace()
                exitProcess(-1)
            }
        }
    }
}

class Exporter: CliktCommand() {
    private val elasticUrl: String? by option(
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
        Elastic.url = elasticUrl!!
        Elastic.initIndex()

        var hitsReturned = 0
        var lastDocumentSort = ""
        var lastDocumentShardId = ""
        val lastPitId = Elastic.client.openPointInTime(
            OpenPointInTimeRequest.Builder()
            .index(Elastic.poiIndexName)
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
                val response = Elastic.client.search(request.build(), OsmPoi::class.java)
                hitsReturned = response.hits().hits().size
                response.hits().hits().forEach { hit ->
                    hit.source()?.let { source ->
                        if (source.location.contains("geometrycollection", true)) {
//                        val row = mutableListOf(source.id, source.name, source.tags.map { "${it.key}=${it.value}" })
                            val row = mutableListOf(source.id, source.name, source.location)
                            writeRow(row)
                        }
                        lastDocumentSort = hit.sort().first()
                        lastDocumentShardId = hit.sort()[1]
                    }
                }
            } while (hitsReturned > 0)
        }
    }

    private fun createSearchRequest(): SearchRequest.Builder {
        return SearchRequest.Builder()
            .withJson(
                StringReader(
                """
                {
                    "size": 100,
                    "sort": [
                        {
                            "id.keyword": {
                                "order": "asc"
                            }
                        },
                        {"_shard_doc": "desc"}
                    ],
                    "query": {
                        "bool": {
                            "must_not": [
                                {
                                    "term": {
                                        "tags.key.keyword": "admin_level"
                                    }
                                }
                            ]
                        }
                    }
                }
                """.trimIndent()
            )
        )
    }
}

/*
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "tags.key.keyword": "boundary"
                                    }
                                },
                                {
                                    "term": {
                                        "tags.value.keyword": "protected_area"
                                    }
                                },
                                {
                                    "term": {
                                        "tags.key.keyword": "type"
                                    }
                                },
                                {
                                    "term": {
                                        "tags.value.keyword": "boundary"
                                    }
                                }
                            ],
                            "filter": {
                                "bool": {
                                    "must_not": {
                                        "term": {
                                            "tags.value.keyword": "nature_reserve"
                                        }
                                    }
                                }
                            }
                        }

 */