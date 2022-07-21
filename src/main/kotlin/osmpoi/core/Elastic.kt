package osmpoi.core

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.ElasticsearchException
import co.elastic.clients.elasticsearch._types.mapping.Property
import co.elastic.clients.json.JsonData
import co.elastic.clients.json.JsonpSerializable
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.json.jsonb.JsonbJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestClientBuilder
import java.io.ByteArrayOutputStream
import java.io.StringReader

class Elastic {
    companion object {
        const val poiIndexName = "osmpoi"
        const val wofIndexName = "wof-cities"

        var socketTimeout = 30 * 1000
        var url: String = ""

        val client: ElasticsearchClient by lazy {
            ElasticsearchClient(
                RestClientTransport(
                    RestClient.builder(HttpHost.create(url))
                        .setRequestConfigCallback(
                            RestClientBuilder.RequestConfigCallback(
                                fun(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder {
                                    return requestConfigBuilder
                                        .setSocketTimeout(socketTimeout)
                                }
                            ))
                        .build(), JacksonJsonpMapper()
                )
            )
        }

        fun initIndex() {
            println("ElasticSearch server: ${client.info().version().number()}")
            createOsmPoiIndex()
            createWofCitiesIndex()
        }

        private fun createOsmPoiIndex() {
            if (client.indices().exists { r -> r.index(poiIndexName) }?.value() == true) {
                return
            }
            println("Creating index '$poiIndexName'")
            client.indices().create { r -> r
                .index(poiIndexName)
                .settings { s -> s
                    .numberOfReplicas("0")
                    .maxResultWindow(100000)
                    .numberOfShards("1")
                }
                .mappings { m -> m
                    .properties("location", Property.of { p -> p.geoShape { g -> g} })
                    .properties("point", Property.of { p -> p.geoPoint { g -> g } })
                    .properties("dateIndexed", Property.of { p -> p.date { d -> d.format("yyyy-MM-dd'T'HH:mm:ssZZZZZ") }})
                }
            }
        }

        private fun createWofCitiesIndex() {
            if (client.indices().exists { r -> r.index(wofIndexName) }?.value() == true) {
                return
            }
            println("Creating index '$wofIndexName'")
            client.indices().create { r -> r
                .index(wofIndexName)
                .settings { s -> s
                    .numberOfReplicas("0")
                    .maxResultWindow(100000)
                    .numberOfShards("1")
                }
                .mappings { m -> m
                    .properties("shape", Property.of { p -> p.geoShape { g -> g} })
                    .properties("point", Property.of { p -> p.geoPoint { g -> g } })
                    .properties("dateIndexed", Property.of { p -> p.date { d -> d.format("yyyy-MM-dd'T'HH:mm:ssZZZZZ") }})
                }
            }
        }

        fun esObjectToString(request: JsonpSerializable): String {
            val baos = ByteArrayOutputStream()
            client._transport().jsonpMapper().jsonProvider().createGenerator(baos).use {
                request.serialize(it, client._transport().jsonpMapper())
            }
            return baos.toString()
        }

        fun toJsonData(json: String): JsonData {
            val mapper = JsonbJsonpMapper()
            val parser = mapper.jsonProvider().createParser(StringReader(json))
            return JsonData.from(parser, mapper)
        }

        fun toReadable(t: Throwable): String {
            var message = "${t.javaClass}: "
            if (t is ElasticsearchException) {
                var error = t.response().error()
                while (error != null) {
                    message += "type=${error.type()} reason=${error.reason()}; "
                    error.rootCause().forEach { er ->
                        message += "root-type: ${er.type()} root-reason: ${er.reason()}"
                    }
                    error = error.causedBy()
                }
            } else {
                message += "${t.message} "
            }
            t.cause?.let {
                message += "(${toReadable(it)}) "
            }
            return message
        }
    }
}
