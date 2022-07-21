package osmpoi.service

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.cors.routing.*
import io.ktor.server.routing.*
import kotlinx.serialization.json.Json
import osmpoi.core.Elastic

fun main(args: Array<String>) {
    Service().main(args)
}

class Service: CliktCommand() {
    val elasticUrl: String by option(
        "-e",
        "--elasticUrl",
        help="The URl for ElasticSearch")
        .required()
    val port: Int by option(
        "-p",
        "--port",
        help="The port to listen to")
        .int()
        .default(5000)


    override fun run() {
        Elastic.url = elasticUrl
        Elastic.socketTimeout = 3 * 60 * 1000
        Elastic.initIndex()

        embeddedServer(Netty, port = port) {
            Service().apply { main() }
        }.start(wait = true)
    }

    fun Application.main() {
        install(RangicLoggingPlugin)
        install(CORS) {
            allowMethod(HttpMethod.Delete)
            allowMethod(HttpMethod.Get)
            allowMethod(HttpMethod.Options)
            allowMethod(HttpMethod.Patch)
            allowMethod(HttpMethod.Post)
            allowMethod(HttpMethod.Put)

            allowNonSimpleContentTypes = true
            anyHost()
            allowHost("localhost:8080")
        }

        install(ContentNegotiation) {
            json(Json {
                prettyPrint = true
                isLenient = true
            })
        }

        routing {
            area()
            pois()
            shapes()
            textSearch()
        }
    }
}
