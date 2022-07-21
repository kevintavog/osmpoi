package osmpoi.service

import io.ktor.server.application.*
import io.ktor.server.application.hooks.*
import io.ktor.server.request.*
import io.ktor.util.*
import mu.KotlinLogging
import osmpoi.models.LocationResponse
import osmpoi.models.PoiResponse

private val logger = KotlinLogging.logger {}

val RangicLoggingPlugin = createApplicationPlugin(name = "RangicLoggingPlugin") {
    val startTimeKey = AttributeKey<Long>("rangicLogging_startTime_Key")
    val responseTimeKey = AttributeKey<Long>("rangicLogging_responseTime_Key")
    val itemCountKey = AttributeKey<Long>("rangicLogging_itemCount_Key")

    onCall { call ->
        call.attributes.put(startTimeKey, System.nanoTime())
    }

    on(ResponseSent) { call ->
        var callDurationMsecs = -1L
        call.attributes.getOrNull(startTimeKey)?.let {
            callDurationMsecs = (System.nanoTime() - it) / 1000L / 1000L
        }
        var itemCount = -1L
        call.attributes.getOrNull(itemCountKey)?.let {
            itemCount = it
        }
        var sendResponseMsecs = -1L
        call.attributes.getOrNull(responseTimeKey)?.let {
            sendResponseMsecs = (System.nanoTime() - it) / 1000L / 1000L
        }

        val statusCode = call.response.status()?.value ?: -1
        val status = call.response.status()?.description ?: "Unknown"
        val method = call.request.local.method.value
        val path = call.request.path()
        val query = call.request.queryString()
        val length = call.response.headers["Content-Length"] ?: "Unknown"

        val message = "statusCode=$statusCode status=$status method=$method path=$path " +
                "query=$query bytesOut=$length items=$itemCount durationMs=${callDurationMsecs - sendResponseMsecs} " +
                "sendResponseMs=$sendResponseMsecs"
        if (statusCode >= 400) {
            logger.warn { message }
        } else {
            logger.info { message }
        }
    }

    onCallRespond { call ->
        call.attributes.put(responseTimeKey, System.nanoTime())
        var itemsReturned = -1
        transformBody { data ->
            when (data) {
                is LocationResponse -> { itemsReturned = 1 }
                is ApiLocationResponse -> { itemsReturned = data.items.size }
                is PoiResponse -> { itemsReturned = data.pois.size }
                else -> { println("data type is ${data.javaClass.simpleName}") }
            }
            data
        }
        call.attributes.put(itemCountKey, itemsReturned.toLong())
    }
}
