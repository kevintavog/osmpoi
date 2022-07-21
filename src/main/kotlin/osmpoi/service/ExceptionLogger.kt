package osmpoi.service

import org.slf4j.LoggerFactory
import osmpoi.core.Elastic

private val logger = LoggerFactory.getLogger("api")

fun logException(t: Throwable, message: String = "") {
    logger.warn("Exception ${Elastic.toReadable(t)} $message")
}
