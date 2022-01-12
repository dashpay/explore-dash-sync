package org.dash.mobile.explore.sync

import com.google.cloud.functions.BackgroundFunction
import com.google.cloud.functions.Context
import mu.KotlinLogging
import java.util.*

private val logger = KotlinLogging.logger {}

class Function : BackgroundFunction<PubSubMessage> {

    override fun accept(message: PubSubMessage?, context: Context) {
        val data = message?.run {
            String(Base64.getDecoder().decode(message.data))
        } ?: "Hello World!"

        logger.info(data)

        logger.info("start sync")
        SyncProcessor().syncData(false)
        logger.info("sync finished")
    }
}

class PubSubMessage {
    var data: String? = null
    var attributes: Map<String, String>? = null
    var messageId: String? = null
    var publishTime: String? = null
}