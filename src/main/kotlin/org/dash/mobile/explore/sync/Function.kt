package org.dash.mobile.explore.sync

import com.google.cloud.functions.BackgroundFunction
import com.google.cloud.functions.Context
import org.slf4j.LoggerFactory
import java.util.*


class Function : BackgroundFunction<PubSubMessage?> {

    private val logger = LoggerFactory.getLogger(Function::class.java)

    override fun accept(message: PubSubMessage?, context: Context) {

        val version = javaClass.getPackage().implementationVersion
        logger.info("Dash Explore Sync ver. $version")

        logger.info("message=$message")

        val args = message?.run {
            String(Base64.getDecoder().decode(message.data))
        } ?: return

        logger.info("${message}\t(data=$args)")

        //{"data":"c3JjPXByb2QgZHN0PWRldg==", "attributes":[["atr1","val1"],["atr2","val2"]]}
        //gcloud pubsub topics publish dash-explore-sync-trigger --message="src=prod dst=dev" --attribute=src=prod,dst=dev

        try {
            SyncProcessor("/tmp/$OUTPUT_FILE")
                .syncData(
                    forceArchive = false,
                    upload = true,
                    srcDev = false
                )
        } catch (ex: Exception) {
            logger.alert(ex.message, ex)
        }
    }

}

data class PubSubMessage(
    var data: String? = null,
    var attributes: Map<String, String>? = null,
    var messageId: String? = null,
    var publishTime: String? = null
)