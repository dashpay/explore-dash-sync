package org.dash.mobile.explore.sync

import com.google.cloud.functions.BackgroundFunction
import com.google.cloud.functions.Context
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.io.File


class Function : BackgroundFunction<PubSubMessage?> {

    private val logger = LoggerFactory.getLogger(Function::class.java)

    @FlowPreview
    override fun accept(message: PubSubMessage?, context: Context) {

        val version = javaClass.getPackage().implementationVersion
        logger.info("Dash Explore Sync ver. $version")

//        logger.info("message=$message")
//
//        val args = message?.run {
//            String(Base64.getDecoder().decode(message.data))
//        } ?: return
//
//        logger.info("${message}\t(data=$args)")

        //{"data":"c3JjPXByb2QgZHN0PWRldg==", "attributes":[["atr1","val1"],["atr2","val2"]]}
        //{"attributes":[["mode","testnet"]]}
        //gcloud pubsub topics publish dash-explore-sync-trigger --message="src=prod dst=dev" --attribute=src=prod,dst=dev

        val mode = when(message?.attributes?.get("mode")) {
            "prod" -> OperationMode.PRODUCTION
            "testnet" -> OperationMode.TESTNET
            "devnet" -> OperationMode.DEVNET
            else -> throw IllegalArgumentException("unknown mode ${message?.attributes}")
        }

        logger.info("mode: $mode")

        try {
            runBlocking {
                launch(Dispatchers.IO) {
                    SyncProcessor(mode).syncData(
                        File("/tmp"),
                        srcDev = false,
                        forceUpload = false
                    )
                }
            }
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