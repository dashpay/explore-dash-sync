package org.dash.mobile.explore.sync

import com.google.cloud.functions.BackgroundFunction
import com.google.cloud.functions.Context
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.dash.mobile.explore.sync.process.DashDirectApiMode
import org.slf4j.LoggerFactory
import java.io.File

class Function : BackgroundFunction<PubSubMessage?> {

    private val logger = LoggerFactory.getLogger(Function::class.java)

    @FlowPreview
    override fun accept(message: PubSubMessage?, context: Context) {
        val version = javaClass.getPackage().implementationVersion
        logger.info("Dash Explore Sync ver. $version")

        val mode = when (message?.attributes?.get("mode")) {
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
                        apiMode = /*if (mode == OperationMode.TESTNET) {
                            DashDirectApiMode.STAGING
                        } else {*/
                            DashDirectApiMode.PROD,
//                        },
                        forceUpload = false,
                        quietMode = false // we need notifications
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
