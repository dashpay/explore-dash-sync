package org.dash.mobile.explore.sync

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.dash.mobile.explore.sync.process.DashDirectApiMode
import java.io.File
import kotlin.system.exitProcess

const val DEV_MODE_ARG = "-dev"
const val STAGING_MODE_ARG = "-staging"
const val UPLOAD_ARG = "-upload"
const val QUIET_ARG = "-quiet"

@FlowPreview
fun main(args: Array<String>) {
    val validParams = setOf(UPLOAD_ARG, DEV_MODE_ARG, STAGING_MODE_ARG, QUIET_ARG)

    var upload = false
    var quietMode = false
    var apiMode = DashDirectApiMode.PROD
    if (args.isNotEmpty()) {
        for (arg in args) {
            if (!validParams.contains(arg)) {
                println("Invalid argument $arg, use one of")
                println("$UPLOAD_ARG - force upload data to GC Storage")
                println("$DEV_MODE_ARG - load data from dev servers")
                println("$STAGING_MODE_ARG - load data from staging servers")
                println("$QUIET_ARG - quiet mode: no notifications are pushed to Slack")
                exitProcess(1)
            }
        }
        upload = args.contains(UPLOAD_ARG)
        quietMode = args.contains(QUIET_ARG)

        apiMode = if (args.contains(DEV_MODE_ARG)) {
            DashDirectApiMode.DEV
        } else if (args.contains(STAGING_MODE_ARG)) {
            DashDirectApiMode.STAGING
        } else {
            DashDirectApiMode.PROD
        }
    }

    runBlocking {
        launch(Dispatchers.IO) {
            SyncProcessor(OperationMode.DEVNET)
                .syncData(File("."), apiMode, upload, quietMode)
        }
    }

    exitProcess(0)
}
