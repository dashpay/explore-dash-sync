package org.dash.mobile.explore.sync

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.system.exitProcess

const val UPLOAD_ARG = "-upload"
const val QUIET_ARG = "-quiet"

@FlowPreview
fun main(args: Array<String>) {
    val validParams = setOf(UPLOAD_ARG, QUIET_ARG)

    var upload = false
    var quietMode = false

    if (args.isNotEmpty()) {
        for (arg in args) {
            if (!validParams.contains(arg)) {
                println("Invalid argument $arg, use one of")
                println("$UPLOAD_ARG - force upload data to GC Storage")
                println("$QUIET_ARG - quiet mode: no notifications are pushed to Slack")
                exitProcess(1)
            }
        }
        upload = args.contains(UPLOAD_ARG)
        quietMode = args.contains(QUIET_ARG)
    }

    runBlocking {
        launch(Dispatchers.IO) {
            SyncProcessor(OperationMode.TESTNET)
                .syncData(File("."), upload, quietMode)
        }
    }

    exitProcess(0)
}
