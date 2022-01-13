package org.dash.mobile.explore.sync

import kotlin.system.exitProcess

const val DEV_MODE_ARG = "-dev"
const val UPLOAD_ARG = "-upload"

fun main(args: Array<String>) {

    val validParams = setOf(UPLOAD_ARG, DEV_MODE_ARG)

    var upload = false
    var srcDev = false
    if (args.isNotEmpty()) {
        for (arg in args) {
            if (!validParams.contains(arg)) {
                println("Invalid argument $arg, use one of")
                println("$UPLOAD_ARG - force upload data to GC Storage")
                println("$DEV_MODE_ARG - load data from dev servers")
                exitProcess(1)
            }
        }
        upload = args.contains(UPLOAD_ARG)
        srcDev = args.contains(DEV_MODE_ARG)
    }

    SyncProcessor(OUTPUT_FILE)
        .syncData(true, upload, srcDev)

    exitProcess(0)
}