package org.dash.mobile.explore.sync

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.system.exitProcess
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import org.slf4j.LoggerFactory

const val UPLOAD_ARG = "-upload"
const val QUIET_ARG = "-quiet"
const val PROD_ARG = "-prod"
const val DEBUG_ARG = "-debug"

@FlowPreview
fun main(args: Array<String>) {
    val validParams = setOf(UPLOAD_ARG, QUIET_ARG, PROD_ARG, DEBUG_ARG)

    var upload = false
    var quietMode = false
    var prodMode = false
    var debugMode = false

    if (args.isNotEmpty()) {
        for (arg in args) {
            if (!validParams.contains(arg)) {
                println("Invalid argument $arg, use one of")
                println("$UPLOAD_ARG - force upload data to GC Storage")
                println("$QUIET_ARG - quiet mode: no notifications are pushed to Slack")
                println("$PROD_ARG - production mode: use production data sources/destinations")
                println("$DEBUG_ARG - output to CSV files for unit tests")
                exitProcess(1)
            }
        }
        upload = args.contains(UPLOAD_ARG)
        quietMode = args.contains(QUIET_ARG)
        prodMode = args.contains(PROD_ARG)
        debugMode = args.contains(DEBUG_ARG)
    }
    configureConsoleLogging()

    runBlocking(Dispatchers.IO) {
        SyncProcessor(
            if (prodMode) OperationMode.PRODUCTION else OperationMode.TESTNET,
            debug = debugMode
        ).syncData(File("."), upload, quietMode)
    }

    exitProcess(0)
}

fun configureConsoleLogging() {
    val context = LoggerFactory.getILoggerFactory() as LoggerContext
    context.reset()
    
    val configurator = JoranConfigurator()
    configurator.context = context
    
    val consoleConfigFile = Function::class.java.classLoader.getResource("logback-console.xml")
    if (consoleConfigFile != null) {
        configurator.doConfigure(consoleConfigFile)
    }
}
