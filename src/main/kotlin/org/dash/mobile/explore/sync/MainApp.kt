package org.dash.mobile.explore.sync

import com.google.gson.*
import kotlinx.coroutines.*
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.util.*
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) = runBlocking {

    if (args.isNotEmpty() && args[0] != "-dev") {
        logger.info("Invalid parameter ${args[0]} do you mean -dev?")
        exitProcess(0)
    }

    val devMode = args.isNotEmpty() && args[0] == "-dev"
    if (devMode) {
        logger.info("DEV mode activated")
    }

    launch(Dispatchers.IO) {

        SyncProcessor().syncData(devMode)

        exitProcess(0)
    }
    return@runBlocking
}
