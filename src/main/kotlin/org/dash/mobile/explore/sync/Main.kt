package org.dash.mobile.explore.sync

import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import kotlinx.coroutines.*
import kotlinx.coroutines.runBlocking
import org.apache.log4j.PropertyConfigurator
import org.dash.mobile.explore.sync.process.CoinFlipImporter
import org.dash.mobile.explore.sync.process.SpreadsheetImporter
import org.slf4j.LoggerFactory
import java.io.FileWriter
import java.util.*
import kotlin.system.exitProcess

private val logger = LoggerFactory.getLogger("org.dash.mobile.explore.sync.main")

fun main(args: Array<String>) = runBlocking {

    PropertyConfigurator.configure(javaClass.classLoader.getResourceAsStream("log4j.properties"))

    launch(Dispatchers.IO) {

        val importers = listOf(
            SpreadsheetImporter(),
            CoinFlipImporter(),
//            DashDirectImporter()
        )

        val explore = JsonObject()
        explore.add("last_update", JsonPrimitive(Date().time))
        importers.forEach {
            explore.add(it.propertyName, it.import(true))
        }

        val data = JsonObject().apply {
            add("explore", explore)
        }

        logger.debug(data.toString())

        FileWriter("dash-wallet-firebase.json").use { writer ->
            val gson = GsonBuilder().create()
            gson.toJson(data, writer)
        }

        exitProcess(0)
    }

    return@runBlocking
}