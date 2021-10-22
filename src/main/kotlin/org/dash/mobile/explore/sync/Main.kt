package org.dash.mobile.explore.sync

import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import kotlinx.coroutines.*
import kotlinx.coroutines.runBlocking
import org.apache.log4j.PropertyConfigurator
import org.dash.mobile.explore.sync.process.CoinFlipImporter
import org.dash.mobile.explore.sync.process.DashDirectImporter
import org.dash.mobile.explore.sync.process.SpreadsheetImporter
import org.slf4j.LoggerFactory
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.util.*
import kotlin.system.exitProcess


private val logger = LoggerFactory.getLogger("org.dash.mobile.explore.sync.main")

fun main(args: Array<String>) = runBlocking {

    PropertyConfigurator.configure(javaClass.classLoader.getResourceAsStream("log4j.properties"))

    launch(Dispatchers.IO) {

        val importers = listOf(
            SpreadsheetImporter(),
            CoinFlipImporter(),
            DashDirectImporter()
        )

        val explore = JsonObject()
        val timestamp = JsonPrimitive(Date().time)
        explore.add("last_update", timestamp)
        importers.forEach {
            val entry = JsonObject()
            entry.add("last_update", timestamp)
            val data = it.import(true)
            entry.add("data_size", JsonPrimitive(data.size()))
            entry.add("data", data)
            explore.add(it.propertyName, entry)
        }

        val data = JsonObject().apply {
            add("explore", explore)
        }

//        logger.debug(data.toString())

        OutputStreamWriter(FileOutputStream("dash-wallet-firebase.json"), StandardCharsets.UTF_8).use { writer ->
            val gson = GsonBuilder().create()
            gson.toJson(data, writer)
        }

        exitProcess(0)
    }

    return@runBlocking
}