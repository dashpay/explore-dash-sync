package org.dash.mobile.explore.sync

import com.google.gson.*
import com.google.gson.reflect.TypeToken
import com.google.gson.stream.JsonReader
import mu.KotlinLogging
import org.dash.mobile.explore.sync.process.CoinFlipImporter
import org.dash.mobile.explore.sync.process.DashDirectImporter
import org.dash.mobile.explore.sync.process.SpreadsheetImporter
import java.io.FileOutputStream
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.util.*


private val logger = KotlinLogging.logger {}

class SyncProcessor {

    lateinit var usStatesAbbrMap: Map<String?, String?>

    // replace state abbr and modified name with the classified one
    // e.g.
    // "AL" -> "Alabama"
    // " Hawaii" -> "Hawaii"
    // "New,Hampshire " -> "New Hampshire"
    // etc.
    private fun fixStatName(inState: JsonElement) = if (inState.isJsonNull || inState.asString.isEmpty()) {
        JsonNull.INSTANCE
    } else {
        // replace state abbr with the full name e.g. AL -> Alabama, etc.
        val inStateStr = inState.asString.replace(',', ' ').trim()
        val state = usStatesAbbrMap[inStateStr]
        if (state != null) {
            JsonPrimitive(state)
        } else {
            JsonPrimitive(inStateStr)
        }
    }

    fun syncData(devMode: Boolean) {

        val gsonReader = Gson()
        val type = object : TypeToken<Map<String?, String?>?>() {}.type
        val statesJson = JsonReader(InputStreamReader(javaClass.classLoader.getResourceAsStream("states_hash.json")!!))
        usStatesAbbrMap = gsonReader.fromJson(statesJson, type) as Map<String?, String?>

        val importers = listOf(
            SpreadsheetImporter(),
            CoinFlipImporter(::fixStatName),
            DashDirectImporter(devMode, ::fixStatName)
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

//        val outFileName = if (devMode) "dash-wallet-firebase-dev.json" else "dash-wallet-firebase-prod.json"
//        OutputStreamWriter(FileOutputStream(outFileName), StandardCharsets.UTF_8).use { writer ->
//            val gson = GsonBuilder().create()
//            gson.toJson(data, writer)
//        }
    }
}