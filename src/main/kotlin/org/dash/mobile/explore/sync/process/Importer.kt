package org.dash.mobile.explore.sync.process

import com.google.gson.Gson
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.reflect.TypeToken
import com.google.gson.stream.JsonReader
import com.google.protobuf.GeneratedMessageLite
import org.slf4j.Logger
import java.io.InputStreamReader

abstract class Importer {

    private val usStatesAbbrMap: Map<String?, String?>

    init {
        val gsonReader = Gson()
        val type = object : TypeToken<Map<String?, String?>?>() {}.type
        val statesJson = JsonReader(InputStreamReader(javaClass.classLoader.getResourceAsStream("states_hash.json")!!))
        usStatesAbbrMap = gsonReader.fromJson(statesJson, type) as Map<String?, String?>
    }

    abstract val logger: Logger

    abstract val propertyName: String

    abstract fun import(): List<GeneratedMessageLite<*, *>>

    inline fun <reified T> convert(inKey: String, inData: JsonObject): T? {
        return try {
            val data = inData.get(inKey)
            if (data.isJsonNull) {
                return null
            }
            val primitiveData = inData.get(inKey).asJsonPrimitive
            when {
                T::class == String::class && primitiveData.isString -> primitiveData.asString as T
                T::class == Int::class && primitiveData.isNumber -> primitiveData.asInt as T
                T::class == Long::class && primitiveData.isNumber -> primitiveData.asLong as T
                T::class == Double::class && primitiveData.isNumber -> primitiveData.asDouble as T
                T::class == Boolean::class && primitiveData.isBoolean -> primitiveData.asBoolean as T
                else -> {
                    throw IllegalArgumentException("$inKey is not a ${T::class} ($primitiveData)")
                }
            }
        } catch (ex: IllegalStateException) {
            logger.error("$inKey is not a JSON primitive (${inData.get(inKey)})")
            throw ex
        }
    }

    // replace state abbr and modified name with the classified one
    // e.g.
    // "AL" -> "Alabama"
    // " Hawaii" -> "Hawaii"
    // "New,Hampshire " -> "New Hampshire"
    // etc.
    fun fixStatName(inState: JsonElement) = if (inState.isJsonNull || inState.asString.isEmpty()) {
        null
    } else {
        // replace state abbr with the full name e.g. AL -> Alabama, etc.
        val inStateStr = inState.asString.replace(',', ' ').trim()
        usStatesAbbrMap[inStateStr] ?: inStateStr
    }
}