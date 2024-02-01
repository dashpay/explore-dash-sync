package org.dash.mobile.explore.sync.process

import com.google.gson.Gson
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.reflect.TypeToken
import com.google.gson.stream.JsonReader
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.transform
import kotlinx.coroutines.withContext
import org.dash.mobile.explore.sync.notice
import org.dash.mobile.explore.sync.process.data.Data
import org.dash.mobile.explore.sync.slack.SlackMessenger
import org.slf4j.Logger
import java.io.FileNotFoundException
import java.io.InputStreamReader
import java.sql.PreparedStatement
import java.util.Properties

abstract class DataSource<T>(val slackMessenger: SlackMessenger) where T : Data {

    private val usStatesAbbrMap: Map<String?, String?>

    init {
        val gsonReader = Gson()
        val type = object : TypeToken<Map<String?, String?>?>() {}.type
        val statesJson = JsonReader(InputStreamReader(javaClass.classLoader.getResourceAsStream("states_hash.json")!!))
        usStatesAbbrMap = gsonReader.fromJson(statesJson, type) as Map<String?, String?>
    }

    abstract val logger: Logger

    protected abstract fun getRawData(): Flow<T>

    fun getData(statement: PreparedStatement) = getRawData()
        .transform { data ->
            data.transferInto(statement)
            emit(data)
        }

    inline fun <reified T> convertJsonData(inKey: String, inData: JsonObject): T? {
        return try {
            val data = inData.get(inKey)
            if (data == null || data.isJsonNull) {
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

    suspend fun getProperties(): Properties {
        logger.notice("Getting secrets")
        val properties = Properties()
        val inputStream = javaClass.classLoader.getResourceAsStream("service.properties")
            ?: throw FileNotFoundException("service properties not found")
        inputStream.use { withContext(Dispatchers.IO) { properties.load(inputStream) } }

        return properties
    }

    /** replace state abbr and modified name with the classified one
     * e.g.
     * "AL" -> "Alabama"
     * " Hawaii" -> "Hawaii"
     * "New,Hampshire " -> "New Hampshire"
     * etc.
     */
    fun fixStateName(inState: JsonElement) = if (inState.isJsonNull || inState.asString.isEmpty()) {
        null
    } else {
        // replace state abbr with the full name e.g. AL -> Alabama, etc.
        val inStateStr = inState.asString.replace(',', ' ').trim()
        usStatesAbbrMap[inStateStr] ?: inStateStr
    }

    fun fixStateName(stateAbbreviation: String): String {
        val trimmed = stateAbbreviation.replace(',', ' ').trim()
        return usStatesAbbrMap[trimmed] ?: stateAbbreviation
    }
}
