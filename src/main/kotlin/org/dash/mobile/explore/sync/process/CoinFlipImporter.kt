package org.dash.mobile.explore.sync.process

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import org.dash.mobile.explore.sync.notice
import org.dash.wallet.features.exploredash.data.model.Protos
import org.slf4j.LoggerFactory
import retrofit2.Call
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.GET
import java.io.IOException

private val JsonElement.asStringOrNull: String?
    get() = if (isJsonPrimitive) asString else null

private const val BASE_URL = "https://storerocket.io/api/user/56wpZAy8An/"

/**
 * Import data from CoinFlip API
 */
class CoinFlipImporter : Importer() {

    override val logger = LoggerFactory.getLogger(CoinFlipImporter::class.java)!!

    override val propertyName = "atm"

    interface Endpoint {

        data class Result(val locations: JsonArray /*val html: JsonObject*/)
        data class Response(val success: Boolean, val results: Result)

        @GET("locations")
        fun getLocations(): Call<Response>
    }

    override fun import(): List<Protos.AtmData> {

        logger.notice("Importing data from CoinFlip")

        val gson = GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
            .create()

        val retrofit: Retrofit = Retrofit.Builder()
            .baseUrl(BASE_URL)
            .addConverterFactory(GsonConverterFactory.create(gson))
            .build()

        val apiService = retrofit.create(Endpoint::class.java)
        val locations = apiService.getLocations()

        try {
            val result = mutableListOf<Protos.AtmData>()
            val response = locations.execute()
            if (response.isSuccessful) {
                val responseData = response.body()

                responseData?.results?.locations?.forEach { location ->
                    val outData = buildMerchant(location.asJsonObject)
                    result.add(outData)
                }

                logger.notice("CoinFlip - imported ${result.size} records")

                return result
            } else {
                logger.error("error: ${response.errorBody()}")
            }
        } catch (ex: IOException) {
            logger.error(ex.message, ex)
        }

        return listOf()
    }

    private fun buildMerchant(inData: JsonObject): Protos.AtmData {
        return Protos.AtmData.newBuilder().apply {
            source = "CoinFlip"
            convert<Int?>("id", inData)?.apply { sourceId = this }
            convert<String?>("name", inData)?.apply { name = this }
            convert<String?>("address", inData)?.apply { address = this }
            convert<String?>("city", inData)?.apply { city = this }
            val inState = inData.get("state")
            fixStatName(inState)?.apply { territory = this }
            convert<String?>("postcode", inData)?.apply { postcode = this }
            convert<String?>("phone", inData)?.apply { phone = this }
            logoLocation = "https://drive.google.com/uc?export=view&id=1C2aOHIUAawrfTp3vvUktXERXF_wNz8QQ"
            convert<String?>("cover_image", inData)?.apply { coverImage = this }
            convert<String?>("lat", inData)?.apply { latitude = this.toDouble() }
            convert<String?>("lng", inData)?.apply { longitude = this.toDouble() }
            website = "https://coinflip.tech/bitcoin-atm?location=${inData.get("slug").asString}"
            val filters = inData.getAsJsonArray("filters").asJsonArray
            if (filters.size() > 0) {
                convert<String?>("name", filters[0].asJsonObject)?.apply { type = this }
            }
            convert<String?>("instagram", inData)?.apply { instagram = this }
            convert<String?>("twitter", inData)?.apply { twitter = this }

            opening = Protos.OpeningHoursData.newBuilder().apply {

                val mon = getDay(inData, "mon")
                mon.first?.apply { monOpen = this }
                mon.second?.apply { monClose = this }

                val tue = getDay(inData, "tue")
                tue.first?.apply { tueOpen = this }
                tue.second?.apply { tueClose = this }

                val wed = getDay(inData, "wed")
                wed.first?.apply { wedOpen = this }
                wed.second?.apply { wedClose = this }

                val thu = getDay(inData, "thu")
                thu.first?.apply { thuOpen = this }
                thu.second?.apply { thuClose = this }

                val fri = getDay(inData, "fri")
                fri.first?.apply { friOpen = this }
                fri.second?.apply { friClose = this }

                val sat = getDay(inData, "sat")
                sat.first?.apply { satOpen = this }
                sat.second?.apply { satClose = this }

                val sun = getDay(inData, "sun")
                sun.first?.apply { sunOpen = this }
                sun.second?.apply { sunClose = this }
            }.build()

            manufacturer = "coinflip"
        }.build()
    }

    private fun getDay(inData: JsonObject, day: String): Pair<String?, String?> {
        /* e.g.
         * "mon":"9:00 am - 10:00 pm",
         * "mon":"24 Hours",
         * "mon":null,
         */
        val dayOpenClose = inData.get(day).run {
            if (isJsonPrimitive) asString?.split(" - ") else null
        }
        val dayOpen = dayOpenClose?.run { first() }
        val dayClose = dayOpenClose?.run { last() }
        return Pair(dayOpen, dayClose)
    }
}