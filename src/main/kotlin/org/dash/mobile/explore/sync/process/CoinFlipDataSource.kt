package org.dash.mobile.explore.sync.process

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.dash.mobile.explore.sync.notice
import org.dash.mobile.explore.sync.process.data.AtmData
import org.dash.mobile.explore.sync.slack.SlackMessenger
import org.slf4j.LoggerFactory
import retrofit2.Call
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.GET
import java.io.IOException

private const val BASE_URL = "https://storerocket.io/api/user/56wpZAy8An/"

/**
 * Import data from CoinFlip API
 */
class CoinFlipDataSource(slackMessenger: SlackMessenger) : DataSource<AtmData>(slackMessenger) {

    override val logger = LoggerFactory.getLogger(CoinFlipDataSource::class.java)!!

    interface Endpoint {

        data class Result(val locations: JsonArray /*val html: JsonObject*/)
        data class Response(val success: Boolean, val results: Result)

        @GET("locations")
        fun getLocations(): Call<Response>
    }

    override fun getRawData(): Flow<AtmData> = flow {
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
            val response = locations.execute()
            if (response.isSuccessful) {
                val responseData = response.body()

                var totalRecords = 0
                responseData?.results?.locations?.forEach { location ->
                    val outData = convert(location.asJsonObject)
                    totalRecords++
                    emit(outData)
                }

                slackMessenger.postSlackMessage("CoinFlip $totalRecords records", logger)
            } else {
                logger.error("error: ${response.errorBody()}")
            }
        } catch (ex: IOException) {
            logger.error(ex.message, ex)
        }
    }

    private fun convert(inData: JsonObject): AtmData {
        return AtmData().apply {
            postcode = convertJsonData<String?>("postcode", inData)
            manufacturer = "coinflip"
//            id = null
            active = true
            name = convertJsonData("name", inData)
            address1 = convertJsonData("address", inData)
//            address2 = null
//            address3 = null
//            address4 = null
            latitude = convertJsonData<String?>("lat", inData)?.toDouble()
            longitude = convertJsonData<String?>("lng", inData)?.toDouble()
            website = "https://coinflip.tech/bitcoin-atm?location=${inData.get("slug").asString}"
            phone = convertJsonData("phone", inData)
            val inState = inData.get("state")
            fixStatName(inState)?.apply {
                territory = this
            }
            city = convertJsonData("city", inData)
            source = "CoinFlip"
            sourceId = convertJsonData("id", inData)
            logoLocation = "https://drive.google.com/uc?export=view&id=1C2aOHIUAawrfTp3vvUktXERXF_wNz8QQ"
//            googleMaps = null
            coverImage = convertJsonData("cover_image", inData)
            val filters = inData.getAsJsonArray("filters").asJsonArray
            if (filters.size() > 0) {
                type = convertJsonData("name", filters[0].asJsonObject)
            }

            val mon = getDay(inData, "mon")
            mon.first?.apply { monOpen = this }
            mon.second?.apply { monClose = this }
            monOpen = mon.first
            monClose = mon.second

            val tue = getDay(inData, "tue")
            tueOpen = tue.first
            tueClose = tue.second

            val wed = getDay(inData, "wed")
            wedOpen = wed.first
            wedClose = wed.second

            val thu = getDay(inData, "thu")
            thuOpen = thu.first
            thuClose = thu.second

            val fri = getDay(inData, "fri")
            friOpen = fri.first
            friClose = fri.second

            val sat = getDay(inData, "sat")
            satOpen = sat.first
            satClose = sat.second

            val sun = getDay(inData, "sun")
            sunOpen = sun.first
            sunClose = sun.second
        }
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
