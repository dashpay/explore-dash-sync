package org.dash.mobile.explore.sync.process

import com.google.gson.*
import com.google.gson.annotations.SerializedName
import okhttp3.OkHttpClient
import org.slf4j.LoggerFactory
import retrofit2.Call
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.Body
import retrofit2.http.Headers
import retrofit2.http.POST
import java.io.IOException
import java.util.concurrent.TimeUnit


//private const val BASE_URL = "https://api.dashdirect.org/"
private const val BASE_URL = "https://apidev.dashdirect.org/"

/**
 * Import data from DashDirect API
 */
class DashDirectImporter(private val fixStatName: (inState: JsonElement) -> JsonElement) : Importer {

    override val propertyName = "dash_direct"

    private val logger = LoggerFactory.getLogger(DashDirectImporter::class.java)

    interface Endpoint {

        data class Response(
            @SerializedName("Successful") val successful: Boolean,
            @SerializedName("ErrorMessage") val errorMessage: String,
            @SerializedName("Data") val data: JsonArray?
        )

        data class AllMerchantLocationsResponse(
            @SerializedName("Successful") val successful: Boolean,
            @SerializedName("ErrorMessage") val errorMessage: String,
            @SerializedName("Data") val data: AllMerchantLocationsResponseData
        )

        data class AllMerchantLocationsResponseData(
            @SerializedName("Merchants") val merchants: ArrayList<AllMerchantLocationsResponseMerchantData>,
            @SerializedName("TotalRows") val totalRows: Int,
            @SerializedName("TotalPages") val totalPages: Int,
            @SerializedName("CurrentPageIndex") val currentPageIndex: Int
        )

        data class AllMerchantLocationsResponseMerchantData(
            @SerializedName("Merchant") val merchant: JsonObject,
            @SerializedName("Locations") val locations: JsonArray
        )

        data class AllMerchantLocationsRequest(
            @SerializedName("PageSize") val pageSize: Int,
            @SerializedName("PageIndex") val pageIndex: Int
        )

        @POST("Merchant/GetAllMerchants")
        fun getAllMerchants(): Call<Response>

        @Headers(
            "appKey: BEBF2B6B-1295-4122-AA8E-09605FE26DE8",
            "apiKey: BCE98756-65A2-46EF-967B-BBF89BA23799"
        )
        @POST("Merchant/GetAllMerchantLocations")
        fun getAllMerchantLocations(@Body requestData: AllMerchantLocationsRequest): Call<AllMerchantLocationsResponse>
    }

    override suspend fun import(save: Boolean): JsonArray {

        logger.info("Importing data from DashDirect")

        val gson = GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
            .create()

        val okHttpClient = OkHttpClient.Builder()
            .connectTimeout(20, TimeUnit.SECONDS)
            .writeTimeout(20, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .build()

        val retrofit: Retrofit = Retrofit.Builder()
            .baseUrl(BASE_URL)
            .addConverterFactory(GsonConverterFactory.create(gson))
            .client(okHttpClient)
            .build()

        val apiService = retrofit.create(Endpoint::class.java)

        val result = JsonArray()
        val pageSize = 20000
        var currentPageIndex = 1
        var totalPages = currentPageIndex + 1
        while (currentPageIndex < totalPages) {
            val merchantLocations =
                apiService.getAllMerchantLocations(Endpoint.AllMerchantLocationsRequest(pageSize, currentPageIndex))
            try {
                val response = merchantLocations.execute()
                if (response.isSuccessful) {
                    val responseData = response.body()!!.data
                    totalPages = responseData.totalPages + 1
                    currentPageIndex = responseData.currentPageIndex + 1
                    var currentRows = 0
                    responseData.merchants.forEach { merchant ->
                        currentRows += merchant.locations.size()
                    }
                    logger.info("DashDirect\t${currentPageIndex - 1}/${totalPages - 1} ($currentRows)")
                    logger.info("DashDirect.totalRows:\t${responseData.totalRows}")

                    responseData.merchants.forEach { merchant ->

                        if (merchant.merchant.get("IsActive").asBoolean) {

                            merchant.locations.forEach { location ->
                                if (location.asJsonObject.get("IsActive").asBoolean) {
                                    val outData = mapData(merchant.merchant, location.asJsonObject)
                                    result.add(outData)
                                }
                            }
                        }
                    }

                } else {
                    logger.error("error: $response")
                    break
                }
            } catch (ex: IOException) {
                logger.error(ex.message, ex)
                return JsonArray()
            }
        }

        logger.info("DashDirect - imported ${result.size()} records")

        return result
    }

    private fun mapData(merchant: JsonObject, location: JsonObject): JsonObject {
        return JsonObject().apply {
            add("source_id", location.get("Id"))
            add("source", JsonPrimitive("DashDirect"))
            add("merchant_id", merchant.get("Id"))
            add("name", merchant.get("LegalName"))
            add("address1", location.get("Address1"))
            add("address2", location.get("Address2"))
            add("city", location.get("City"))
            val inState = location.get("State")
            val outState = fixStatName(inState)
            add("territory", outState)
            add("postcode", location.get("PostalCode"))
            add("phone", location.get("Phone"))
            add("logo_location", merchant.get("LogoUrl"))
            add("cover_image", merchant.get("CardImageUrl"))
            add("latitude", location.get("GpsLat"))
            add("longitude", location.get("GpsLong"))
            add("website", merchant.get("Website"))
            add("type", merchantType(merchant))
            add("deeplink", merchant.get("DeepLink"))
//            add("buy_sell", buySel)
            add("mon_open", location.get("MondayOpen"))
            add("mon_close", location.get("MondayClose"))
            add("tue_open", location.get("TuesdayOpen"))
            add("tue_close", location.get("TuesdayClose"))
            add("wed_open", location.get("WednesdayOpen"))
            add("wed_close", location.get("WednesdayClose"))
            add("thu_open", location.get("ThursdayOpen"))
            add("thu_close", location.get("ThursdayClose"))
            add("fri_open", location.get("FridayOpen"))
            add("fri_close", location.get("FridayClose"))
            add("sat_open", location.get("SaturdayOpen"))
            add("sat_close", location.get("SaturdayClose"))
            add("sun_open", location.get("SundayOpen"))
            add("sun_close", location.get("SundayClose"))
            add("payment_method", JsonPrimitive("gift card"))
        }
    }

    private fun merchantType(inData: JsonObject): JsonElement {
        val isPhysical = inData.getAsJsonPrimitive("IsPhysical").isBoolean
        val isOnline = inData.getAsJsonPrimitive("IsOnline").isBoolean
        return when {
            isPhysical && isOnline -> JsonPrimitive("both")
            isPhysical -> JsonPrimitive("physical")
            isOnline -> JsonPrimitive("online")
            else -> JsonNull.INSTANCE
        }
    }
}