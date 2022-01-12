package org.dash.mobile.explore.sync.process

import com.google.gson.*
import com.google.gson.annotations.SerializedName
import mu.KotlinLogging
import okhttp3.OkHttpClient
import retrofit2.Call
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.Body
import retrofit2.http.Headers
import retrofit2.http.POST
import java.io.IOException
import java.util.concurrent.TimeUnit


private const val BASE_URL = "https://api.dashdirect.org/"
private const val DEV_BASE_URL = "https://apidev.dashdirect.org/"

/**
 * Import data from DashDirect API
 */
class DashDirectImporter(private val devApi: Boolean, private val fixStatName: (inState: JsonElement) -> JsonElement) :
    Importer() {

    override val propertyName = "dash_direct"

    override val logger = KotlinLogging.logger {}

    private val baseUrl by lazy {
        if (devApi) {
            DEV_BASE_URL
        } else {
            BASE_URL
        }
    }

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

    override fun import(save: Boolean): JsonArray {

        logger.info("Importing data from DashDirect ($baseUrl")

        val gson = GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
            .create()

        val okHttpClient = OkHttpClient.Builder()
            .connectTimeout(20, TimeUnit.SECONDS)
            .writeTimeout(20, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .build()

        val retrofit: Retrofit = Retrofit.Builder()
            .baseUrl(baseUrl)
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

    enum class DataType {

        BOOLEAN,
        NUMBER,
        TEXT;

        fun isEquivalentTo(data: JsonPrimitive): Boolean {
            return when (this) {
                BOOLEAN -> data.isBoolean
                NUMBER -> data.isNumber
                TEXT -> data.isString
            }
        }
    }

    private fun mapData(merchant: JsonObject, location: JsonObject): JsonObject {
        return JsonObject().apply {
            add("source_id", addValidOrDie("Id", location, DataType.NUMBER))
            add("source", JsonPrimitive("DashDirect"))
            add("merchant_id", addValidOrDie("Id", merchant, DataType.NUMBER))
            add("name", addValidOrDie("LegalName", merchant, DataType.TEXT))
            add("address1", addValidOrDie("Address1", location, DataType.TEXT))
            add("address2", addValidOrDie("Address2", location, DataType.TEXT))
            add("city", addValidOrDie("City", location, DataType.TEXT))
            val inState = location.get("State")
            val outState = fixStatName(inState)
            add("territory", outState)
            add("postcode", addValidOrDie("PostalCode", location, DataType.TEXT))
            add("phone", addValidOrDie("Phone", location, DataType.TEXT))
            add("logo_location", addValidOrDie("LogoUrl", merchant, DataType.TEXT))
            add("cover_image", addValidOrDie("CardImageUrl", merchant, DataType.TEXT))
            add("latitude", addValidOrDie("GpsLat", location, DataType.NUMBER))
            add("longitude", addValidOrDie("GpsLong", location, DataType.NUMBER))
            add("website", addValidOrDie("Website", merchant, DataType.TEXT))
            add("type", merchantType(merchant))
            add("deeplink", addValidOrDie("DeepLink", merchant, DataType.TEXT))
//            add("buy_sell", buySel)
            add("mon_open", addValidOrDie("MondayOpen", location, DataType.TEXT))
            add("mon_close", addValidOrDie("MondayClose", location, DataType.TEXT))
            add("tue_open", addValidOrDie("TuesdayOpen", location, DataType.TEXT))
            add("tue_close", addValidOrDie("TuesdayClose", location, DataType.TEXT))
            add("wed_open", addValidOrDie("WednesdayOpen", location, DataType.TEXT))
            add("wed_close", addValidOrDie("WednesdayClose", location, DataType.TEXT))
            add("thu_open", addValidOrDie("ThursdayOpen", location, DataType.TEXT))
            add("thu_close", addValidOrDie("ThursdayClose", location, DataType.TEXT))
            add("fri_open", addValidOrDie("FridayOpen", location, DataType.TEXT))
            add("fri_close", addValidOrDie("FridayClose", location, DataType.TEXT))
            add("sat_open", addValidOrDie("SaturdayOpen", location, DataType.TEXT))
            add("sat_close", addValidOrDie("SaturdayClose", location, DataType.TEXT))
            add("sun_open", addValidOrDie("SundayOpen", location, DataType.TEXT))
            add("sun_close", addValidOrDie("SundayClose", location, DataType.TEXT))
            add("payment_method", JsonPrimitive("gift card"))
        }
    }

    private fun merchantType(inData: JsonObject): JsonElement {
        val isPhysical = inData.getAsJsonPrimitive("IsPhysical").asBoolean
        val isOnline = inData.getAsJsonPrimitive("IsOnline").asBoolean
        return when {
            isPhysical && isOnline -> JsonPrimitive("both")
            isPhysical -> JsonPrimitive("physical")
            isOnline -> JsonPrimitive("online")
            else -> JsonNull.INSTANCE
        }
    }
}