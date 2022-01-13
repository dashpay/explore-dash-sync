package org.dash.mobile.explore.sync.process

import com.google.gson.*
import com.google.gson.annotations.SerializedName
import okhttp3.OkHttpClient
import org.dash.mobile.explore.sync.notice
import org.dash.wallet.features.exploredash.data.model.Protos
import org.slf4j.LoggerFactory
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
class DashDirectImporter(private val devApi: Boolean) :
    Importer() {

    override val logger = LoggerFactory.getLogger(DashDirectImporter::class.java)!!

    override val propertyName = "dash_direct"

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

    override fun import(): List<Protos.MerchantData> {

        logger.notice("Importing data from DashDirect ($baseUrl)")

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

        val result = mutableListOf<Protos.MerchantData>()
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
                    logger.info("DashDirect ${currentPageIndex - 1}/${totalPages - 1} ($currentRows)")
                    logger.info("DashDirect.totalRows: ${responseData.totalRows}")

                    responseData.merchants.forEach { merchant ->
                        if (merchant.merchant.get("IsActive").asBoolean) {
                            val totalLocations = merchant.locations.size()

                            merchant.locations.forEach { location ->
                                if (location.asJsonObject.get("IsActive").asBoolean) {
                                    val outData =
                                        buildMerchant(merchant.merchant, location.asJsonObject, totalLocations)

                                    if (isValidLocation(outData)) {
                                        result.add(outData)
                                    }
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
                return listOf<Protos.MerchantData>()
            }
        }

        logger.notice("DashDirect - imported ${result.size} records")

        return result
    }

    private fun buildMerchant(merchant: JsonObject, location: JsonObject, totalLocations: Int): Protos.MerchantData {
        return Protos.MerchantData.newBuilder().apply {
            source = "DashDirect"
            convert<Int?>("Id", location)?.apply { sourceId = this }
            convert<Long?>("Id", merchant)?.apply { merchantId = this }
            convert<String?>("LegalName", merchant)?.apply { name = this }
            convert<String?>("Address1", location)?.apply { address1 = this }
            convert<String?>("Address2", location)?.apply { address2 = this }
            convert<String?>("City", location)?.apply { city = this }
            val inState = location.get("State")
            fixStatName(inState)?.apply { territory = this }
            convert<String?>("PostalCode", location)?.apply { postcode = this }
            convert<String?>("Phone", location)?.apply { phone = this }
            convert<String?>("LogoUrl", merchant)?.apply { logoLocation = this }
            convert<String?>("CardImageUrl", merchant)?.apply { coverImage = this }
            convert<Double?>("GpsLat", location)?.apply { latitude = this }
            convert<Double?>("GpsLong", location)?.apply { longitude = this }
            convert<String?>("Website", merchant)?.apply { website = this }
            merchantType(merchant, totalLocations)?.apply { type = this }
            convert<String?>("DeepLink", merchant)?.apply { deeplink = this }
            opening = Protos.OpeningHoursData.newBuilder().apply {
                convert<String?>("MondayOpen", location)?.apply { monOpen = this }
                convert<String?>("MondayClose", location)?.apply { monClose = this }
                convert<String?>("TuesdayOpen", location)?.apply { tueOpen = this }
                convert<String?>("TuesdayClose", location)?.apply { tueClose = this }
                convert<String?>("WednesdayOpen", location)?.apply { wedOpen = this }
                convert<String?>("WednesdayClose", location)?.apply { wedClose = this }
                convert<String?>("ThursdayOpen", location)?.apply { thuOpen = this }
                convert<String?>("ThursdayClose", location)?.apply { thuClose = this }
                convert<String?>("FridayOpen", location)?.apply { friOpen = this }
                convert<String?>("FridayClose", location)?.apply { friClose = this }
                convert<String?>("SaturdayOpen", location)?.apply { satOpen = this }
                convert<String?>("SaturdayClose", location)?.apply { satClose = this }
                convert<String?>("SundayOpen", location)?.apply { sunOpen = this }
                convert<String?>("SundayClose", location)?.apply { sunClose = this }
            }.build()
            paymentMethod = "gift card"
        }.build()
    }

    private fun merchantType(inData: JsonObject, totalLocations: Int): String? {
        val isPhysical = inData.getAsJsonPrimitive("IsPhysical").asBoolean
        val isOnline = inData.getAsJsonPrimitive("IsOnline").asBoolean
        return when {
            isPhysical && isOnline -> "both"
            isPhysical -> "physical"
            isOnline && totalLocations > 1 -> "both"
            isOnline -> "online"
            else -> {
                logger.error("Merchant has invalid type:\n${inData}")
                null
            }
        }
    }

    private fun isValidLocation(merchantRecord: Protos.MerchantData): Boolean {

        val type = merchantRecord.type ?: return false

        if (type == "online") {
            return true
        }

        val isAddress1Empty = merchantRecord.address1.isNullOrEmpty()
        val isAddress2Empty = merchantRecord.address2.isNullOrEmpty()

        val isLatitudeEmpty = !merchantRecord.hasLatitude() || merchantRecord.latitude == 0.0
        val isLongitudeEmpty = !merchantRecord.hasLongitude() || merchantRecord.longitude == 0.0

        return !isAddress1Empty || !isAddress2Empty || !isLatitudeEmpty || !isLongitudeEmpty
    }
}