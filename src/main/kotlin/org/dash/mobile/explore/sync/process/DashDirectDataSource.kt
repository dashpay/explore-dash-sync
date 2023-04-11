package org.dash.mobile.explore.sync.process

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.annotations.SerializedName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import org.dash.mobile.explore.sync.notice
import org.dash.mobile.explore.sync.process.data.MerchantData
import org.dash.mobile.explore.sync.slack.SlackMessenger
import org.slf4j.LoggerFactory
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.*
import java.io.FileNotFoundException
import java.io.IOException
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.collections.ArrayList

private const val BASE_URL = "https://api.dashdirect.org/"
private const val DEV_BASE_URL = "https://apidev.dashdirect.org/"
private const val CRAYPAY_URL = "https://auth.craypay.com/"

/**
 * Import data from DashDirect API
 */
class DashDirectDataSource(private val devApi: Boolean, slackMessenger: SlackMessenger) :
    DataSource<MerchantData>(slackMessenger) {

    override val logger = LoggerFactory.getLogger(DashDirectDataSource::class.java)!!

    private val baseUrl by lazy {
        if (devApi) {
            DEV_BASE_URL
        } else {
            BASE_URL
        }
    }

    interface TokenEndpoint {
        data class TokenResponse(
            @SerializedName("access_token") val accessToken: String,
            @SerializedName("expires_in") val expiresIn: Int,
            @SerializedName("token_type") val tokenType: String,
            @SerializedName("scope") val scope: String
        )

        @FormUrlEncoded
        @POST("connect/token")
        suspend fun getToken(
            @Field("client_id") clientId: String,
            @Field("client_secret") clientSecret: String,
            @Field("grant_type") grantType: String,
            @Field("scope") scope: String
        ): TokenResponse
    }

    interface Endpoint {
        data class AllMerchantLocationsResponse(
            @SerializedName("Successful") val successful: Boolean,
            @SerializedName("ErrorMessage") val errorMessage: String,
            @SerializedName("Data") val data: AllMerchantLocationsResponseData
        )

        data class AllMerchantLocationsResponseData(
            @SerializedName("Merchants") val merchants: ArrayList<MerchantData>,
            @SerializedName("TotalRows") val totalRows: Int,
            @SerializedName("TotalPages") val totalPages: Int,
            @SerializedName("CurrentPageIndex") val currentPageIndex: Int
        )

        data class MerchantData(
            @SerializedName("Merchant") val merchant: JsonObject,
            @SerializedName("Locations") val locations: JsonArray
        )

        data class AllMerchantLocationsRequest(
            @SerializedName("PageSize") val pageSize: Int,
            @SerializedName("PageIndex") val pageIndex: Int
        )

        @Headers(
            "firstName: Wallet",
            "lastName: Dash",
            "email: mobile@dash.org"
        )
        @POST("dashdirect/GetAllMerchantLocations")
        suspend fun getAllMerchantLocations(
            @Header("Authorization") token: String,
            @Body requestData: AllMerchantLocationsRequest
        ): AllMerchantLocationsResponse
    }

    private val auth: TokenEndpoint
    private val apiService: Endpoint

    init {
        val gson = GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
            .create()

        val okHttpClient = OkHttpClient.Builder()
            .connectTimeout(20, TimeUnit.SECONDS)
            .writeTimeout(20, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .build()

        val authRetrofit: Retrofit = Retrofit.Builder()
            .baseUrl(CRAYPAY_URL)
            .addConverterFactory(GsonConverterFactory.create(gson))
            .client(okHttpClient)
            .build()

        val retrofit: Retrofit = Retrofit.Builder()
            .baseUrl(baseUrl)
            .addConverterFactory(GsonConverterFactory.create(gson))
            .client(okHttpClient)
            .build()

        auth = authRetrofit.create(TokenEndpoint::class.java)
        apiService = retrofit.create(Endpoint::class.java)
    }

    var inactive = 0
    var invalid = 0

    override fun getRawData(): Flow<MerchantData> = flow {
        val token = getAuthToken()
        logger.notice("Importing data from DashDirect ($baseUrl)")

        val pageSize = 20000
        var currentPageIndex = 1
        var totalPages = currentPageIndex + 1
        var counter = 0

        while (currentPageIndex < totalPages) {
            try {
                val response =
                    apiService.getAllMerchantLocations(
                        "${token.tokenType} ${token.accessToken}",
                        Endpoint.AllMerchantLocationsRequest(pageSize, currentPageIndex)
                    )
                if (response.successful) {
                    val responseData = response.data
                    totalPages = responseData.totalPages + 1
                    currentPageIndex = responseData.currentPageIndex + 1
                    var currentRows = 0
                    responseData.merchants.forEach { merchant ->
                        currentRows += merchant.locations.size()
                    }
                    logger.info("DashDirect ${currentPageIndex - 1}/${totalPages - 1} ($currentRows)")
                    logger.info("DashDirect.totalRows: ${responseData.totalRows}")

                    responseData.merchants.forEach { merchant ->
                        merchant.locations.forEach { location ->
                            val locationData = location.asJsonObject

                            if (locationData.get("IsActive").asBoolean) {
                                val type = getType(merchant)

                                if (isValidLocation(type, locationData)) {
                                    counter++
                                    emit(convert(merchant, locationData))
                                } else {
                                    invalid++
                                }
                            } else {
                                inactive++
                            }
                        }
                    }
                } else {
                    logger.error("error: $response")
                    break
                }
            } catch (ex: IOException) {
                logger.error(ex.message, ex)
            }
        }
        logger.notice("DashDirect - imported $counter records (inactive $inactive, invalid $invalid)")
        slackMessenger.postSlackMessage("DashDirect $counter records")
    }

    private suspend fun getAuthToken(): TokenEndpoint.TokenResponse {
        logger.notice("Getting secrets")
        val properties = Properties()
        val inputStream = javaClass.classLoader.getResourceAsStream("service.properties")
            ?: throw FileNotFoundException("service properties not found")
        inputStream.use { withContext(Dispatchers.IO) { properties.load(inputStream) } }

        val clientId = properties.getProperty("CRAYPAY_CLIENT_ID")
        val clientSecret = properties.getProperty("CRAYPAY_CLIENT_SECRET")
        require(clientId.isNotEmpty())
        require(clientSecret.isNotEmpty())

        logger.notice("Fetching CrayPay auth token")
        return auth.getToken(clientId, clientSecret, "client_credentials", "dash_wallet_dev")
    }

    private fun convert(
        merchant: Endpoint.MerchantData,
        location: JsonObject
    ): MerchantData {
        val merchantData = merchant.merchant

        return MerchantData().apply {
            deeplink = convertJsonData("DeepLink", merchantData)
//            plusCode = null
//            addDate = null
//            updateDate = null
            paymentMethod = "gift card"
            merchantId = convertJsonData("Id", merchantData)
            active = true
            name = convertJsonData("LegalName", merchantData)
            address1 = getAddress1(location)
            address2 = getAddress2(location)
            address3 = convertJsonData("PostalCode", location)
            //            address4 = null
            latitude = getLatitude(location)
            longitude = getLongitude(location)
            website = convertJsonData("Website", merchantData)
            phone = convertJsonData("Phone", location)
            val inState = location.get("State")
            fixStatName(inState)?.apply {
                territory = this
            }
            city = convertJsonData("City", location)
            source = "DashDirect"
            sourceId = convertJsonData("Id", location)
            logoLocation = convertJsonData("LogoUrl", merchantData)
//            googleMaps = null
            coverImage = convertJsonData("CardImageUrl", merchantData)
            type = getType(merchant)

            monOpen = convertJsonData("MondayOpen", location)
            monClose = convertJsonData("MondayClose", location)
            tueOpen = convertJsonData("TuesdayOpen", location)
            tueClose = convertJsonData("TuesdayClose", location)
            wedOpen = convertJsonData("WednesdayOpen", location)
            wedClose = convertJsonData("WednesdayClose", location)
            thuOpen = convertJsonData("ThursdayOpen", location)
            thuClose = convertJsonData("ThursdayClose", location)
            friOpen = convertJsonData("FridayOpen", location)
            friClose = convertJsonData<String?>("FridayClose", location)
            satOpen = convertJsonData("SaturdayOpen", location)
            satClose = convertJsonData("SaturdayClose", location)
            sunOpen = convertJsonData("SundayOpen", location)
            sunClose = convertJsonData("SundayClose", location)
        }
    }

    private fun isValidLocation(type: String?, location: JsonObject): Boolean {
        if (type == null) {
            return false
        }

        if (type == "online") {
            return true
        }

        val isAddress1Empty = getAddress1(location).isNullOrEmpty()
        val isAddress2Empty = getAddress2(location).isNullOrEmpty()

        val isLatitudeEmpty = getLatitude(location) == 0.0
        val isLongitudeEmpty = getLongitude(location) == 0.0

        return !isAddress1Empty || !isAddress2Empty || !isLatitudeEmpty || !isLongitudeEmpty
    }

    private fun getType(merchant: Endpoint.MerchantData): String? {
        val merchantData = merchant.merchant
        val totalLocations = merchant.locations.size()
        val isPhysical = merchantData.getAsJsonPrimitive("IsPhysical").asBoolean
        val isOnline = merchantData.getAsJsonPrimitive("IsOnline").asBoolean
        return when {
            isPhysical && isOnline -> "both"
            isPhysical -> "physical"
            isOnline && totalLocations > 1 -> "both"
            isOnline -> "online"
            else -> {
                logger.error("Merchant has invalid type:\n$merchantData")
                null
            }
        }
    }

    private fun getAddress1(location: JsonObject): String? {
        return convertJsonData("Address1", location)
    }

    private fun getAddress2(location: JsonObject): String? {
        return convertJsonData("Address2", location)
    }

    private fun getLatitude(location: JsonObject): Double? {
        return convertJsonData("GpsLat", location)
    }

    private fun getLongitude(location: JsonObject): Double? {
        return convertJsonData("GpsLong", location)
    }
}
