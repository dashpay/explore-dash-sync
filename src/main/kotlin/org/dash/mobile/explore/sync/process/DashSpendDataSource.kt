package org.dash.mobile.explore.sync.process

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.annotations.SerializedName
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import okhttp3.OkHttpClient
import okhttp3.logging.HttpLoggingInterceptor
import org.dash.mobile.explore.sync.notice
import org.dash.mobile.explore.sync.process.data.MerchantData
import org.dash.mobile.explore.sync.slack.SlackMessenger
import org.slf4j.LoggerFactory
import retrofit2.HttpException
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.*
import java.io.IOException
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.collections.ArrayList

private const val BASE_URL = "https://spend.ctx.com/"

/**
 * Import data from DashSpend API
 */
class DashSpendDataSource(slackMessenger: SlackMessenger) :
    DataSource<MerchantData>(slackMessenger) {

    override val logger = LoggerFactory.getLogger(DashSpendDataSource::class.java)!!

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
            @SerializedName("pageSize") val pageSize: Int,
            @SerializedName("pageIndex") val pageIndex: Int
        )

        data class Pagination(
            val page: Int,
            val pages: Int,
            val perPage: Int,
            val total: Int
        )

        data class MerchantsResponse(
            @SerializedName("pagination") val pagination: Pagination,
            @SerializedName("result") val result: JsonArray,
        )

        @GET("merchants")
        suspend fun getAllMerchantLocationsOld(
            @Header("X-Api-Key") apiKey: String,
            @Header("X-Api-Secret") appKey: String,
            // TODO: pagination not tested
            // @Body requestData: AllMerchantLocationsRequest
        ): MerchantsResponse
    }

    private val apiService: Endpoint

    init {
        val gson = GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
            .create()

        val okHttpClient = OkHttpClient.Builder()
            .connectTimeout(20, TimeUnit.SECONDS)
            .writeTimeout(20, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .also { client ->
                val logging = HttpLoggingInterceptor { message -> println(message) }
                logging.level = HttpLoggingInterceptor.Level.HEADERS
                client.addInterceptor(logging)
            }
            .build()

        val retrofit: Retrofit = Retrofit.Builder()
            .baseUrl(BASE_URL)
            .addConverterFactory(GsonConverterFactory.create(gson))
            .client(okHttpClient)
            .build()

        apiService = retrofit.create(Endpoint::class.java)
    }

    var inactive = 0
    var invalid = 0

    override fun getRawData(): Flow<MerchantData> = flow {
        val properties = getProperties()
        val apiKey = properties.getProperty("X-Api-Key")
        val apiSecret = properties.getProperty("X-Api-Secret")
        require(apiKey.isNotEmpty())
        require(apiSecret.isNotEmpty())

        logger.notice("Importing data from CTX Spend ($BASE_URL)")

        val pageSize = 20000
        var currentPageIndex = 1
        var totalPages = currentPageIndex + 1
        var counter = 0

        while (currentPageIndex < totalPages) {
            try {
                val response = apiService.getAllMerchantLocationsOld(
                    apiKey,
                    apiSecret,
                    // Endpoint.AllMerchantLocationsRequest(pageSize, currentPageIndex)
                )

                if (!response.result.isJsonNull && !response.result.isEmpty) {
                    val responseData = response.result
                    val pagination = response.pagination
                    totalPages = pagination.pages + 1
                    currentPageIndex = pagination.page + 1
                    var currentRows = 0
                    currentRows = responseData.size()
                    logger.info("DashSpend ${currentPageIndex - 1}/${totalPages - 1} ($currentRows)")
                    logger.info("DashSpend.totalRows: ${pagination.total}")

                    responseData.forEach { merchant ->
                        val location = merchant
                        val locationData = location.asJsonObject
                            // TODO: does this properly count inactive locations
                            // does CTX mark them as inactive?
                            //if (locationData.get("IsActive").asBoolean) {
                                val type = getType(merchant)

                                if (isValidLocation(type, locationData)) {
                                    counter++
                                    val merchantData = convert(merchant, locationData)

                                    if (merchantData.name.isNullOrEmpty()) {
                                        invalid++
                                    } else {
                                        emit(merchantData)
                                    }
                                } else {
                                    invalid++
                                }
                            //} else {
                            //    inactive++
                            //}
                        }

                } else {
                    logger.error("error: $response")
                    break
                }
            } catch (ex: IOException) {
                logger.error(ex.message, ex)
            } catch (ex: HttpException) {
                logger.error(ex.message, ex)
                throw ex
            }
        }
        logger.notice("DashSpend - imported $counter records (inactive $inactive, invalid $invalid)")
        slackMessenger.postSlackMessage("DashSpend $counter records")
    }

    private fun convert(
        merchant: JsonElement,
        location: JsonObject
    ): MerchantData {
        val merchantData = merchant.asJsonObject

        return MerchantData().apply {
            deeplink = convertJsonData("deeplink", merchantData)
//            plusCode = null
//            addDate = null
//            updateDate = null
            paymentMethod = "gift card"
            merchantId = convertJsonData("merchantId", merchantData)
            active = true
            name = convertJsonData("name", merchantData)
            address1 = getAddress1(location)
            address2 = getAddress2(location)
            address3 = convertJsonData("postalCode", location)
            //            address4 = null
            latitude = getLatitude(location)
            longitude = getLongitude(location)
            website = convertJsonData("website", merchantData)
            phone = convertJsonData("phone", location)
            val inState = location.get("territory")
            inState?.let {
                fixStateName(inState)?.apply {
                    territory = this
                }
            }
            city = convertJsonData("city", location)
            source = "CTXSpend"
            sourceId = convertJsonData("sourceId", location)
            logoLocation = convertJsonData("logoUrl", merchantData)
//            googleMaps = null
            coverImage = convertJsonData("cardImageUrl", merchantData)
            type = getType(merchant)
            redeemType = convertJsonData("redeemType", merchantData)

            // TODO: Does CTX have these fields?
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

    private fun getType(merchant: JsonElement): String? {
        val merchantData = merchant
        //val totalLocations = merchant.locations.size()
        val isPhysical = merchantData.asJsonObject["type"].asString == "physical"
        val isOnline = merchantData.asJsonObject["type"].asString == "online"
        return when {
            isPhysical && isOnline -> "both"
            isPhysical -> "physical"
            //isOnline && totalLocations > 1 -> "both"
            isOnline -> "online"
            else -> {
                logger.error("Merchant has invalid type:\n$merchantData")
                null
            }
        }
    }

    private fun getAddress1(location: JsonObject): String? {
        return convertJsonData("address1", location)
    }

    private fun getAddress2(location: JsonObject): String? {
        return convertJsonData("address2", location)
    }

    private fun getLatitude(location: JsonObject): Double? {
        return convertJsonData("latitude", location)
    }

    private fun getLongitude(location: JsonObject): Double? {
        return convertJsonData("longitude", location)
    }
}
