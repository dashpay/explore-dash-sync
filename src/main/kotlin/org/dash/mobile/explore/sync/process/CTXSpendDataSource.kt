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
import org.dash.mobile.explore.sync.DataSourceReport
import org.dash.mobile.explore.sync.notice
import org.dash.mobile.explore.sync.process.data.MerchantData
import org.dash.mobile.explore.sync.slack.SlackMessenger
import org.slf4j.LoggerFactory
import retrofit2.HttpException
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.*
import java.io.IOException
import java.util.concurrent.TimeUnit
import kotlin.let

private const val BASE_URL = "https://spend.ctx.com/"

/**
 * Import data from CTXSpend API
 */
class CTXSpendDataSource(slackMessenger: SlackMessenger) :
    DataSource<MerchantData>(slackMessenger) {
    override val logger = LoggerFactory.getLogger(CTXSpendDataSource::class.java)!!
    val merchantList = hashSetOf<String>()
    var dataSourceReport: DataSourceReport? = null

    interface Endpoint {
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
        suspend fun getAllMerchants(
            @Header("X-Api-Key") apiKey: String,
            @Header("X-Api-Secret") appKey: String,
            // TODO: pagination not tested
            @Query("perPage") perPage: Int = 20,
            @Query("page") page: Int = 1
        ): MerchantsResponse

        @GET("dcg/locations")
        suspend fun getAllMerchantLocations(
            @Header("X-Api-Key") apiKey: String,
            @Header("X-Api-Secret") appKey: String,
        ): JsonArray
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

        val pageSize = 100
        var currentPageIndex = 1
        var totalPages = currentPageIndex + 1

        val merchants = linkedMapOf<String, JsonObject>()
        val disabledMerchants = linkedMapOf<String, JsonObject>()

        while (currentPageIndex < totalPages) {
            try {
                val response = apiService.getAllMerchants(
                    apiKey,
                    apiSecret,
                    pageSize,
                    currentPageIndex
                )

                if (!response.result.isJsonNull && !response.result.isEmpty) {
                    val responseData = response.result
                    val pagination = response.pagination
                    totalPages = pagination.pages + 1
                    currentPageIndex = pagination.page + 1
                    val currentRows = responseData.size()
                    logger.info("CTXSpend Merchants ${currentPageIndex - 1}/${totalPages - 1} ($currentRows)")
                    logger.info("CTXSpend Merchants / totalRows: ${pagination.total}")

                    responseData.forEach { merchant ->
                        val merchantData = merchant.asJsonObject
                        if (merchants.containsKey(merchantData["id"].asString)) {
                            logger.warn("merchant already exists")
                        }
                        // if a merchant is disabled, then exclude it from the merchant list
                        // CTX will exclude the locations from the locations API response
                        val disabled = !merchantData["enabled"].asBoolean
                        if (!disabled) {
                            merchants[merchantData["id"].asString] = merchantData.deepCopy()
                            merchantList.add(merchantData["name"].asString)
                        } else {
                            disabledMerchants[merchantData["id"].asString] = merchantData.deepCopy()
                        }
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
        logger.info("CTXSpend Merchants: ${merchants.size}")
        logger.info("CTXSpend Disabled Merchants: (${disabledMerchants.map { it.value["name"] }.joinToString(", ") }.)")
        // load locations
        var counter = 0
        val locationResponse = apiService.getAllMerchantLocations(apiKey, apiSecret)
        val invalidLocations = linkedMapOf<String, JsonObject>()

        if (!locationResponse.isJsonNull && !locationResponse.isEmpty) {
            logger.info("CTXSpend Locations Records: ${locationResponse.size()}")
            locationResponse.forEach { location ->
                val locationData = location.asJsonObject

                // do we have the merchant information
                val merchantId = locationData["merchantId"]
                merchants[merchantId.asString]?.let { merchant ->
                    val type = getType(merchant, locationData)

                    if (isValidLocation(type, locationData)) {
                        counter++
                        val merchantData = convert(merchant, locationData)

                        if (merchantData.name.isNullOrEmpty() || merchantData.address1?.contains("Address 1") == true) {
                            invalid++
                            invalidLocations[locationData["merchantId"].asString] = locationData
                        } else {
                            emit(merchantData)
                        }
                    } else {
                        invalid++
                        invalidLocations[locationData["id"].asString] = locationData

                    }
                } ?: logMissingMerchant(merchantId, merchants)
            }
        }
        logger.info("CTXSpend $counter records (inactive $inactive, invalid (${ 
            invalidLocations.map { 
                it.value["merchantId"]
            }.joinToString(", ") 
        }), locations missing ${missingMerchants})")
        dataSourceReport = DataSourceReport(
            "CTX",
            merchants.size,
            counter,
            disabledMerchants.map { it.value["name"].asString }
        )
    }

    private val missingMerchants = hashSetOf<String>()

    private fun logMissingMerchant(merchantId: JsonElement, merchants: Map<String, JsonObject>) {
        if (!missingMerchants.contains(merchantId.asString)) {
            logger.warn("merchant id not found: {}: {}", merchantId.asString, merchants[merchantId.asString]?.get("name"))
        } else {
            missingMerchants.add(merchantId.asString)
        }
    }

    private fun convert(
        merchant: JsonObject,
        location: JsonObject,
    ): MerchantData {
        val merchantData = merchant.asJsonObject

        return MerchantData().apply {
            deeplink = convertJsonData("deeplink", merchantData)
            paymentMethod = "gift card"
            merchantId = convertJsonData("id", merchantData)
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
            val inState = location["territory"]
            inState?.let {
                fixStateName(inState)?.apply {
                    territory = this
                }
            }
            city = convertJsonData("city", location)
            source = "CTX"
            sourceId = convertJsonData("sourceId", location)
            logoLocation = convertJsonData("logoUrl", merchantData)
            coverImage = convertJsonData("cardImageUrl", merchantData)
            type = getType(merchant, location)
            redeemType = convertJsonData("redeemType", merchantData)
            savingsPercentage = convertJsonData("savingsPercentage", merchantData)
            denominationsType = convertJsonData("denominationsType", merchantData)

            // TODO: Does CTX have these fields? no
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

    private fun getType(merchant: JsonObject, location: JsonObject): String? {

        val isPhysical = merchant["type"].asString == "physical"
        val isOnline = merchant["type"].asString == "online"
        return when {
            isPhysical && isOnline -> "both"
            isPhysical -> "physical"
            isOnline && location["address1"].asString != "online" -> "physical" // CTX marks all merchants as online, but if the location has an address, then it is physical
            isOnline -> "online"
            else -> {
                logger.error("Merchant has invalid type:\n$merchant")
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

    fun getReport(): DataSourceReport {
        return dataSourceReport ?: throw IllegalStateException("Report not yet generated. Call getRawData() first.")
    }
}
