package org.dash.mobile.explore.sync.process

import com.google.gson.GsonBuilder
import com.google.gson.annotations.SerializedName
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.Response
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
import java.util.concurrent.TimeUnit

private const val BASE_URL = "https://api.piggy.cards/dash/v1/"

/**
 * Import data from PiggyCards API
 */
class PiggyCardsDataSource(slackMessenger: SlackMessenger) :
    DataSource<MerchantData>(slackMessenger) {
        //private val userId = "user-3587"
    //private val password = ""
    private val userId =  "user-3516"
    private val password = "_xRXRr3ll$"
    private var token: String = ""
    override val logger = LoggerFactory.getLogger(PiggyCardsDataSource::class.java)!!

    interface Endpoint {
        data class Brand(
            val id: String,
            val name: String
        )
        data class Merchant(
            val id: String,
            val name: String,
            val description: String?,
            val website: String?,
            val logoUrl: String?,
            val cardImageUrl: String?,
            val category: String?,
            val country: String?,
            val savingsPercentage: Double?,
            val redeemType: String?,
            val denominationsType: String?,
            val enabled: Boolean = true
        )

        data class GiftcardResponse(
            val code: Int,
            val message: String,
            val data: List<Giftcard>?
        )

        data class Giftcard(
            val id: Int,
            val name: String,
            val description: String,
            val image: String,
            @SerializedName("price_type") val priceType: String,
            val currency: String,
            @SerializedName("discount_percentage") val discountPercentage: Double,
            @SerializedName("min_denomination") val minDenomination: Int,
            @SerializedName("max_denomination") val maxDenomination: Int,
            val denomination: String,
            val fee: Int,
            val quantity: Int,
            @SerializedName("brand_id") val brandId: Int
        )

        data class Location(
            val name: String,
            val latitude: Double,
            val longitude: Double,
            @SerializedName("street_number") val streetNumber: String,
            val street: String,
            val city: String,
            val state: String,
            val zip: String,
            @SerializedName("opening_hours") val openingHours: String,
            val phone: String,
            val shop: String,
            val website: String,
            val wheelchair: String
        )

        data class LoginRequest(
            @SerializedName("userId")
            val userId: String,
            @SerializedName("password")
            val password: String
        )

        data class LoginResponse(
            @SerializedName("access_token")
            val accessToken: String,
            @SerializedName("token_type")
            val tokenType: String,
            @SerializedName("expires_in")
            val expiresIn: Int
        )

        @POST("login")
        suspend fun login(@Body loginRequest: LoginRequest): LoginResponse

        @GET("brands/{country}")
        suspend fun getBrands(@Path("country") country: String): List<Brand>

        @GET("giftcards/{country}")
        suspend fun getGiftCards(
            @Path("country") country: String,
            @Query("brandId") brandId: String?
        ): GiftcardResponse

        @GET("locations/{id}")
        suspend fun getMerchantLocations(@Path("id") id: String): List<Location>
    }

    internal class PiggyCardsHeadersInterceptor(val getToken: () -> String) : Interceptor {

        override fun intercept(chain: Interceptor.Chain): Response {
            var request = chain.request()
            val requestBuilder = request.newBuilder()

            if (getToken().isNotEmpty()) {
                requestBuilder.addHeader("Authorization", "Bearer ${getToken()}")
            }

            request = requestBuilder.build()
            return chain.proceed(request)
        }
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
            //.also { client ->
            //    val logging = HttpLoggingInterceptor { message -> println(message) }
            //    logging.level = HttpLoggingInterceptor.Level.HEADERS
            //    client.addInterceptor(logging)
            //}
            .addInterceptor(PiggyCardsHeadersInterceptor() { token })
            .build()

        val retrofit: Retrofit = Retrofit.Builder()
            .baseUrl(BASE_URL)
            .addConverterFactory(GsonConverterFactory.create(gson))
            .client(okHttpClient)
            .build()

        apiService = retrofit.create(Endpoint::class.java)

        runBlocking {
            val loginResponse = apiService.login(Endpoint.LoginRequest(userId, password))
            token = loginResponse.accessToken
        }
    }

    var inactive = 0
    var invalid = 0

    override fun getRawData(): Flow<MerchantData> = flow {
        val properties = getProperties()
        val country = properties.getProperty("country", "US")
        // val brandId = properties.getProperty("brandId", "22")

        logger.notice("Importing data from PiggyCards ($BASE_URL)")

        var counter = 0

        try {
            val brands = apiService.getBrands(country)
            brands.forEach { brand ->
                logger.info("brand: $brand")
                val giftcardsResponse = apiService.getGiftCards(country, brand.id)

                if (giftcardsResponse.code == 200) {
                    logger.info("  PiggyCards Gift Cards: ${giftcardsResponse.data?.size ?: 0}")
                    var lastGiftcard: Endpoint.Giftcard? = null

                    val giftcard = giftcardsResponse.data?.firstOrNull()// { giftcard ->
                    giftcard?.let { giftcard ->
                        logger.info("    giftcard: $giftcard")
                        counter++
                        val merchantData = convert(brand, giftcard)

                        if (merchantData.name.isNullOrEmpty()) {
                            invalid++
                        } else {
                            val locations = apiService.getMerchantLocations(brand.id)
                            if (locations.isEmpty()) {
                                emit(merchantData.copy(type = "online"))
                            }
                            locations.forEach { location ->
                                logger.info("      location: $location")

                                if (isValidLocation("", location)) {
                                    val merchantWithLocation = merchantData.copy(
                                        address1 = location.streetNumber + " " + location.street,
                                        city = location.city,
                                        territory = location.state,
                                        latitude = location.latitude,
                                        longitude = location.longitude,
                                        type = "physical"
                                    )
                                    emit(merchantWithLocation)
                                }
                            }
                        }
                    } ?: {
                        logger.info("there is a problem with $giftcardsResponse")
                    }
                    //}
                } else {
                    logger.error("PiggyCards API error: ${giftcardsResponse.code} - ${giftcardsResponse.message}")
                }
            }
        } catch (ex: IOException) {
            logger.error(ex.message, ex)
        } catch (ex: HttpException) {
            logger.error(ex.message, ex)
            throw ex
        } catch (ex: NullPointerException) {
            logger.error(ex.message, ex)
        }

        logger.notice("PiggyCards - imported $counter records (inactive $inactive, invalid $invalid)")
        slackMessenger.postSlackMessage("PiggyCards $counter records")
    }

    private fun convert(
        brand: Endpoint.Brand,
        giftcard: Endpoint.Giftcard,
    ): MerchantData {
        return MerchantData().apply {
            deeplink = null
            paymentMethod = "gift card"
            merchantId = brand.id
            active = true
            name = brand.name
            address1 = "online"
            address2 = null
            address3 = null
            latitude = null
            longitude = null
            website = null
            phone = null
            territory = null
            city = null
            source = "PiggyCards"
            sourceId = giftcard.id.toString()
            logoLocation = giftcard.image
            coverImage = giftcard.image
            type = "online"
            redeemType = "barcode"
            savingsPercentage = (giftcard.discountPercentage * 100).toInt()
            denominationsType = if (giftcard.priceType == "Range") {
                "min-max"
            } else {
                "fixed"
            }
        }
    }

    private fun isValidLocation(type: String?, location: Endpoint.Location): Boolean {
        if (type == null) {
            return false
        }

        if (type == "online") {
            return true
        }

        val isAddress1Empty = location.street.isNullOrEmpty() || location.streetNumber.isNullOrEmpty()
        val isLatitudeEmpty = location.latitude == null || location.latitude == 0.0
        val isLongitudeEmpty = location.longitude == null || location.longitude == 0.0

        return !isAddress1Empty || !isLatitudeEmpty || !isLongitudeEmpty
    }

//    private fun getType(merchant: Endpoint.Merchant, location: Endpoint.Location): String? {
//        val hasPhysicalAddress = !location.address1.isNullOrEmpty() ||
//                                 !location.address2.isNullOrEmpty() ||
//                                 (location.latitude != null && location.latitude != 0.0) ||
//                                 (location.longitude != null && location.longitude != 0.0)
//
//        val hasWebsite = !merchant.website.isNullOrEmpty()
//
//        return when {
//            hasPhysicalAddress && hasWebsite -> "both"
//            hasPhysicalAddress -> "physical"
//            hasWebsite -> "online"
//            else -> {
//                logger.error("Merchant has invalid type: ${merchant.name}")
//                null
//            }
//        }
//    }
}