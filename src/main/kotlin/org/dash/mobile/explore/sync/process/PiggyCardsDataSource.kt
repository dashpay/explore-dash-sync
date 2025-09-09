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
import org.dash.mobile.explore.sync.DataSourceReport
import org.dash.mobile.explore.sync.OperationMode
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
import kotlin.math.max

private const val PROD_BASE_URL = "https://api.piggy.cards/dash/v1/"
private const val DEV_BASE_URL = "https://apidev.piggy.cards/dash/v1/"
private const val BASE_URL = DEV_BASE_URL
/**
 * Import data from PiggyCards API
 */
class PiggyCardsDataSource(slackMessenger: SlackMessenger, private val mode: OperationMode) :
    DataSource<MerchantData>(slackMessenger) {
    private lateinit var userId: String
    private lateinit var password: String
    private var token: String = ""
    override val logger = LoggerFactory.getLogger(PiggyCardsDataSource::class.java)!!
    val merchantList = hashSetOf<String>()
    var dataSourceReport: DataSourceReport? = null

    interface Endpoint {
        data class Brand(
            val id: String,
            val name: String
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
        ) {
            val isFixed = priceType == "Fixed"

            fun toShortString(): String {
                return "GiftCard(id=$id, name=$name, priceType=$priceType, currency=$currency, discount=$discountPercentage, min=$minDenomination, max=$maxDenomination, denom=$denomination, fee=$fee, brand=$brandId)"
            }
        }

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
            .addInterceptor(PiggyCardsHeadersInterceptor() { token })
            .also { client ->
                val logging = HttpLoggingInterceptor { message -> println(message) }
                logging.level = HttpLoggingInterceptor.Level.HEADERS
                client.addInterceptor(logging)
            }
            .build()
        val baseUrl = if (mode == OperationMode.TESTNET) {
            DEV_BASE_URL
        } else {
            PROD_BASE_URL
        }
        val retrofit: Retrofit = Retrofit.Builder()
            .baseUrl(baseUrl)
            .addConverterFactory(GsonConverterFactory.create(gson))
            .client(okHttpClient)
            .build()

        apiService = retrofit.create(Endpoint::class.java)

        runBlocking {
            val properties = getProperties()
            if (mode == OperationMode.TESTNET) {
                userId = properties.getProperty("PIGGY_CARDS_USER_ID")
                password = properties.getProperty("PIGGY_CARDS_PASSWORD")
            } else if (mode == OperationMode.PRODUCTION) {
                userId = properties.getProperty("PIGGY_CARDS_USER_ID_PROD")
                password = properties.getProperty("PIGGY_CARDS_PASSWORD_PROD")
            }
            val loginResponse = apiService.login(Endpoint.LoginRequest(userId, password))
            token = loginResponse.accessToken
        }
    }

    var inactive = 0
    var invalid = 0

    override fun getRawData(): Flow<MerchantData> = flow {
        val properties = getProperties()
        val country = properties.getProperty("country", "US")

        logger.notice("Importing data from PiggyCards ($BASE_URL)")

        var locationCount = 0
        val invalidLocations = linkedMapOf<String, Endpoint.Location>()

        try {
            val brands = apiService.getBrands(country)
            logger.info("PiggyCard Merchants: ${brands.size}")
            brands.forEach { brand ->
                logger.info("brand: $brand")
                merchantList.add(brand.name)
                val giftcardsResponse = apiService.getGiftCards(country, brand.id)

                if (giftcardsResponse.code == 200) {
                    logger.info("  PiggyCards Gift Cards: ${giftcardsResponse.data?.size ?: 0}")
                    var discountPercentage = 0.0
                    val immediateDeliveryCards = arrayListOf<Endpoint.Giftcard>()
                    giftcardsResponse.data?.forEach { giftcard ->
                        val info = if (giftcard.priceType == "Fixed") {
                            giftcard.denomination
                        } else {
                            "(${giftcard.minDenomination}, ${giftcard.maxDenomination})"
                        }
                        logger.info("    giftcard: ${giftcard.name}, type = ${giftcard.priceType}[$info ${giftcard.currency}], discount=${giftcard.discountPercentage}, inv=${giftcard.quantity}")
                        discountPercentage = max(discountPercentage, giftcard.discountPercentage)
                        if (giftcard.name.lowercase().contains("(instant delivery)")) {
                            immediateDeliveryCards.add(giftcard)
                        }
                    }
                    
                    // if immediate delivery cards are available, then they get priority
                    if (immediateDeliveryCards.isNotEmpty()) {
                        discountPercentage = immediateDeliveryCards.first().discountPercentage
                    }

                    // choose the first non-fixed card if available, otherwise the first card
                    val giftcard = giftcardsResponse.data?.first { !it.isFixed }
                        ?: giftcardsResponse.data?.first()?.copy(discountPercentage = discountPercentage)
                    if (giftcard != null) {
                        // logger.info("    giftcard: ${giftcard.name}, type = ${giftcard.priceType}")
                        val merchantData = convert(brand, giftcard)

                        if (merchantData.name.isNullOrEmpty()) {
                            invalid++
                        } else {
                            val locations = apiService.getMerchantLocations(brand.id)
                            // add the online entry
                            emit(merchantData.copy(type = "online"))

                            var locationsAdded = 0
                            locations.forEach { location ->
                                // logger.info("      location: $location")
                                if (isValidLocation("physical", location)) {
                                    val merchantWithLocation = merchantData.copy(
                                        address1 = createAddress(location),
                                        city = location.city,
                                        territory = fixStateName(location.state),
                                        latitude = location.latitude,
                                        longitude = location.longitude,
                                        website = location.website,
                                        type = "physical"
                                    )
                                    emit(merchantWithLocation)
                                    locationsAdded++
                                    locationCount++
                                } else {
                                    invalidLocations[location.name] = location
                                }
                            }
                            if (locations.isNotEmpty()) {
                                logger.info("{} locations {} of {}", brand.name, locationsAdded, locations.size)
                            }
                        }
                    } else {
                        logger.info("there is a problem with $giftcardsResponse for ")
                    }
                } else {
                    logger.error("PiggyCards API error: ${giftcardsResponse.code} - ${giftcardsResponse.message}")
                }
            }
            dataSourceReport = DataSourceReport(
                "PiggyCards",
                brands.size,
                locationCount
            )
        } catch (ex: IOException) {
            logger.error(ex.message, ex)
        } catch (ex: HttpException) {
            logger.error(ex.message, ex)
            throw ex
        } catch (ex: NullPointerException) {
            logger.error(ex.message, ex)
        }
        logger.info("PiggyCards - imported $locationCount records (invalid ${ 
            invalidLocations.map { it.value.name }.joinToString(", ") 
        })")

    }

    private fun createAddress(location: Endpoint.Location): String {
        return if (location.streetNumber != "Unknown") {
            location.streetNumber + " " + location.street
        } else {
            location.street
        }
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
            territory = ""
            city = null
            source = "PiggyCards"
            sourceId = brand.id
            logoLocation = giftcard.image
            coverImage = giftcard.image
            type = "online"
            redeemType = "barcode"
            // these fields may not be correct, just based on a single card
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

    fun getReport(): DataSourceReport {
        return dataSourceReport ?: throw IllegalStateException("Report not yet generated. Call getRawData() first.")
    }
}