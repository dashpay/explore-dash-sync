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
import java.io.File
import java.io.IOException
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import kotlin.math.max

private const val PROD_BASE_URL = "https://api.piggy.cards/dash/v1/"
private const val BASE_URL = PROD_BASE_URL
/**
 * Import data from PiggyCards API
 */
class PiggyCardsDataSource(slackMessenger: SlackMessenger, private val mode: OperationMode, debugMode: Boolean) :
    DataSource<MerchantData>(slackMessenger, debugMode) {
        companion object {
            const val SERVICE_FEE = 150 // 1.5% for CurPay
        }
    private lateinit var userId: String
    private lateinit var password: String
    private var token: String = ""
    override val logger = LoggerFactory.getLogger(PiggyCardsDataSource::class.java)!!
    val merchantList = hashSetOf<String>()

    // Member fields for HTML generation
    private var allBrands = mutableListOf<Endpoint.Brand>()
    private var brandToGiftcards = mutableMapOf<String, List<Endpoint.Giftcard>>()

    val disabledList = arrayListOf<String>(
        // no items
    )
    val disabledGiftcards = mapOf(
        "174" to listOf("Xbox Live", "Xbox Game Pass")
    )
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
            @SerializedName("min_denomination") val minDenomination: Double,
            @SerializedName("max_denomination") val maxDenomination: Double,
            val denomination: String,
            val fee: Int,
            val quantity: Int,
            @SerializedName("brand_id") val brandId: Int
        ) {
            val isFixed = priceType == "Fixed" || priceType == "Option"

            fun toShortString(): String {
                return "GiftCard(id=$id, name=$name, priceType=$priceType, currency=$currency, discount=$discountPercentage, min=$minDenomination, max=$maxDenomination, denom=$denomination, fee=$fee, brand=$brandId)"
            }
        }

        /**
         * Location
         * {
         *      "name":"Burger King",
         *      "latitude":49.666313,
         *      "longitude":-112.793823,
         *      "street_number":"2416",
         *      "street":"Fairway Plaza Rd S",
         *      "country":"CA",
         *      "city":"Lethbridge",
         *      "state":"AB",
         *      "zip":"Unknown",
         *      "opening_hours":"Unknown",
         *      "phone":"(403) 380-4771",
         *      "shop":"Unknown",
         *      "website":"Unknown",
         *      "wheelchair":"Unknown"
         * }
         *
         */

        data class Location(
            val name: String,
            val latitude: Double,
            val longitude: Double,
            @SerializedName("street_number") val streetNumber: String,
            val street: String,
            val city: String,
            val state: String,
            val zip: String,
            val country: String,
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
                logging.level = loggingLevel
                logging.redactHeader("Authorization")
                client.addInterceptor(logging)
            }
            .build()

        val retrofit: Retrofit = Retrofit.Builder()
            .baseUrl(PROD_BASE_URL)
            .addConverterFactory(GsonConverterFactory.create(gson))
            .client(okHttpClient)
            .build()

        apiService = retrofit.create(Endpoint::class.java)

        runBlocking {
            val properties = getProperties()
            userId = properties.getProperty("PIGGY_CARDS_USER_ID_PROD")
            password = properties.getProperty("PIGGY_CARDS_PASSWORD_PROD")

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
        val negativeDiscountBrands = arrayListOf<MerchantData>()

        try {
            val brands = apiService.getBrands(country)
            allBrands.clear()
            allBrands.addAll(brands)
            brandToGiftcards.clear()
            logger.info("PiggyCard Merchants: ${brands.size}")
            brands.forEach { brand ->
                logger.info("brand: $brand")
                merchantList.add(brand.name)
                val giftcardsResponse = try {
                    apiService.getGiftCards(country, brand.id)
                } catch (e: Exception) {
                    logger.error("error obtaining giftcard information: ", e)
                    null
                }

                if (giftcardsResponse != null && giftcardsResponse.code == 200) {
                    logger.info("  PiggyCards Gift Cards: ${giftcardsResponse.data?.size ?: 0}")
                    brandToGiftcards[brand.id] = giftcardsResponse.data ?: emptyList()
                    var discountPercentage = 0.0
                    val immediateDeliveryCards = arrayListOf<Endpoint.Giftcard>()
                    // remove disabled cards
                    val disabledGiftCardsForMerchant = disabledGiftcards[brand.id].orEmpty()
                    val giftCards = if (disabledGiftCardsForMerchant.isEmpty()) {
                        giftcardsResponse.data
                    } else {
                        giftcardsResponse.data?.filter { giftCard ->
                            !disabledGiftCardsForMerchant.any { name -> giftCard.name.contains(name) }
                        }
                    }

                    giftCards?.forEach { giftCard ->
                        val info = if (giftCard.priceType == "Fixed") {
                            giftCard.denomination
                        } else {
                            "(${giftCard.minDenomination}, ${giftCard.maxDenomination})"
                        }
                        logger.info("    giftCard: ${giftCard.name}, type = ${giftCard.priceType}[$info ${giftCard.currency}], discount=${giftCard.discountPercentage}, inv=${giftCard.quantity}")
                        discountPercentage = max(discountPercentage, giftCard.discountPercentage)
                        if (giftCard.name.lowercase().contains("(instant delivery)")) {
                            immediateDeliveryCards.add(giftCard)
                        }
                    }
                    if (giftCards != null && immediateDeliveryCards.isNotEmpty()) {
                        // add rest of fixed cards
                        immediateDeliveryCards.addAll(giftCards.filter { it.priceType == "Fixed" && !it.name.contains("(instant delivery)")} )
                    }

                    // choose the first non-fixed card if available, otherwise the first card
                    val firstRangeCard = giftCards?.firstOrNull { !it.isFixed }
                    val giftCard = when {
                        immediateDeliveryCards.isNotEmpty() -> {
                            discountPercentage = immediateDeliveryCards.maxOf { it.discountPercentage }
                            immediateDeliveryCards.first().copy(discountPercentage = discountPercentage)
                        }
                        firstRangeCard != null -> firstRangeCard
                        !giftCards.isNullOrEmpty() -> {
                            discountPercentage = giftCards.maxOf { it.discountPercentage }
                            giftCards.first().copy(discountPercentage = discountPercentage)
                        }
                        else -> null
                    }
                    if (giftCard != null) {
                        val merchantData = convert(brand, giftCard)

                        if (merchantData.name.isNullOrEmpty()) {
                            invalid++
                        } else if ((merchantData.savingsPercentage ?: 0) < 0) {
                            invalid++
                            negativeDiscountBrands.add(merchantData)
                        } else {
                            val locations = apiService.getMerchantLocations(brand.id)
                            // add the online entry
                            emit(merchantData.copy(type = "online"))

                            var locationsAdded = 0
                            locations.forEach { location ->
                                if (isValidLocation("physical", location) && location.country == country) {
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
                    logger.error("PiggyCards API error: ${giftcardsResponse?.code} - ${giftcardsResponse?.message}")
                }
            }
            dataSourceReport = DataSourceReport(
                "PiggyCards",
                brands.size,
                locationCount,
                disabledList,
                negativeDiscountBrands.map { it.name ?: "unknown" }
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
            name = MerchantNameNormalizer.removeSuffix(brand.name)
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
            savingsPercentage = (giftcard.discountPercentage * 100).toInt() - SERVICE_FEE
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

        val isAddress1Empty = location.street.isEmpty() || location.streetNumber.isEmpty()
        val isLatitudeEmpty = location.latitude == 0.0
        val isLongitudeEmpty = location.longitude == 0.0

        return !isAddress1Empty || !isLatitudeEmpty || !isLongitudeEmpty
    }

    fun getReport(): DataSourceReport {
        return dataSourceReport ?: throw IllegalStateException("Report not yet generated. Call getRawData() first.")
    }

    override fun generateHtmlFile(): String? {
        if (allBrands.isEmpty()) {
            logger.warn("No brands data available for HTML generation")
            return null
        }

        val currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
        val filename = "piggycards-${this.mode}-$currentDate.html"

        logger.info("Generating HTML file: $filename")

        val htmlContent = generateHtmlContent(allBrands, brandToGiftcards, currentDate)

        return try {
            val file = File(filename)
            file.writeText(htmlContent)
            logger.info("HTML file generated successfully: ${file.absolutePath}")
            filename
        } catch (ex: IOException) {
            logger.error("Failed to write HTML file: ${ex.message}", ex)
            null
        }
    }

    private fun generateHtmlContent(brands: List<Endpoint.Brand>, brandToGiftcards: Map<String, List<Endpoint.Giftcard>>, currentDate: String): String {
        val brandsJson = brands.joinToString(",\n") { brand ->
            """        { "id": "${brand.id}", "name": "${escapeJson(brand.name)}" }"""
        }

        val giftcardsJson = brandToGiftcards.entries.joinToString(",\n") { (brandId, giftcards) ->
            val cardsJson = giftcards.joinToString(",\n") { card ->
                val effectiveDiscount = ((card.discountPercentage * 100) - SERVICE_FEE) / 100.0
                """            {
                "id": ${card.id},
                "name": "${escapeJson(card.name)}",
                "description": "${escapeJson(cleanHtmlDescription(card.description))}",
                "image": "${escapeJson(card.image)}",
                "priceType": "${escapeJson(card.priceType)}",
                "currency": "${escapeJson(card.currency)}",
                "discountPercentage": ${card.discountPercentage},
                "effectiveDiscount": $effectiveDiscount,
                "minDenomination": ${card.minDenomination},
                "maxDenomination": ${card.maxDenomination},
                "denomination": "${escapeJson(card.denomination)}",
                "fee": ${card.fee},
                "quantity": ${card.quantity},
                "brandId": ${card.brandId}
            }"""
            }
            """        "$brandId": [$cardsJson]"""
        }

        return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PiggyCards ${this.mode} - $currentDate</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f5f5f5; height: 100vh; overflow: hidden; }
        .container { display: flex; height: 100vh; }
        .sidebar { width: 300px; background-color: #fff; border-right: 1px solid #e0e0e0; overflow-y: auto; box-shadow: 2px 0 10px rgba(0,0,0,0.1); }
        .sidebar-header { padding: 20px; background-color: #4CAF50; color: white; text-align: center; }
        .sidebar-header h1 { font-size: 1.2em; margin-bottom: 5px; }
        .brand-list { padding: 0; }
        .brand-item { padding: 15px 20px; border-bottom: 1px solid #f0f0f0; cursor: pointer; transition: background-color 0.2s; display: flex; align-items: center; }
        .brand-item:hover { background-color: #f8f9fa; }
        .brand-item.active { background-color: #e3f2fd; border-left: 4px solid #2196F3; }
        .brand-logo { width: 40px; height: 40px; margin-right: 12px; border-radius: 4px; object-fit: cover; background-color: #f0f0f0; }
        .brand-name { font-weight: 500; color: #333; }
        .main-content { flex: 1; background-color: #fff; overflow-y: auto; padding: 20px; }
        .content-header { margin-bottom: 30px; padding-bottom: 20px; border-bottom: 2px solid #f0f0f0; }
        .brand-title { font-size: 2.2em; color: #333; margin-bottom: 10px; display: flex; align-items: center; }
        .brand-title img { width: 60px; height: 60px; margin-right: 15px; border-radius: 8px; object-fit: cover; }
        .giftcards-section { margin-bottom: 40px; }
        .section-title { font-size: 1.5em; color: #555; margin-bottom: 20px; padding-bottom: 10px; border-bottom: 1px solid #e0e0e0; }
        .discount-positive { color: #4CAF50; font-weight: bold; }
        .discount-negative { color: #f44336; font-weight: bold; }
        .brand-description { background-color: #f8f9fa; padding: 25px; border-radius: 8px; margin-top: 20px; }
        .brand-description h3 { color: #333; margin-bottom: 15px; font-size: 1.3em; }
        .attributes-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-top: 20px; }
        .attribute { display: flex; flex-direction: column; padding: 12px; background-color: #fff; border-radius: 6px; border-left: 4px solid #2196F3; }
        .attribute-label { font-size: 0.85em; color: #666; text-transform: uppercase; margin-bottom: 4px; }
        .attribute-value { font-weight: 500; color: #333; }
        .empty-state { text-align: center; padding: 100px 20px; color: #999; }
        .empty-state h2 { margin-bottom: 10px; }
        .status-bar { background-color: #4CAF50; color: white; padding: 10px 20px; text-align: center; font-size: 0.9em; }
        .table-container { overflow-x: auto; border: 1px solid #e0e0e0; border-radius: 8px; background-color: white; }
        .giftcards-table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
        .giftcards-table th { background-color: #f8f9fa; color: #555; font-weight: 600; padding: 12px 8px; text-align: left; border-bottom: 2px solid #e0e0e0; white-space: nowrap; }
        .giftcards-table td { padding: 10px 8px; border-bottom: 1px solid #f0f0f0; color: #333; }
        .giftcards-table tbody tr:hover { background-color: #f8f9fa; }
        .giftcards-table tbody tr:last-child td { border-bottom: none; }
        .giftcards-table tbody tr.selected { background-color: #e3f2fd !important; border-left: 4px solid #2196F3; }
        .giftcard-descriptions { background-color: #f8f9fa; padding: 25px; border-radius: 8px; margin-top: 20px; }
        .giftcard-descriptions h3 { color: #333; margin-bottom: 20px; font-size: 1.3em; }
        .description-item { background-color: white; padding: 15px; border-radius: 6px; border-left: 4px solid #2196F3; }
        .description-item h4 { color: #333; margin-bottom: 8px; font-size: 1.1em; }
        .description-item p { color: #666; line-height: 1.4; margin: 0; }
    </style>
</head>
<body>
    <div class="status-bar" id="statusBar">Data loaded from PiggyCards API</div>
    <div class="container">
        <div class="sidebar">
            <div class="sidebar-header">
                <h1>PiggyCards</h1>
                <div>${this.mode} - $currentDate</div>
            </div>
            <div class="brand-list" id="brandList"></div>
        </div>
        <div class="main-content" id="mainContent">
            <div class="empty-state">
                <h2>Select a Brand</h2>
                <p>Choose a brand from the left sidebar to view its gift cards and details.</p>
            </div>
        </div>
    </div>
    <script>
        const brands = [
$brandsJson
        ];
        const giftcards = {
$giftcardsJson
        };

        class PiggyCardsViewer {
            constructor() {
                this.selectedBrand = null;
                this.statusBar = document.getElementById('statusBar');
                this.brandList = document.getElementById('brandList');
                this.mainContent = document.getElementById('mainContent');
                this.init();
            }

            init() {
                this.renderBrandList();
                this.updateStatus(`Loaded ${'$'}{brands.length} brands with ${'$'}{Object.values(giftcards).reduce((sum, cards) => sum + cards.length, 0)} total gift cards`);
            }

            renderBrandList() {
                this.brandList.innerHTML = '';
                brands.forEach(brand => {
                    const brandElement = document.createElement('div');
                    brandElement.className = 'brand-item';
                    brandElement.onclick = () => this.selectBrand(brand, brandElement);
                    const giftcardsForBrand = giftcards[brand.id] || [];
                    const firstCard = giftcardsForBrand[0];
                    const logoUrl = firstCard ? firstCard.image : '';
                    brandElement.innerHTML = `
                        <img class="brand-logo" src="${'$'}{logoUrl}" alt="${'$'}{brand.name}" onerror="this.style.display='none'">
                        <div class="brand-name">${'$'}{brand.name} (${'$'}{giftcardsForBrand.length})</div>
                    `;
                    this.brandList.appendChild(brandElement);
                });
            }

            selectBrand(brand, element) {
                document.querySelectorAll('.brand-item').forEach(item => item.classList.remove('active'));
                element.classList.add('active');
                this.selectedBrand = brand;
                this.renderBrandDetails(brand);
            }

            renderBrandDetails(brand) {
                const brandGiftcards = giftcards[brand.id] || [];
                const firstCard = brandGiftcards[0];
                const logoUrl = firstCard ? firstCard.image : '';
                this.mainContent.innerHTML = `
                    <div class="content-header">
                        <div class="brand-title">
                            <img src="${'$'}{logoUrl}" alt="${'$'}{brand.name}" onerror="this.style.display='none'">
                            ${'$'}{brand.name}
                        </div>
                    </div>
                    <div class="brand-description">
                        <h3>Brand Information</h3>
                        <div class="attributes-grid">
                            <div class="attribute"><div class="attribute-label">Brand ID</div><div class="attribute-value">${'$'}{brand.id}</div></div>
                            <div class="attribute"><div class="attribute-label">Total Cards</div><div class="attribute-value">${'$'}{brandGiftcards.length}</div></div>
                            <div class="attribute"><div class="attribute-label">Currency</div><div class="attribute-value">${'$'}{firstCard ? firstCard.currency : 'USD'}</div></div>
                            <div class="attribute"><div class="attribute-label">Max Discount</div><div class="attribute-value">${'$'}{brandGiftcards.length ? Math.max(...brandGiftcards.map(c => c.discountPercentage || 0)).toFixed(2) : 0}%</div></div>
                            <div class="attribute"><div class="attribute-label">Available Quantity</div><div class="attribute-value">${'$'}{brandGiftcards.reduce((sum, c) => sum + (c.quantity || 0), 0)}</div></div>
                        </div>
                    </div>
                    <div class="giftcards-section">
                        <h2 class="section-title">Gift Cards (${'$'}{brandGiftcards.length})</h2>
                        <div class="table-container">
                            <table class="giftcards-table">
                                <thead>
                                    <tr>
                                        <th>Name</th><th>Type</th><th>Currency</th><th>Denomination</th>
                                        <th>Discount</th><th>Net Discount</th><th>Fee</th><th>Quantity</th><th>ID</th>
                                    </tr>
                                </thead>
                                <tbody>${'$'}{brandGiftcards.map(card => this.renderGiftcardRow(card)).join('')}</tbody>
                            </table>
                        </div>
                    </div>
                    <div class="giftcard-descriptions" id="giftcardDescriptions" style="display: none;">
                        <h3>Gift Card Description</h3>
                        <div class="description-item">
                            <h4 id="selectedCardName"></h4>
                            <p id="selectedCardDescription"></p>
                        </div>
                    </div>
                `;
            }

            renderGiftcardRow(card) {
                const discount = card.discountPercentage || 0;
                const effectiveDiscount = card.effectiveDiscount || 0;
                const discountClass = discount > 0 ? 'discount-positive' : discount < 0 ? 'discount-negative' : '';
                const effectiveClass = effectiveDiscount > 0 ? 'discount-positive' : 'discount-negative';
                return `
                    <tr onclick="window.piggyCardsViewer.selectGiftcard(${'$'}{card.id})" style="cursor: pointer;">
                        <td>${'$'}{card.name}</td><td>${'$'}{card.priceType}</td><td>${'$'}{card.currency}</td>
                        <td>${'$'}{card.denomination}</td>
                        <td class="${'$'}{discountClass}">${'$'}{discount.toFixed(2)}%</td>
                        <td class="${'$'}{effectiveClass}">${'$'}{effectiveDiscount.toFixed(0)} bps</td>
                        <td>${'$'}{card.fee}</td><td>${'$'}{card.quantity}</td><td>${'$'}{card.id}</td>
                    </tr>
                `;
            }

            selectGiftcard(cardId) {
                if (!this.selectedBrand) return;
                const brandGiftcards = giftcards[this.selectedBrand.id] || [];
                const card = brandGiftcards.find(c => c.id === cardId);
                if (card) {
                    document.getElementById('selectedCardName').textContent = card.name;
                    document.getElementById('selectedCardDescription').innerHTML = card.description || 'No description available';
                    document.getElementById('giftcardDescriptions').style.display = 'block';
                    document.querySelectorAll('.giftcards-table tbody tr').forEach(row => row.classList.remove('selected'));
                    event.currentTarget.classList.add('selected');
                }
            }

            updateStatus(message) { this.statusBar.textContent = message; }
        }

        document.addEventListener('DOMContentLoaded', () => {
            window.piggyCardsViewer = new PiggyCardsViewer();
        });
    </script>
</body>
</html>"""
    }

    private fun cleanHtmlDescription(description: String): String {
        return description
            .replace("\\u0026", "&")
            .replace("\\n", "<br>")
            .replace("\\r", "")
            .replace("\\t", "&nbsp;&nbsp;&nbsp;&nbsp;")
            .replace("&#10;", "<br>")
            .replace("&#13;", "")
            .replace("&#160;", "&nbsp;")
            .replace("&#9;", "&nbsp;&nbsp;&nbsp;&nbsp;")
            .replace("&#8217;", "'")
            .replace("&#8220;", "\"")
            .replace("&#8221;", "\"")
            .replace("&#8239;", "&nbsp;")
            .replace("&#174;", "®")
            .replace("&#169;", "©")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&nbsp;", " ")
            .replace("&amp;", "&")
            .replace(Regex("<br>\\s*<br>\\s*<br>+"), "<br><br>")
            .replace(Regex("^<br>+"), "")
            .replace(Regex("<br>+$"), "")
            .trim()
    }

    private fun escapeJson(str: String): String {
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t")
    }
}