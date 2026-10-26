package org.dash.mobile.explore.sync

import com.google.gson.GsonBuilder
import com.google.gson.annotations.SerializedName
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.Response
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.Path
import retrofit2.http.Query
import java.io.FileNotFoundException
import java.util.Locale
import java.util.Properties
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

/**
 * Standalone tool that queries the PiggyCards API and prints the inventory
 * levels of all gift cards of all merchants, sorted by quantity (ascending).
 *
 * Run with: ./gradlew piggyInventory
 */

private const val PIGGY_BASE_URL = "https://api.piggy.cards/dash/v1/"

interface PiggyInventoryEndpoint {
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
        @SerializedName("price_type") val priceType: String,
        val currency: String,
        @SerializedName("discount_percentage") val discountPercentage: Double,
        @SerializedName("min_denomination") val minDenomination: Double,
        @SerializedName("max_denomination") val maxDenomination: Double,
        val denomination: String,
        val quantity: Int,
        @SerializedName("brand_id") val brandId: Int
    )

    data class LoginRequest(
        @SerializedName("userId") val userId: String,
        @SerializedName("password") val password: String
    )

    data class LoginResponse(
        @SerializedName("access_token") val accessToken: String,
        @SerializedName("token_type") val tokenType: String,
        @SerializedName("expires_in") val expiresIn: Int
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
}

private data class InventoryRow(
    val merchant: String,
    val denomination: String,
    val discount: Double,
    val quantity: Int
)

fun main() {
    val properties = Properties()
    val inputStream = PiggyInventoryEndpoint::class.java.classLoader.getResourceAsStream("service.properties")
        ?: throw FileNotFoundException("service.properties not found")
    inputStream.use { properties.load(it) }

    val userId = properties.getProperty("PIGGY_CARDS_USER_ID_PROD")
    val password = properties.getProperty("PIGGY_CARDS_PASSWORD_PROD")
    val country = properties.getProperty("country", "US")

    var token = ""
    val okHttpClient = OkHttpClient.Builder()
        .connectTimeout(20, TimeUnit.SECONDS)
        .writeTimeout(20, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .addInterceptor(object : Interceptor {
            override fun intercept(chain: Interceptor.Chain): Response {
                val requestBuilder = chain.request().newBuilder()
                if (token.isNotEmpty()) {
                    requestBuilder.addHeader("Authorization", "Bearer $token")
                }
                return chain.proceed(requestBuilder.build())
            }
        })
        .build()

    val apiService = Retrofit.Builder()
        .baseUrl(PIGGY_BASE_URL)
        .addConverterFactory(GsonConverterFactory.create(GsonBuilder().create()))
        .client(okHttpClient)
        .build()
        .create(PiggyInventoryEndpoint::class.java)

    val rows = arrayListOf<InventoryRow>()

    runBlocking(Dispatchers.IO) {
        println("Logging in to PiggyCards ($PIGGY_BASE_URL)...")
        token = apiService.login(PiggyInventoryEndpoint.LoginRequest(userId, password)).accessToken

        val brands = apiService.getBrands(country)
        println("Found ${brands.size} merchants, fetching gift cards...")

        brands.forEachIndexed { index, brand ->
            print("\r  [${index + 1}/${brands.size}] ${brand.name.padEnd(50).take(50)}")
            val response = try {
                apiService.getGiftCards(country, brand.id)
            } catch (e: Exception) {
                System.err.println("\nerror obtaining gift cards for ${brand.name}: ${e.message}")
                null
            }
            if (response != null && response.code == 200) {
                response.data.orEmpty().forEach { card ->
                    rows.add(InventoryRow(brand.name, formatDenomination(card), card.discountPercentage, card.quantity))
                }
            } else if (response != null) {
                System.err.println("\nPiggyCards API error for ${brand.name}: ${response.code} - ${response.message}")
            }
        }
        println()
    }

    if (rows.isEmpty()) {
        println("No gift cards found.")
        exitProcess(1)
    }

    rows.sortBy { it.quantity }
    printTable(rows)
    exitProcess(0)
}

private fun formatDenomination(card: PiggyInventoryEndpoint.Giftcard): String {
    return if (card.priceType == "Fixed" || card.priceType == "Option") {
        "${card.denomination} ${card.currency}"
    } else {
        "${formatAmount(card.minDenomination)} - ${formatAmount(card.maxDenomination)} ${card.currency}"
    }
}

private fun formatAmount(amount: Double): String {
    return if (amount == amount.toLong().toDouble()) {
        amount.toLong().toString()
    } else {
        String.format(Locale.US, "%.2f", amount)
    }
}

private fun formatDiscount(discount: Double): String {
    return String.format(Locale.US, "%.2f%%", discount)
}

private fun printTable(rows: List<InventoryRow>) {
    val headers = listOf("Merchant Name", "Denomination", "Discount", "Quantity")
    val merchantWidth = maxOf(headers[0].length, rows.maxOf { it.merchant.length })
    val denomWidth = maxOf(headers[1].length, rows.maxOf { it.denomination.length })
    val discountWidth = maxOf(headers[2].length, rows.maxOf { formatDiscount(it.discount).length })
    val quantityWidth = maxOf(headers[3].length, rows.maxOf { it.quantity.toString().length })

    val separator = "+" + "-".repeat(merchantWidth + 2) +
        "+" + "-".repeat(denomWidth + 2) +
        "+" + "-".repeat(discountWidth + 2) +
        "+" + "-".repeat(quantityWidth + 2) + "+"

    println(separator)
    println(
        "| " + headers[0].padEnd(merchantWidth) +
            " | " + headers[1].padEnd(denomWidth) +
            " | " + headers[2].padStart(discountWidth) +
            " | " + headers[3].padStart(quantityWidth) + " |"
    )
    println(separator)
    rows.forEach { row ->
        println(
            "| " + row.merchant.padEnd(merchantWidth) +
                " | " + row.denomination.padEnd(denomWidth) +
                " | " + formatDiscount(row.discount).padStart(discountWidth) +
                " | " + row.quantity.toString().padStart(quantityWidth) + " |"
        )
    }
    println(separator)
    println("Total: ${rows.size} gift cards, ${rows.map { it.merchant }.distinct().size} merchants")
}