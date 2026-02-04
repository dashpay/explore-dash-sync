package org.dash.mobile.explore.sync.process

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.dash.mobile.explore.sync.notice
import org.dash.mobile.explore.sync.process.data.AtmLocation
import org.dash.mobile.explore.sync.slack.SlackMessenger
import org.slf4j.LoggerFactory
import retrofit2.Response
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.Field
import retrofit2.http.FormUrlEncoded
import retrofit2.http.POST
import java.io.IOException
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/**
 * Import data from CoinAtmRadar API
 */
class CoinAtmRadarDataSource(slackMessenger: SlackMessenger, debugMode: Boolean) : DataSource<AtmLocation>(slackMessenger, debugMode) {
    companion object {
        private const val BASE_URL = "https://coinatmradar.com/ext_api/"
    }

    override val logger = LoggerFactory.getLogger(CoinAtmRadarDataSource::class.java)!!

    interface Endpoint {

        data class Result(val error: Any?, val locations: List<AtmLocation>?)

        @FormUrlEncoded
        @POST("dashwallet/")
        suspend fun getLocations(
            @Field("api_time") apiTime: Long,
            @Field("public_key") publicKey: String,
            @Field("auth_hash") authHash: String
        ): Response<Result>
    }

    private val retrofit = Retrofit.Builder()
        .baseUrl(BASE_URL)
        .addConverterFactory(GsonConverterFactory.create())
        .build()

    override fun getRawData(): Flow<AtmLocation> = flow {
        val properties = getProperties()
        val publicKey = properties.getProperty("COIN_ATM_RADAR_PUBLIC_KEY")
        val authKey = properties.getProperty("COIN_ATM_RADAR_AUTH_KEY")
        require(publicKey.isNotEmpty()) { "CoinATMRadar public key is empty. Check service.properties" }
        require(authKey.isNotEmpty()) { "CoinATMRadar auth key is empty. Check service.properties" }

        logger.notice("Importing data from CoinATMRadar")
        val apiService = retrofit.create(Endpoint::class.java)

        try {
            val currentTime = System.currentTimeMillis() / 1000
            val authHash = sha512("$publicKey~$currentTime~$authKey")
            val response = apiService.getLocations(
                apiTime = currentTime,
                publicKey = publicKey,
                authHash = authHash
            )

            if (response.isSuccessful) {
                val responseData = response.body()

                if (responseData?.error == null) {
                    var totalRecords = 0
                    responseData?.locations?.forEach { location ->
                        totalRecords++
                        location.state?.let {
                            location.state = fixStateName(it)
                        }
                        emit(location)
                    }

                    slackMessenger.postSlackMessage("CoinATMRadar $totalRecords records", logger)
                } else {
                    logger.error("error: ${responseData.error}")
                }
            } else {
                logger.error("error: ${response.errorBody()}")
            }
        } catch (ex: IOException) {
            logger.error(ex.message, ex)
        }
    }
}

fun sha512(input: String): String {
    val digest = MessageDigest.getInstance("SHA-512")
    val encodedHash = digest.digest(input.toByteArray(StandardCharsets.UTF_8))
    return bytesToHex(encodedHash)
}

fun bytesToHex(bytes: ByteArray): String {
    val bigInteger = BigInteger(1, bytes)
    val hexString = bigInteger.toString(16)
    return hexString.padStart(2 * bytes.size, '0')
}
