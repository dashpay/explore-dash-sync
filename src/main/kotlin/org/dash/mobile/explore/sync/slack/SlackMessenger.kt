package org.dash.mobile.explore.sync.slack

import com.google.gson.GsonBuilder
import okhttp3.OkHttpClient
import org.dash.mobile.explore.sync.notice
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.Body
import retrofit2.http.POST
import retrofit2.http.Path
import java.util.concurrent.TimeUnit

private const val SLACK_ENDPOINT = "https://hooks.slack.com"// T03CM2KCH/B0367DR17JB/AcO0fLbx3q9S4moIrb7joGy3
private const val SLACK_CHANNEL_URL_KEY = "AcO0fLbx3q9S4moIrb7joGy3"

class SlackMessenger {

    private val logger = LoggerFactory.getLogger(SlackMessenger::class.java)!!

    interface Endpoint {

        @POST("/services/T03CM2KCH/B0367DR17JB/{slack_channel_url_key}")
        suspend fun postSlackMessage(
            @Path(value = "slack_channel_url_key") urlKey: String,
            @Body slackMessage: SlackMessage
        )
    }

    private val apiService: Endpoint

    init {
        val gson = GsonBuilder()
            .create()

        val okHttpClient = OkHttpClient.Builder()
            .connectTimeout(20, TimeUnit.SECONDS)
            .writeTimeout(20, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .build()

        val retrofit: Retrofit = Retrofit.Builder()
            .baseUrl(SLACK_ENDPOINT)
            .addConverterFactory(GsonConverterFactory.create(gson))
            .client(okHttpClient)
            .build()

        apiService = retrofit.create(Endpoint::class.java)
    }

    suspend fun postSlackMessage(message: String, logger: Logger? = null) {
        logger?.notice(message)
        val emoji = ":success:"
        val slackMessage = SlackMessage(message, emoji, false)
        apiService.postSlackMessage(SLACK_CHANNEL_URL_KEY, slackMessage)
    }
}