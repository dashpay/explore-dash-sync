package org.dash.mobile.explore.sync.process

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.annotations.SerializedName
import org.dash.mobile.explore.sync.Mapping
import org.slf4j.LoggerFactory
import retrofit2.Call
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.POST
import java.io.IOException

private const val BASE_URL = "https://api.dashdirect.org/"

/**
 * Import data from DashDirect API
 */
class DashDirectImporter : Importer {

    override val propertyName = "location"

    private val logger = LoggerFactory.getLogger(DashDirectImporter::class.java)

    interface Endpoint {

        data class Response(
            @SerializedName("Successful") val successful: Boolean,
            @SerializedName("ErrorMessage") val errorMessage: String,
            @SerializedName("Data") val data: JsonArray?
        )

        @POST("Merchant/GetAllMerchants")
        fun getAllMerchants(): Call<Response>
    }

    override suspend fun import(save: Boolean): JsonArray {

        val gson = GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
            .create()

        val retrofit: Retrofit = Retrofit.Builder()
            .baseUrl(BASE_URL)
            .addConverterFactory(GsonConverterFactory.create(gson))
            .build()

        val apiService = retrofit.create(Endpoint::class.java)
        val merchants = apiService.getAllMerchants()

        try {
            val result = JsonArray()
            val response = merchants.execute()
            if (response.isSuccessful) {
                val responseData = response.body()

                responseData?.data?.forEach { merchant ->
                    val outData = JsonObject()
                    Mapping.dashDirect.forEach asd@{ (keyOut, keyIn) ->
                        if (keyIn.isNotEmpty()) {
                            val value = merchant.asJsonObject.get(keyIn)
                            if (value.isJsonPrimitive) {
                                outData.add(keyOut, value.asJsonPrimitive)
                                return@asd
                            }
                            outData.add(keyOut, JsonNull.INSTANCE)
                        }
                    }
                    result.add(outData)
                }
                return result
            } else {
                logger.error("error: ${response.errorBody()}")
            }
        } catch (ex: IOException) {
            logger.error(ex.message, ex)
        }

        return JsonArray()
    }
}