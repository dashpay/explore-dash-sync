package org.dash.mobile.explore.sync.process

import com.google.gson.Gson
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
import java.io.Writer
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import kotlin.let

private const val BASE_URL = "https://spend.ctx.com/"
private const val STAGING_BASE_URL = "https://staging.spend.ctx.com/"

/**
 * Import data from CTXSpend API
 */
class CTXSpendDataSource(slackMessenger: SlackMessenger, private val operationMode: OperationMode, debugMode: Boolean) :
    DataSource<MerchantData>(slackMessenger, debugMode) {
    override val logger = LoggerFactory.getLogger(CTXSpendDataSource::class.java)!!
    val merchantList = hashSetOf<String>()
    var dataSourceReport: DataSourceReport? = null
    val baseUrl = if (operationMode == OperationMode.PRODUCTION) BASE_URL else STAGING_BASE_URL

    // Member fields for HTML generation
    private var allMerchants = linkedMapOf<String, JsonObject>()
    private var merchantLocations = linkedMapOf<String, MutableList<JsonObject>>() // merchantId -> locations

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
                logging.level = loggingLevel
                logging.redactHeader("Authorization")
                client.addInterceptor(logging)
            }
            .build()

        val retrofit: Retrofit = Retrofit.Builder()
            .baseUrl(baseUrl)
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

        logger.notice("Importing data from CTX Spend ($baseUrl)")

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
        logger.info("CTXSpend Disabled Merchants: (${
            disabledMerchants.map { it.value["name"] }.joinToString(", ") 
        })")
        allMerchants = merchants
        merchantLocations.clear()
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
                        merchantLocations.getOrPut(merchantId.asString) { mutableListOf() }.add(locationData)
                        val merchantData = convert(merchant, locationData)

                        if (merchantData.name.isNullOrEmpty() || merchantData.address1?.contains("Address 1") == true) {
                            invalid++
                            invalidLocations[locationData["merchantId"].asString] = locationData
                        } else {
                            emit(merchantData)
                        }
                    } else {
                        invalid++
                        val locationKey = locationData["id"]?.asString
                            ?: locationData["merchantId"]?.asString
                            ?: "unknown"
                        invalidLocations[locationKey] = locationData
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
        // Set.add returns true only the first time the id is seen, so each missing
        // merchant is warned about exactly once.
        if (missingMerchants.add(merchantId.asString)) {
            logger.warn("merchant id not found: {}: {}", merchantId.asString, merchants[merchantId.asString]?.get("name"))
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
            name = MerchantNameNormalizer.getNormalizedName(convertJsonData("name", merchantData))
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

        val merchantType = merchant["type"]?.asString
        val isPhysical = merchantType == "physical" || merchantType == "any"
        // CTX uses "any" for merchants redeemable both online and in-store; treat it like "online"
        // and let the location address decide whether it is physical
        val isOnline = merchantType == "online" || merchantType == "any"
        val locationAddress1 = location["address1"]?.asString
        return when {
            // isPhysical && isOnline -> "both"
            isPhysical -> "physical"
            isOnline && locationAddress1 != "online" -> "physical"
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

    override fun generateHtmlFile(): String? {
        if (allMerchants.isEmpty()) {
            logger.warn("No merchant data available for HTML generation")
            return null
        }

        val currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
        val filename = "ctx-${this.operationMode}-$currentDate.html"

        logger.info("Generating HTML file: $filename")

        return try {
            val file = File(filename)
            file.bufferedWriter().use { writer ->
                writeHtmlContent(writer, allMerchants, merchantLocations, currentDate)
            }
            logger.info("HTML file generated successfully: ${file.absolutePath}")
            filename
        } catch (ex: IOException) {
            logger.error("Failed to write HTML file: ${ex.message}", ex)
            null
        }
    }

    private fun writeMerchantEntry(writer: Writer, id: String, m: JsonObject, locationCount: Int) {
        val entry = JsonObject().apply {
            addProperty("id", id)
            addProperty("name", m["name"]?.asString ?: "")
            addProperty("enabled", m["enabled"]?.asString ?: "")
            addProperty("logoUrl", m["logoUrl"]?.asString ?: "")
            addProperty("website", m["website"]?.asString ?: "")
            addProperty("savingsPercentage", m["savingsPercentage"]?.let { if (!it.isJsonNull) it.asInt else null })
            addProperty("redeemType", m["redeemType"]?.asString ?: "")
            addProperty("denominationsType", m["denominationsType"]?.asString ?: "")
            addProperty("type", m["type"]?.asString ?: "")
            addProperty("locationCount", locationCount)
        }
        writer.write("        " + toJsScriptSafe(entry))
    }

    private fun writeHtmlContent(
        writer: Writer,
        merchants: Map<String, JsonObject>,
        locations: Map<String, List<JsonObject>>,
        currentDate: String
    ) {
        val totalLocations = locations.values.sumOf { it.size }

        writer.write("""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CTX Spend ${this.operationMode} - $currentDate</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f5f5f5; height: 100vh; overflow: hidden; }
        .container { display: flex; height: 100vh; }
        .sidebar { width: 300px; background-color: #fff; border-right: 1px solid #e0e0e0; overflow-y: auto; box-shadow: 2px 0 10px rgba(0,0,0,0.1); }
        .sidebar-header { padding: 20px; background-color: #1565C0; color: white; text-align: center; }
        .sidebar-header h1 { font-size: 1.2em; margin-bottom: 5px; }
        .brand-item { padding: 15px 20px; border-bottom: 1px solid #f0f0f0; cursor: pointer; transition: background-color 0.2s; display: flex; align-items: center; }
        .brand-item:hover { background-color: #f8f9fa; }
        .brand-item.active { background-color: #e3f2fd; border-left: 4px solid #1565C0; }
        .brand-logo { width: 40px; height: 40px; margin-right: 12px; border-radius: 4px; object-fit: cover; background-color: #f0f0f0; }
        .brand-name { font-weight: 500; color: #333; font-size: 0.9em; }
        .main-content { flex: 1; background-color: #fff; overflow-y: auto; padding: 20px; }
        .content-header { margin-bottom: 20px; padding-bottom: 20px; border-bottom: 2px solid #f0f0f0; }
        .brand-title { font-size: 1.8em; color: #333; display: flex; align-items: center; }
        .brand-title img { width: 50px; height: 50px; margin-right: 12px; border-radius: 8px; object-fit: cover; }
        .info-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 20px 0; }
        .attribute { display: flex; flex-direction: column; padding: 12px; background-color: #f8f9fa; border-radius: 6px; border-left: 4px solid #1565C0; }
        .attribute-label { font-size: 0.8em; color: #666; text-transform: uppercase; margin-bottom: 4px; }
        .attribute-value { font-weight: 500; color: #333; }
        .section-title { font-size: 1.2em; color: #555; margin: 20px 0 12px; padding-bottom: 8px; border-bottom: 1px solid #e0e0e0; }
        .table-container { overflow-x: auto; border: 1px solid #e0e0e0; border-radius: 8px; }
        .locations-table { width: 100%; border-collapse: collapse; font-size: 0.88em; }
        .locations-table th { background-color: #f8f9fa; color: #555; font-weight: 600; padding: 10px 8px; text-align: left; border-bottom: 2px solid #e0e0e0; white-space: nowrap; }
        .locations-table td { padding: 9px 8px; border-bottom: 1px solid #f0f0f0; color: #333; }
        .locations-table tbody tr:hover { background-color: #f8f9fa; }
        .locations-table tbody tr:last-child td { border-bottom: none; }
        .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.8em; font-weight: 600; }
        .badge-online { background-color: #e3f2fd; color: #1565C0; }
        .badge-physical { background-color: #e8f5e9; color: #2e7d32; }
        .empty-state { text-align: center; padding: 100px 20px; color: #999; }
        .empty-state h2 { margin-bottom: 10px; }
        .status-bar { background-color: #1565C0; color: white; padding: 10px 20px; text-align: center; font-size: 0.9em; }
    </style>
</head>
<body>
    <div class="status-bar" id="statusBar">Data loaded from CTX Spend API</div>
    <div class="container">
        <div class="sidebar">
            <div class="sidebar-header">
                <h1>CTX Spend</h1>
                <div>${this.operationMode} - $currentDate</div>
            </div>
            <div id="brandList"></div>
        </div>
        <div class="main-content" id="mainContent">
            <div class="empty-state">
                <h2>Select a Merchant</h2>
                <p>Choose a merchant from the left sidebar to view its locations.</p>
            </div>
        </div>
    </div>
    <script>
        const merchants = [
""")

        var firstMerchant = true
        for ((id, m) in merchants) {
            if (!firstMerchant) writer.write(",\n")
            firstMerchant = false
            writeMerchantEntry(writer, id, m, locations[id]?.size ?: 0)
        }

        writer.write("""
        ];

        class CTXViewer {
            constructor() {
                this.selectedMerchant = null;
                this.statusBar = document.getElementById('statusBar');
                this.brandList = document.getElementById('brandList');
                this.mainContent = document.getElementById('mainContent');
                this.init();
            }

            init() {
                this.renderMerchantList();
                this.updateStatus(`Loaded ${'$'}{merchants.length} merchants with $totalLocations total locations`);
            }

            renderMerchantList() {
                this.brandList.innerHTML = '';
                merchants.forEach(m => {
                    const el = document.createElement('div');
                    el.className = 'brand-item';
                    el.onclick = () => this.selectMerchant(m, el);
                    el.innerHTML = `
                        <img class="brand-logo" src="${'$'}{m.logoUrl}" alt="${'$'}{m.name}" onerror="this.style.display='none'">
                        <div class="brand-name">${'$'}{m.name} (${'$'}{m.locationCount})</div>
                    `;
                    this.brandList.appendChild(el);
                });
            }

            selectMerchant(m, element) {
                document.querySelectorAll('.brand-item').forEach(el => el.classList.remove('active'));
                element.classList.add('active');
                this.selectedMerchant = m;
                this.renderMerchantDetails(m);
            }

            renderMerchantDetails(m) {
                this.mainContent.innerHTML = `
                    <div class="content-header">
                        <div class="brand-title">
                            <img src="${'$'}{m.logoUrl}" alt="${'$'}{m.name}" onerror="this.style.display='none'">
                            ${'$'}{m.name}
                        </div>
                    </div>
                    <div class="info-grid">
                        <div class="attribute"><div class="attribute-label">Merchant ID</div><div class="attribute-value">${'$'}{m.id}</div></div>
                        <div class="attribute"><div class="attribute-label">Enabled</div><div class="attribute-value">${'$'}{m.enabled}</div></div>
                        <div class="attribute"><div class="attribute-label">Type</div><div class="attribute-value">${'$'}{m.type}</div></div>
                        <div class="attribute"><div class="attribute-label">Savings</div><div class="attribute-value">${'$'}{m.savingsPercentage != null ? m.savingsPercentage + ' bps' : 'N/A'}</div></div>
                        <div class="attribute"><div class="attribute-label">Redeem Type</div><div class="attribute-value">${'$'}{m.redeemType || 'N/A'}</div></div>
                        <div class="attribute"><div class="attribute-label">Denominations</div><div class="attribute-value">${'$'}{m.denominationsType || 'N/A'}</div></div>
                        <div class="attribute"><div class="attribute-label">Website</div><div class="attribute-value">${'$'}{m.website ? '<a href="' + m.website + '" target="_blank">' + m.website + '</a>' : 'N/A'}</div></div>
                        <div class="attribute"><div class="attribute-label">Locations</div><div class="attribute-value">${'$'}{m.locationCount}</div></div>
                    </div>
                `;
            }

            updateStatus(message) { this.statusBar.textContent = message; }
        }

        document.addEventListener('DOMContentLoaded', () => { new CTXViewer(); });
    </script>
</body>
</html>""")
    }

    /**
     * Serializes a JSON value with Gson (correctly escaping control characters) and makes it
     * safe to inline inside an HTML <script> block by neutralizing closing-tag sequences such
     * as </script>.
     */
    private fun toJsScriptSafe(value: JsonElement): String {
        return jsonGson.toJson(value).replace("</", "<\\/")
    }

    companion object {
        private val jsonGson = Gson()
    }
}
