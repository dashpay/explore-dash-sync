package org.dash.mobile.explore.sync.process

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.SheetsScopes
import com.google.api.services.sheets.v4.model.CellData
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.dash.mobile.explore.sync.notice
import org.dash.mobile.explore.sync.process.data.MerchantData
import org.dash.mobile.explore.sync.slack.SlackMessenger
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.util.*

private const val APPLICATION_NAME = "Explore Dash Sync"
const val CREDENTIALS_FILE_PATH = "credentials.json"
private const val SPREADSHEET_ID = "1YU5UShf5ruTZKJxglP36h-87W02bsDY3L5MmpYjFCGA"

/**
 * Import data from Google Sheet: https://docs.google.com/spreadsheets/d/1YU5UShf5ruTZKJxglP36h-87W02bsDY3L5MmpYjFCGA
 */
class DCGDataSource(private val useTestnetSheet: Boolean, slackMessenger: SlackMessenger, debugMode: Boolean) :
    DataSource<MerchantData>(slackMessenger, debugMode) {

    override val logger = LoggerFactory.getLogger(DCGDataSource::class.java)!!

    private val jsonFactory = GsonFactory.getDefaultInstance()

    override fun getRawData(): Flow<MerchantData> = flow {
        logger.notice(
            "Importing data from Google Sheet " +
                    "https://docs.google.com/spreadsheets/d/1YU5UShf5ruTZKJxglP36h-87W02bsDY3L5MmpYjFCGA"
        )

        // Load Service user credentials
        val credentials = try {
            // Try to use Application Default Credentials (works in Cloud Functions)
            GoogleCredentials.getApplicationDefault()
                .createScoped(SheetsScopes.SPREADSHEETS_READONLY)
        } catch (ex: Exception) {
            // Fallback to credentials.json for local development
            logger.info("Using credentials.json for local development")
            val resourceStream = javaClass.classLoader.getResourceAsStream(CREDENTIALS_FILE_PATH)
                ?: throw FileNotFoundException(
                    "Google API credentials ($CREDENTIALS_FILE_PATH) not found." +
                            "You can download it from https://console.cloud.google.com/apis/credentials"
                )
            GoogleCredentials.fromStream(resourceStream)
                .createScoped(SheetsScopes.SPREADSHEETS_READONLY)
        }
        credentials.refreshIfExpired()
        // Build a new authorized API client service.
        val httpTransport = GoogleNetHttpTransport.newTrustedTransport()
        val service = Sheets.Builder(httpTransport, jsonFactory, HttpCredentialsAdapter(credentials))
            .setApplicationName(APPLICATION_NAME)
            .build()

        logger.info("Fetching spreadsheet metadata...")
        val spreadsheet = try {
            service.spreadsheets()[SPREADSHEET_ID].execute()
        } catch (ex: Exception) {
            logger.error("Failed to fetch spreadsheet metadata: ${ex.javaClass.simpleName}: ${ex.message}", ex)
            throw ex
        }
        logger.info("Spreadsheet metadata fetched successfully")

        val sheetIndex = if (useTestnetSheet) 1 else 0
        logger.info("Number of sheets: ${spreadsheet.sheets.size}")
        val sheetName = spreadsheet.sheets[sheetIndex].properties.title
        logger.info("Using sheet: $sheetName")

        // Fetch values using the more efficient Values API
        logger.info("Fetching cell values...")
        val response = try {
            service.spreadsheets().values()
                .get(SPREADSHEET_ID, sheetName)
                .setValueRenderOption("FORMATTED_VALUE")
                .execute()
        } catch (ex: Exception) {
            logger.error("Failed to fetch cell values: ${ex.javaClass.simpleName}: ${ex.message}", ex)
            throw ex
        }
        logger.info("Cell values fetched successfully")

        val values = response.getValues()
        if (values == null || values.isEmpty()) {
            logger.warn("No data found in sheet")
            slackMessenger.postSlackMessage("DCG Merchants: 0 records (sheet is empty)", logger)
            return@flow
        }

        logger.info("Number of rows: ${values.size}")
        val headers = mutableListOf<String>()

        var totalRecords = 0
        val merchantNames = hashSetOf<String>()
        for (rowIndex in values.indices) {
            val rowData = values[rowIndex]
            if (rowIndex == 0) {
                // Header row
                for (cell in rowData) {
                    if (cell != null && cell.toString().isNotEmpty()) {
                        headers.add(cell.toString())
                    } else {
                        break
                    }
                }
                logger.info("Number of columns: ${headers.size}")
                continue
            } else {
                logger.info("Processing row $rowIndex")
                val merchant = convertFromValues(rowData)
                if (merchant != null) {
                    totalRecords++
                    merchant.name?.let {
                        merchantNames.add(it)
                    }
                    emit(merchant)
                } else {
                    logger.info("Skipping empty row at index $rowIndex")
                    continue
                }
            }
        }
        slackMessenger.postSlackMessage("DCG Merchants $totalRecords location records (merchants: ${merchantNames.size})", logger)
    }

    private fun convertFromValues(rowData: List<Any>): MerchantData? {
        // Check if row is empty
        var emptyRow = true
        for (cell in rowData) {
            if (cell.toString().isNotEmpty()) {
                emptyRow = false
                break
            }
        }
        if (emptyRow) {
            return null
        }

        return MerchantData().apply {
            plusCode = getValueAsString(rowData, ColHeader.PLUS_CODE)
            paymentMethod = getValueAsString(rowData, ColHeader.PAYMENT_METHOD)?.trim()
            val merchantIdValue = getValueAsString(rowData, ColHeader.MERCHANT_ID)
            merchantId = merchantIdValue?.toDoubleOrNull()?.toLong()?.toString() ?: merchantIdValue
            active = getValueAsBoolean(rowData, ColHeader.ACTIVE)
            name = getValueAsString(rowData, ColHeader.NAME)
            address1 = getValueAsString(rowData, ColHeader.ADDRESS1)
            address2 = getValueAsString(rowData, ColHeader.ADDRESS2)
            address3 = getValueAsString(rowData, ColHeader.ADDRESS3)
            address4 = getValueAsString(rowData, ColHeader.ADDRESS4)
            latitude = getValueAsDouble(rowData, ColHeader.LATITUDE)
            longitude = getValueAsDouble(rowData, ColHeader.LONGITUDE)
            website = getValueAsString(rowData, ColHeader.WEBSITE)
            phone = getValueAsString(rowData, ColHeader.PHONE)
            getValueAsString(rowData, ColHeader.TERRITORY)?.apply {
                territory = this.trim()
            }
            source = "DCG"
            val sourceIdValue = getValueAsString(rowData, ColHeader.SOURCE_ID)
            sourceId = sourceIdValue?.toDoubleOrNull()?.toInt()?.toString() ?: sourceIdValue ?: "0"
            logoLocation = getValueAsString(rowData, ColHeader.LOGO_LOCATION)
            googleMaps = getValueAsString(rowData, ColHeader.GOOGLE_MAPS)
            coverImage = null
            type = getValueAsString(rowData, ColHeader.TYPE)?.trim()
            redeemType = "none"
            instagram = getValueAsString(rowData, ColHeader.INSTAGRAM)
            twitter = getValueAsString(rowData, ColHeader.TWITTER)
            delivery = getValueAsString(rowData, ColHeader.DELIVERY)

            monOpen = getValueAsString(rowData, ColHeader.MON_OPEN)
            monClose = getValueAsString(rowData, ColHeader.MON_CLOSE)
            tueOpen = getValueAsString(rowData, ColHeader.TUE_OPEN)
            tueClose = getValueAsString(rowData, ColHeader.TUE_CLOSE)
            wedOpen = getValueAsString(rowData, ColHeader.WED_OPEN)
            wedClose = getValueAsString(rowData, ColHeader.WED_CLOSE)
            thuOpen = getValueAsString(rowData, ColHeader.THU_OPEN)
            thuClose = getValueAsString(rowData, ColHeader.THU_CLOSE)
            friOpen = getValueAsString(rowData, ColHeader.FRI_OPEN)
            friClose = getValueAsString(rowData, ColHeader.FRI_CLOSE)
            satOpen = getValueAsString(rowData, ColHeader.SAT_OPEN)
            satClose = getValueAsString(rowData, ColHeader.SAT_CLOSE)
            sunOpen = getValueAsString(rowData, ColHeader.SUN_OPEN)
            sunClose = getValueAsString(rowData, ColHeader.SUN_CLOSE)
        }
    }

    private fun getValueAsString(rowData: List<Any>, colHeader: ColHeader): String? {
        val colIndex = colHeader.ordinal
        if (colIndex >= rowData.size) return null
        val value = rowData[colIndex].toString()
        return if (value.isEmpty()) null else value
    }

    private fun getValueAsDouble(rowData: List<Any>, colHeader: ColHeader): Double? {
        val str = getValueAsString(rowData, colHeader) ?: return null
        return str.toDoubleOrNull()
    }

    private fun getValueAsBoolean(rowData: List<Any>, colHeader: ColHeader): Boolean? {
        val str = getValueAsString(rowData, colHeader) ?: return null
        return when (str.uppercase()) {
            "TRUE", "YES", "1" -> true
            "FALSE", "NO", "0" -> false
            else -> null
        }
    }

    private fun convert(rowData: List<CellData>): MerchantData? {
        var emptyRow = true
        for (cell in rowData) {
            if (!cell.formattedValue.isNullOrEmpty()) {
                emptyRow = false
                break
            }
        }
        if (emptyRow) {
            return null
        }

        return MerchantData().apply {
//            deeplink = null
            plusCode = convert(rowData, ColHeader.PLUS_CODE)
//            addDate = null
//            updateDate = null
            paymentMethod = convert<String?>(rowData, ColHeader.PAYMENT_METHOD)?.trim()
            val merchantIdAsLong = convert<Int?>(rowData, ColHeader.MERCHANT_ID)?.toLong()
            merchantId = merchantIdAsLong.toString()
//            id = null
            active = convert(rowData, ColHeader.ACTIVE)
            name = convert(rowData, ColHeader.NAME)
            address1 = convert(rowData, ColHeader.ADDRESS1)
            address2 = convert(rowData, ColHeader.ADDRESS2)
            address3 = convert(rowData, ColHeader.ADDRESS3)
            address4 = convert(rowData, ColHeader.ADDRESS4)
            latitude = convert(rowData, ColHeader.LATITUDE)
            longitude = convert(rowData, ColHeader.LONGITUDE)
            website = convert(rowData, ColHeader.WEBSITE)
            phone = convert(rowData, ColHeader.PHONE)
            convert<String?>(rowData, ColHeader.TERRITORY)?.apply {
                territory = this.trim()
            }
//            city = null
            source = "DCG"
            val sourceIdAsInt:Int = convert(rowData, ColHeader.SOURCE_ID) ?: 0
            sourceId = sourceIdAsInt.toString()
            logoLocation = convert(rowData, ColHeader.LOGO_LOCATION)
            googleMaps = convert(rowData, ColHeader.GOOGLE_MAPS)
            coverImage = null
            type = convert<String?>(rowData, ColHeader.TYPE)?.trim()
            redeemType = "none"
            instagram = convert(rowData, ColHeader.INSTAGRAM)
            twitter = convert(rowData, ColHeader.TWITTER)
            delivery = convert(rowData, ColHeader.DELIVERY)

            monOpen = convert(rowData, ColHeader.MON_OPEN)
            monClose = convert(rowData, ColHeader.MON_CLOSE)
            tueOpen = convert(rowData, ColHeader.TUE_OPEN)
            tueClose = convert(rowData, ColHeader.TUE_CLOSE)
            wedOpen = convert(rowData, ColHeader.WED_OPEN)
            wedClose = convert(rowData, ColHeader.WED_CLOSE)
            thuOpen = convert(rowData, ColHeader.THU_OPEN)
            thuClose = convert(rowData, ColHeader.THU_CLOSE)
            friOpen = convert(rowData, ColHeader.FRI_OPEN)
            friClose = convert(rowData, ColHeader.FRI_CLOSE)
            satOpen = convert(rowData, ColHeader.SAT_OPEN)
            satClose = convert(rowData, ColHeader.SAT_CLOSE)
            sunOpen = convert(rowData, ColHeader.SUN_OPEN)
            sunClose = convert(rowData, ColHeader.SUN_CLOSE)

            // min and max purchase amounts are not specified in the DCG source data
        }
    }

    private inline fun <reified T> convert(rowData: List<CellData>, colHeader: ColHeader): T? {
        val colIndex = colHeader.ordinal
        val cellData = rowData[colIndex]
        return when {
            cellData.formattedValue == null -> null
            cellData.userEnteredValue?.boolValue != null -> cellData.userEnteredValue.boolValue as T
            cellData.userEnteredValue?.numberValue != null -> {
                val type = cellData.effectiveFormat?.numberFormat?.type
                when {
                    type.equals("NUMBER") -> {
                        if (T::class == Int::class) {
                            cellData.userEnteredValue.numberValue.toInt() as T
                        } else {
                            cellData.userEnteredValue.numberValue as T
                        }
                    }
                    type.equals("TIME") -> {
                        cellData.formattedValue.lowercase(Locale.getDefault()) as T
                    }
                    else -> {
                        cellData.formattedValue as T
                    }
                }
            }
            else -> cellData.formattedValue as T
        }
    }

    enum class ColHeader {
        SOURCE_ID,
        ADD_DATE,
        UPDATE_DATE,
        NAME,
        ADDRESS1,
        ADDRESS2,
        ADDRESS3,
        ADDRESS4,
        LATITUDE,
        LONGITUDE,
        PLUS_CODE,
        TERRITORY,
        GOOGLE_MAPS,
        LOGO_LOCATION,
        WEBSITE,
        PAYMENT_METHOD,
        TYPE,
        PHONE,
        MON_OPEN,
        MON_CLOSE,
        TUE_OPEN,
        TUE_CLOSE,
        WED_OPEN,
        WED_CLOSE,
        THU_OPEN,
        THU_CLOSE,
        FRI_OPEN,
        FRI_CLOSE,
        SAT_OPEN,
        SAT_CLOSE,
        SUN_OPEN,
        SUN_CLOSE,
        INSTAGRAM,
        TWITTER,
        DELIVERY,
        ACTIVE,
        MERCHANT_ID,
        SAVINGS_PERCENTAGE,
    }
}
