package org.dash.mobile.explore.sync.process

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.SheetsScopes
import com.google.api.services.sheets.v4.model.CellData
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import org.dash.mobile.explore.sync.notice
import org.dash.wallet.features.exploredash.data.model.Protos
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.io.IOException

private const val APPLICATION_NAME = "Explore Dash Sync"
const val CREDENTIALS_FILE_PATH = "dash-wallet-firebase-3dcb5c05f13e.json"
private const val SPREADSHEET_ID = "1YU5UShf5ruTZKJxglP36h-87W02bsDY3L5MmpYjFCGA"

/**
 * Import data from Google Sheet: https://docs.google.com/spreadsheets/d/1YU5UShf5ruTZKJxglP36h-87W02bsDY3L5MmpYjFCGA
 */
class SpreadsheetImporter : Importer() {

    override val logger = LoggerFactory.getLogger(SpreadsheetImporter::class.java)!!

    override val propertyName = "dcg_merchant"

    private val jsonFactory = GsonFactory.getDefaultInstance()

    @Throws(IOException::class)
    override fun import(): List<Protos.MerchantData> {

        logger.notice("Importing data from Google Sheet https://docs.google.com/spreadsheets/d/1YU5UShf5ruTZKJxglP36h-87W02bsDY3L5MmpYjFCGA")

        // Load Service user credentials
        val resourceStream = javaClass.classLoader.getResourceAsStream(CREDENTIALS_FILE_PATH)
            ?: throw FileNotFoundException(
                "Google API credentials ($CREDENTIALS_FILE_PATH) not found." +
                        "You can download it from https://console.cloud.google.com/apis/credentials"
            )
        val credentials = GoogleCredentials.fromStream(resourceStream)
            .createScoped(SheetsScopes.SPREADSHEETS_READONLY)
        credentials.refreshIfExpired()

        // Build a new authorized API client service.
        val httpTransport = GoogleNetHttpTransport.newTrustedTransport()
        val service = Sheets.Builder(httpTransport, jsonFactory, HttpCredentialsAdapter(credentials))
            .setApplicationName(APPLICATION_NAME)
            .build()

        val spreadsheet = service.spreadsheets()[SPREADSHEET_ID]
            .setIncludeGridData(true)
            .execute()

        val sheetIndex = 0
        val sheet = spreadsheet.sheets[sheetIndex]
        val gridData = sheet.data[0]

        val headers = mutableListOf<String>()
        val result = mutableListOf<Protos.MerchantData>()

        for (rowIndex in gridData.rowData.indices) {
            val rowData = gridData.rowData[rowIndex].getValues()
            if (rowIndex == 0) {
                for (cellData in rowData) {
                    cellData.formattedValue?.also {
                        headers.add(it)
                    } ?: break
                }
                logger.info("num of cols ${headers.size}")
                continue
            } else {
                val merchant = convert(rowData)
                if (merchant != null) {
                    result.add(merchant)
                } else {
                    logger.info("num of rows ${rowIndex - 1}")
                    break
                }
            }
        }
        logger.notice("Google Sheet - imported ${result.size} records")
        return result
    }

    private fun convert(rowData: List<CellData>): Protos.MerchantData? {
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

        return Protos.MerchantData.newBuilder().apply {
            source = "DCG"
            convert<Int?>(rowData, ColHeader.SOURCE_ID)?.apply { sourceId = this }
            convert<String?>(rowData, ColHeader.NAME)?.apply { name = this }
            convert<String?>(rowData, ColHeader.ADDRESS1)?.apply { address1 = this }
            convert<String?>(rowData, ColHeader.ADDRESS2)?.apply { address2 = this }
            convert<String?>(rowData, ColHeader.ADDRESS3)?.apply { address3 = this }
            convert<String?>(rowData, ColHeader.ADDRESS4)?.apply { address4 = this }
            convert<Double?>(rowData, ColHeader.LATITUDE)?.apply { latitude = this }
            convert<Double?>(rowData, ColHeader.LONGITUDE)?.apply { longitude = this }
            convert<String?>(rowData, ColHeader.PLUS_CODE)?.apply { plusCode = this }
            convert<String?>(rowData, ColHeader.TERRITORY)?.apply { territory = this }
            convert<String?>(rowData, ColHeader.GOOGLE_MAPS)?.apply { googleMaps = this }
            convert<String?>(rowData, ColHeader.LOGO_LOCATION)?.apply { logoLocation = this }
            convert<String?>(rowData, ColHeader.WEBSITE)?.apply { website = this }
            convert<String?>(rowData, ColHeader.PAYMENT_METHOD)?.apply { paymentMethod = this }
            convert<String?>(rowData, ColHeader.TYPE)?.apply { type = this }
            convert<String?>(rowData, ColHeader.PHONE)?.apply { phone = this }
            opening = Protos.OpeningHoursData.newBuilder().apply {
                convert<String?>(rowData, ColHeader.MON_OPEN)?.apply { monOpen = this }
                convert<String?>(rowData, ColHeader.MON_CLOSE)?.apply { monClose = this }
                convert<String?>(rowData, ColHeader.TUE_OPEN)?.apply { tueOpen = this }
                convert<String?>(rowData, ColHeader.TUE_CLOSE)?.apply { tueClose = this }
                convert<String?>(rowData, ColHeader.WED_OPEN)?.apply { wedOpen = this }
                convert<String?>(rowData, ColHeader.WED_CLOSE)?.apply { wedClose = this }
                convert<String?>(rowData, ColHeader.THU_OPEN)?.apply { thuOpen = this }
                convert<String?>(rowData, ColHeader.THU_CLOSE)?.apply { thuClose = this }
                convert<String?>(rowData, ColHeader.FRI_OPEN)?.apply { friOpen = this }
                convert<String?>(rowData, ColHeader.FRI_CLOSE)?.apply { friClose = this }
                convert<String?>(rowData, ColHeader.SAT_OPEN)?.apply { satOpen = this }
                convert<String?>(rowData, ColHeader.SAT_CLOSE)?.apply { satClose = this }
                convert<String?>(rowData, ColHeader.SUN_OPEN)?.apply { sunOpen = this }
                convert<String?>(rowData, ColHeader.SUN_CLOSE)?.apply { sunClose = this }
            }.build()
            convert<String?>(rowData, ColHeader.INSTAGRAM)?.apply { instagram = this }
            convert<String?>(rowData, ColHeader.TWITTER)?.apply { twitter = this }
            convert<String?>(rowData, ColHeader.DELIVERY)?.apply { delivery = this }
            convert<Boolean?>(rowData, ColHeader.ACTIVE)?.apply { active = this }
            convert<Int>(rowData, ColHeader.MERCHANT_ID)?.apply { merchantId = this.toLong() }
        }.build()
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
                        cellData.formattedValue.toLowerCase() as T
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
        MERCHANT_ID
    }
}
