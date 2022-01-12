package org.dash.mobile.explore.sync.process

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.SheetsScopes
import com.google.api.services.sheets.v4.model.CellData
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import mu.KotlinLogging
import java.io.FileNotFoundException
import java.io.IOException

private const val APPLICATION_NAME = "Explore Dash Sync"
private const val CREDENTIALS_FILE_PATH = "credentials.json"
private const val SPREADSHEET_ID = "1YU5UShf5ruTZKJxglP36h-87W02bsDY3L5MmpYjFCGA"

/**
 * Import data from Google Sheet: https://docs.google.com/spreadsheets/d/1YU5UShf5ruTZKJxglP36h-87W02bsDY3L5MmpYjFCGA
 */
class SpreadsheetImporter : Importer() {

    override val propertyName = "dcg_merchant"

    override val logger = KotlinLogging.logger {}

    private val jsonFactory = GsonFactory.getDefaultInstance()

    @Throws(IOException::class)
    override fun import(save: Boolean): JsonArray {

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

        logger.info("Importing data from Google Sheet https://docs.google.com/spreadsheets/d/1YU5UShf5ruTZKJxglP36h-87W02bsDY3L5MmpYjFCGA")

        val titles = mutableListOf<String>()
        val result = JsonArray()
        for (rowIndex in gridData.rowData.indices) {
            val rowData = gridData.rowData[rowIndex].getValues()
            val jsonObject = JsonObject()
            var emptyRow: Boolean
            if (rowIndex == 0) {
                for (cellData in rowData) {
                    cellData.formattedValue?.also {
                        titles.add(it.toLowerCase())
                    } ?: break
                }
                logger.info("num of cols ${titles.size}")
                continue
            } else {
                emptyRow = true
                for (colIndex in titles.indices) {
                    val title = titles[colIndex]
                    if (colIndex >= rowData.size) {
                        jsonObject.add(title, null)
                        break
                    }
                    val cellData = rowData[colIndex]
                    val jsonElement = convertToJsonElement(cellData)

                    if (jsonElement != null) {
                        emptyRow = false
                    }

                    jsonObject.add(title, jsonElement)
                }
                jsonObject.add("source", JsonPrimitive("DCG"))
            }
            if (emptyRow) {
                logger.info("num of rows ${rowIndex - 1}")
                break
            }
            result.add(jsonObject)
        }

        logger.info("Google Sheet - imported ${result.size()} records")

        return result
    }

    private fun convertToJsonElement(cellData: CellData): JsonElement? {
        return when {
            cellData.formattedValue == null -> null
            cellData.userEnteredValue?.boolValue != null -> JsonPrimitive(cellData.userEnteredValue.boolValue)
            cellData.userEnteredValue?.numberValue != null -> {
                val type = cellData.effectiveFormat?.numberFormat?.type
                when {
                    type.equals("NUMBER") -> {
                        JsonPrimitive(cellData.userEnteredValue.numberValue)
                    }
                    type.equals("TIME") -> {
                        JsonPrimitive(cellData.formattedValue.toLowerCase())
                    }
                    else -> {
                        JsonPrimitive(cellData.formattedValue)
                    }
                }
            }
            else -> JsonPrimitive(cellData.formattedValue)
        }
    }
}