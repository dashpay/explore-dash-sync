package org.dash.mobile.explore.sync.process

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.client.util.store.FileDataStoreFactory
import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.SheetsScopes
import com.google.api.services.sheets.v4.model.CellData
import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.io.InputStreamReader

private const val APPLICATION_NAME = "Explore Dash Sync"
private const val TOKENS_DIRECTORY_PATH = "tokens"
private const val CREDENTIALS_FILE_PATH = "credentials.json"
private const val SPREADSHEET_ID = "1YU5UShf5ruTZKJxglP36h-87W02bsDY3L5MmpYjFCGA"

/**
 * Import data from Google Sheet: https://docs.google.com/spreadsheets/d/1YU5UShf5ruTZKJxglP36h-87W02bsDY3L5MmpYjFCGA
 */
class SpreadsheetImporter() : Importer {

    override val propertyName = "merchant"

    private val logger = LoggerFactory.getLogger(SpreadsheetImporter::class.java)

    private val jsonFactory = GsonFactory.getDefaultInstance()

    override suspend fun import(save: Boolean): JsonArray {
        // Build a new authorized API client service.
        val httpTransport = GoogleNetHttpTransport.newTrustedTransport()
        val service = Sheets.Builder(httpTransport, jsonFactory, getCredentials(httpTransport))
            .setApplicationName(APPLICATION_NAME)
            .build()

        val spreadsheet = service.spreadsheets()[SPREADSHEET_ID]
            .setIncludeGridData(true)
            .execute()

        val sheetIndex = 0
        val sheet = spreadsheet.sheets[sheetIndex]
        val gridData = sheet.data[0]

        val titles = mutableListOf<String>()
        val jsonArray = JsonArray()
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
            }
            if (emptyRow) {
                logger.info("num of rows ${rowIndex - 1}")
                break
            }
            jsonArray.add(jsonObject)
        }
        return jsonArray
    }

    private fun convertToJsonElement(cellData: CellData): JsonElement? {
        return when {
            cellData.formattedValue == null -> null
            cellData.userEnteredValue?.boolValue != null -> JsonPrimitive(cellData.userEnteredValue.boolValue)
            cellData.userEnteredValue?.numberValue != null -> {
                val type = cellData.effectiveFormat?.numberFormat?.type
                if (type.equals("NUMBER")) {
                    JsonPrimitive(cellData.userEnteredValue.numberValue)
                } else if (type.equals("TIME")) {
                    JsonPrimitive(cellData.formattedValue.toLowerCase())
                } else {
                    JsonPrimitive(cellData.formattedValue)
                }
            }
            else -> JsonPrimitive(cellData.formattedValue)
        }
    }

    /**
     * Creates an authorized Credential object.
     *
     * @param HTTP_TRANSPORT The network HTTP Transport.
     * @return An authorized Credential object.
     * @throws IOException If the credentials.json file cannot be found.
     */
    @Throws(IOException::class)
    private fun getCredentials(HTTP_TRANSPORT: NetHttpTransport): Credential {
        // Load client secrets.
        val resourceStream = javaClass.classLoader.getResourceAsStream(CREDENTIALS_FILE_PATH)
            ?: throw FileNotFoundException(
                "Google API credentials ($CREDENTIALS_FILE_PATH) not found." +
                        "You can download it from https://console.cloud.google.com/apis/credentials"
            )

        val clientSecrets = GoogleClientSecrets.load(jsonFactory, InputStreamReader(resourceStream))

        val scopes = listOf(SheetsScopes.SPREADSHEETS_READONLY)

        // Build flow and trigger user authorization request.
        val flow = GoogleAuthorizationCodeFlow.Builder(
            HTTP_TRANSPORT, jsonFactory, clientSecrets, scopes
        )
            .setDataStoreFactory(FileDataStoreFactory(File(TOKENS_DIRECTORY_PATH)))
            .setAccessType("offline")
            .build()
        val receiver = LocalServerReceiver.Builder().setPort(8888).build()
        return AuthorizationCodeInstalledApp(flow, receiver).authorize("user")
    }
}