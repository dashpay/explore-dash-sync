package org.dash.mobile.explore.sync

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.transform
import kotlinx.coroutines.flow.asFlow
import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.model.enums.CompressionLevel
import net.lingala.zip4j.model.enums.EncryptionMethod
import org.dash.mobile.explore.sync.process.CoinAtmRadarDataSource
import org.dash.mobile.explore.sync.process.DCGDataSource
import org.dash.mobile.explore.sync.process.CTXSpendDataSource
import org.dash.mobile.explore.sync.process.MerchantNameNormalizer
import org.dash.mobile.explore.sync.process.PiggyCardsDataSource
import org.dash.mobile.explore.sync.process.data.AtmLocation
import org.dash.mobile.explore.sync.process.data.Crc32c
import org.dash.mobile.explore.sync.process.data.Data
import org.dash.mobile.explore.sync.process.data.MerchantData
import org.dash.mobile.explore.sync.process.data.GiftCardProvider
import org.dash.mobile.explore.sync.slack.SlackMessenger
import org.dash.mobile.explore.sync.utils.CSVExporter.saveMerchantDataToCsv
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.zip.CheckedInputStream

@FlowPreview
class SyncProcessor(private val mode: OperationMode, private val debug: Boolean = false) {
    companion object {
        const val CURRENT_VERSION = 4
        const val BUILD = 5
    }

    private val logger = LoggerFactory.getLogger(SyncProcessor::class.java)!!

    private val slackMessenger = SlackMessenger()

    private lateinit var dbFile: File

    private val gcManager by lazy {
        GCManager(mode)
    }

    @FlowPreview
    suspend fun syncData(workingDir: File, forceUpload: Boolean, quietMode: Boolean) {
        slackMessenger.quietMode = quietMode
        slackMessenger.postSlackMessage("### Sync started for v$CURRENT_VERSION ($BUILD) ### - $mode", logger)

        try {
            val syncLock = gcManager.checkLock()
            val syncLockCreateTime = syncLock.first
            // lock expires after 10 minutes if it wasn't removed for any reason
            val lockValid = System.currentTimeMillis() < (syncLockCreateTime + TimeUnit.MINUTES.toMillis(10))
            if (lockValid) {
                slackMessenger.postSlackMessage("Sync already in progress (${syncLock.second})", logger)
                return
            }
            gcManager.createLockFile(mode.name)

            dbFile = createEmptyDB(workingDir)
            val locationsDbFile = createLocationsDB(workingDir)
            importData(dbFile, locationsDbFile)

            val dbFileChecksum = calculateChecksum(dbFile)
            logger.debug("DB file checksum $dbFileChecksum")

            val dbZipFileName = when (mode) {
                OperationMode.PRODUCTION -> "${dbFile.nameWithoutExtension}-v$CURRENT_VERSION.zip"
                OperationMode.TESTNET -> "${dbFile.nameWithoutExtension}-v$CURRENT_VERSION-testnet.zip"
                OperationMode.DEVNET -> "${dbFile.nameWithoutExtension}-v$CURRENT_VERSION-devnet.zip"
            }

            val dbZipFile = File(workingDir, dbZipFileName)

            val remoteChecksum = gcManager.remoteChecksum(dbZipFile)
            val changesDetected = dbFileChecksum != remoteChecksum

            if (changesDetected || forceUpload) {
                if (changesDetected) {
                    if (remoteChecksum != null) {
                        slackMessenger.postSlackMessage(
                            "Changes detected ($dbFileChecksum vs $remoteChecksum) - updating",
                            logger
                        )
                    } else {
                        logger.notice("No remote data - uploading")
                    }
                } else {
                    logger.notice("Force upload active - updating")
                }

                throwIfCanceled()
                val timestamp = Calendar.getInstance().timeInMillis
                val password = dbFileChecksum.toCharArray()
                compress(dbFile, dbZipFile, password, timestamp, dbFileChecksum)

                throwIfCanceled()
                // upload the zipped database
                gcManager.uploadObject(dbZipFile, timestamp, dbFileChecksum)
                // copy and upload the uncompressed database
                val locationsDbChecksum = calculateChecksum(locationsDbFile)
                gcManager.uploadObject(locationsDbFile, timestamp, locationsDbChecksum)
            } else {
                logger.notice("No changes were detected, updating canceled")
                slackMessenger.postSlackMessage("No changes detected, updating canceled")
            }

            slackMessenger.postSlackMessage("### Sync finished ###", logger)

            gcManager.deleteLockFile()
        } catch (ex: InterruptedException) {
            slackMessenger.postSlackMessage("!!! Sync canceled !!!", logger)
            gcManager.deleteLockFile()
        } catch (ex: Exception) {
            logger.error(ex.message, ex)
            slackMessenger.postSlackMessage("### Sync failed ### ${ex.message}")
            gcManager.deleteLockFile()
        }
    }

    @Throws(InterruptedException::class)
    private fun throwIfCanceled() {
        if (gcManager.cancelRequested()) {
            throw InterruptedException("Sync canceled")
        }
    }

    @Throws(SQLException::class)
    private suspend fun importData(dbFile: File, locationsDbFile: File) {
        val ctxDataSource = CTXSpendDataSource(slackMessenger)
        val ctxData = ctxDataSource.getDataList()
        val ctxReport = ctxDataSource.getReport()
        val piggyCardsDataSource = PiggyCardsDataSource(slackMessenger, mode)
        val piggyCardsData = piggyCardsDataSource.getDataList()
        val piggyCardsReport = piggyCardsDataSource.getReport()
        val report = SyncReport(listOf(ctxReport, piggyCardsReport))
        if (debug) {
            saveMerchantDataToCsv(ctxData, "ctx.csv")
            saveMerchantDataToCsv(piggyCardsData, "piggycards.csv")
        }
        var matchedInfo: List<MerchantLocationMerger.MatchInfo>? = null

        val dbConnection = DriverManager.getConnection("jdbc:sqlite:${dbFile.path}")
        val combinedMerchants = try {
            // merchant table
            var prepStatement = dbConnection.prepareStatement(MerchantData.INSERT_STATEMENT)
            val dcgDataFlow = DCGDataSource(mode != OperationMode.PRODUCTION, slackMessenger).getData(prepStatement)
            val merger = MerchantLocationMerger(debug)
            val combinedMerchants = merger.combineMerchants(listOf(ctxData, piggyCardsData))
            logger.info("Duplicate locations: ${combinedMerchants.matchInfo.size}")
            val merchantSet = hashSetOf<String>()
            combinedMerchants.giftCardProviders.forEach { merchantSet.add(it.merchantId!!) }
            logger.info("Total merchants: ${merchantSet.size}")
            logger.info("Total Locations: ${combinedMerchants.merchants.size}")
            report.mergedLocations = combinedMerchants.matchInfo.size
            report.totalMerchants = merchantSet.size
            report.totalLocations = combinedMerchants.merchants.size

            matchedInfo = combinedMerchants.matchInfo
            val combinedMerchantsFlow = combinedMerchants.merchants.asFlow().transform { data ->
                data.transferInto(prepStatement)
                emit(data)
            }
            
            val merchantDataFlow = flowOf(dcgDataFlow, combinedMerchantsFlow).flattenConcat()
            syncData(merchantDataFlow, prepStatement)

            // gift_card_provider table
            prepStatement = dbConnection.prepareStatement(GiftCardProvider.INSERT_STATEMENT)
            val giftCardProviderFlow = combinedMerchants.giftCardProviders.asFlow().transform { data ->
                data.transferInto(prepStatement)
                emit(data)
            }
            syncData(giftCardProviderFlow, prepStatement)

            // atm table
            prepStatement = dbConnection.prepareStatement(AtmLocation.INSERT_STATEMENT)
            val coinAtmRadarDataFlow = CoinAtmRadarDataSource(slackMessenger).getData(prepStatement)
            val atmDataFlow = flowOf(coinAtmRadarDataFlow).flattenConcat()
            syncData(atmDataFlow, prepStatement)
            combinedMerchants
        } catch (ex: SQLException) {
            logger.error(ex.message, ex)
            throw ex
        } finally {
            if (!dbConnection.isClosed) {
                dbConnection.close()
            }
        }

        // Compare to the previously created database
        val previousLocationsFile = gcManager.downloadMostRecentLocationsDb()
        
        val ctxLocations = mutableListOf<MerchantData>()
        val piggyCardsLocations = mutableListOf<MerchantData>()

        previousLocationsFile?.let { locationFile ->
            logger.info("Loading previous locations data from ${locationFile.name}")
            val previousDbConnection = DriverManager.getConnection("jdbc:sqlite:${locationFile.path}")
            try {
                // Load CTX records
                val ctxQuery = "SELECT * FROM from_providers WHERE source = 'CTX'"
                previousDbConnection.prepareStatement(ctxQuery).use { statement ->
                    val resultSet = statement.executeQuery()
                    while (resultSet.next()) {
                        ctxLocations.add(MerchantData(
                            merchantId = resultSet.getString("merchantId"),
                            active = resultSet.getInt("active") == 1,
                            name = resultSet.getString("name"),
                            address1 = resultSet.getString("address1"),
                            address2 = resultSet.getString("address2"),
                            address3 = resultSet.getString("address3"),
                            address4 = resultSet.getString("address4"),
                            latitude = resultSet.getObject("latitude") as Double?,
                            longitude = resultSet.getObject("longitude") as Double?,
                            website = resultSet.getString("website"),
                            phone = resultSet.getString("phone"),
                            territory = resultSet.getString("territory"),
                            city = resultSet.getString("city"),
                            source = resultSet.getString("source"),
                            sourceId = resultSet.getString("sourceId"),
                            type = resultSet.getString("type"),
                            redeemType = resultSet.getString("redeemType"),
                            savingsPercentage = resultSet.getObject("savingsPercentage") as Int?,
                            denominationsType = resultSet.getString("denominationsType")
                        ))
                    }
                }
                
                // Load PiggyCards records
                val piggyQuery = "SELECT * FROM from_providers WHERE source = 'PiggyCards'"
                previousDbConnection.prepareStatement(piggyQuery).use { statement ->
                    val resultSet = statement.executeQuery()
                    while (resultSet.next()) {
                        piggyCardsLocations.add(MerchantData(
                            merchantId = resultSet.getString("merchantId"),
                            active = resultSet.getInt("active") == 1,
                            name = resultSet.getString("name"),
                            address1 = resultSet.getString("address1"),
                            address2 = resultSet.getString("address2"),
                            address3 = resultSet.getString("address3"),
                            address4 = resultSet.getString("address4"),
                            latitude = resultSet.getObject("latitude") as Double?,
                            longitude = resultSet.getObject("longitude") as Double?,
                            website = resultSet.getString("website"),
                            phone = resultSet.getString("phone"),
                            territory = resultSet.getString("territory"),
                            city = resultSet.getString("city"),
                            source = resultSet.getString("source"),
                            sourceId = resultSet.getString("sourceId"),
                            type = resultSet.getString("type"),
                            redeemType = resultSet.getString("redeemType"),
                            savingsPercentage = resultSet.getObject("savingsPercentage") as Int?,
                            denominationsType = resultSet.getString("denominationsType")
                        ))
                    }
                }
                
                // Query distinct names from both sources
                val previousCTXMerchants = mutableListOf<String>()
                val previousPiggyCardsMerchants = mutableListOf<String>()
                
                // Load distinct CTX names
                val ctxNamesQuery = "SELECT DISTINCT name FROM from_providers WHERE source = 'CTX' AND name IS NOT NULL"
                previousDbConnection.prepareStatement(ctxNamesQuery).use { statement ->
                    val resultSet = statement.executeQuery()
                    while (resultSet.next()) {
                        previousCTXMerchants.add(resultSet.getString("name"))
                    }
                }
                
                // Load distinct PiggyCards names
                val piggyNamesQuery = "SELECT DISTINCT name FROM from_providers WHERE source = 'PiggyCards' AND name IS NOT NULL"
                previousDbConnection.prepareStatement(piggyNamesQuery).use { statement ->
                    val resultSet = statement.executeQuery()
                    while (resultSet.next()) {
                        previousPiggyCardsMerchants.add(resultSet.getString("name"))
                    }
                }
                
                logger.info("Loaded ${ctxLocations.size} CTX locations and ${piggyCardsLocations.size} PiggyCards locations from previous database")
                logger.info("Found ${previousCTXMerchants.size} distinct CTX merchant names and ${previousPiggyCardsMerchants.size} distinct PiggyCards merchant names")

                val currentCTXMerchants = ctxDataSource.merchantList.toList()
                val (newCTX, removedCTX) = findListDifferences(previousCTXMerchants, currentCTXMerchants) {
                    it
                }
                logger.info("CTX New merchants: $newCTX")
                logger.info("CTX Removed merchants: $removedCTX")
                 report.updateDataSourceReport(report["CTX"]!!.copy(newMerchants = newCTX, removedMerchants =  removedCTX))

                val currentPiggyCardsMerchants = piggyCardsDataSource.merchantList.toList()
                val (newPC, removedPC) = findListDifferences(previousPiggyCardsMerchants, currentPiggyCardsMerchants) {
                    MerchantNameNormalizer.normalizeName(it)
                }
                logger.info("PiggyCards New merchants: $newPC")
                logger.info("PiggyCards Removed merchants: $removedPC")
                report.updateDataSourceReport(report["PiggyCards"]!!.copy(newMerchants = newPC, removedMerchants =  removedPC))
            } catch (ex: SQLException) {
                logger.warn("Failed to load previous locations data: ${ex.message}")
            } finally {
                if (!previousDbConnection.isClosed) {
                    previousDbConnection.close()
                }
            }
        }


        // Process locations database
        val locationsDbConnection = DriverManager.getConnection("jdbc:sqlite:${locationsDbFile.path}")
        try {
            createLocationsTable(locationsDbConnection, "from_providers")
            populateLocationsTable(locationsDbConnection, "from_providers", ctxData + piggyCardsData)
            createDuplicatesTable(locationsDbConnection)
            populateDuplicatesTable(locationsDbConnection, matchedInfo, ctxData, piggyCardsData)
            createLocationsTable(locationsDbConnection, "final")
            populateLocationsTable(locationsDbConnection, "final", combinedMerchants.merchants)
        } catch (ex: SQLException) {
            logger.error(ex.message, ex)
            throw ex
        } finally {
            if (!locationsDbConnection.isClosed) {
                locationsDbConnection.close()
            }
        }
        slackMessenger.postSlackMessage(report.toString(), logger)
    }


    @Throws(IOException::class)
    private fun createEmptyDB(workingDir: File): File {
        val dbFile = File(workingDir, "explore.db")
        if (dbFile.exists()) {
            dbFile.delete()
        }
        dbFile.createNewFile()
        logger.debug("Creating empty DB ${dbFile.absolutePath}")

        val emptyDBStream = javaClass.classLoader.getResourceAsStream("explore-empty.db")
            ?: throw FileNotFoundException()

        emptyDBStream.use { input ->
            dbFile.outputStream().use { output ->
                input.copyTo(output)
            }
        }
        logger.debug("Empty DB created ${dbFile.absolutePath}")
        return dbFile
    }

    @Throws(IOException::class)
    private fun createLocationsDB(workingDir: File): File {
        val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.US)
        val today = dateFormat.format(Date())
        val dbFile = File(workingDir, "locations-$mode-$today.db")
        if (dbFile.exists()) {
            dbFile.delete()
        }
        dbFile.createNewFile()
        logger.info("Creating locations DB ${dbFile.absolutePath}")
        return dbFile
    }

    private fun calculateChecksum(file: File): String {
        return CheckedInputStream(file.inputStream(), Crc32c()).use {
            val buffer = ByteArray(1024)
            while (it.read(buffer, 0, buffer.size) >= 0) {
                // just calculate the checksum
            }
            it.checksum.value.toString(16)
        }
    }

    private fun <T> findListDifferences(
        previous: List<T>,
        current: List<T>,
        keySelector: (T) -> Any?
    ): Pair<List<T>, List<T>> {
        val previousKeys = previous.associateBy(keySelector)
        val currentKeys = current.associateBy(keySelector)
        val newItems = current.filter { keySelector(it) !in previousKeys }
        val removedItems = previous.filter { keySelector(it) !in currentKeys }
        return Pair(newItems, removedItems)
    }

    private suspend fun <T> syncData(data: Flow<T>, prepStatement: PreparedStatement) where T : Data {
        var batchSize = 0
        var totalRecords = 0

        data.onCompletion {
            if (batchSize > 0) {
                prepStatement.executeBatch()
                batchSize = 0
            }

            if (totalRecords == 0) {
                throw IllegalStateException("No data received")
            }

            logger.info("Table sync complete ($totalRecords records)")
        }.collect {
            prepStatement.addBatch()
            batchSize++
            totalRecords++

            if (batchSize == 128) {
                prepStatement.executeBatch()
                batchSize = 0
            }

            if (totalRecords % 20000 == 0) {
                throwIfCanceled()
            }
        }
    }

    @Throws(SQLException::class)
    private fun createLocationsTable(connection: Connection, tableName: String) {
        val sqlStream = javaClass.classLoader.getResourceAsStream("create_${tableName}_table.sql")
            ?: throw FileNotFoundException("create_${tableName}_table.sql not found in resources")

        val createTableSQL = sqlStream.bufferedReader().use { it.readText() }
        connection.createStatement().use { statement ->
            statement.execute(createTableSQL)
        }
        logger.info("Created $tableName table")
    }

    @Throws(SQLException::class)
    private fun populateLocationsTable(connection: Connection, tableName: String, data: List<MerchantData>) {
        val insertSQL = """
            INSERT INTO $tableName (
                merchantId,
                active, name, address1, address2, address3, address4,
                latitude, longitude, website, phone, territory, city,
                source, sourceId,
                type, redeemType, savingsPercentage, denominationsType
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent()

        connection.prepareStatement(insertSQL).use { prepStatement ->
            for (merchant in data) {
                prepStatement.setString(1, merchant.merchantId)
                prepStatement.setInt(2, if (merchant.active == true) 1 else 0)
                prepStatement.setString(3, merchant.name)
                prepStatement.setString(4, merchant.address1)
                prepStatement.setString(5, merchant.address2)
                prepStatement.setString(6, merchant.address3)
                prepStatement.setString(7, merchant.address4)
                merchant.latitude?.let { prepStatement.setDouble(8, it) } ?: prepStatement.setNull(
                    8,
                    java.sql.Types.REAL
                )
                merchant.longitude?.let { prepStatement.setDouble(9, it) } ?: prepStatement.setNull(
                    9,
                    java.sql.Types.REAL
                )
                prepStatement.setString(10, merchant.website)
                prepStatement.setString(11, merchant.phone)
                prepStatement.setString(12, merchant.territory)
                prepStatement.setString(13, merchant.city)
                prepStatement.setString(14, merchant.source)
                prepStatement.setString(15, merchant.sourceId)
                prepStatement.setString(16, merchant.type)
                prepStatement.setString(17, merchant.redeemType)
                merchant.savingsPercentage?.let { prepStatement.setInt(18, it) } ?: prepStatement.setNull(
                    18,
                    java.sql.Types.INTEGER
                )
                prepStatement.setString(19, merchant.denominationsType)
                prepStatement.addBatch()
            }
            prepStatement.executeBatch()
        }
        logger.info("Populated locations table with ${data.size} records")
    }

    @Throws(IOException::class)
    private fun compress(inFile: File, outFile: File, password: CharArray, timestamp: Long, checksum: String) {
        logger.debug("Compressing $inFile to $outFile")
        val zipParameters = ZipParameters()
        zipParameters.isEncryptFiles = true
        zipParameters.encryptionMethod = EncryptionMethod.AES
        zipParameters.compressionLevel = CompressionLevel.HIGHER
        ZipFile(outFile, password).apply {
            addFile(inFile, zipParameters)
            comment = "$timestamp#$checksum"
        }
        logger.debug("Compressing done $outFile")
    }

    @Throws(SQLException::class)
    private fun createDuplicatesTable(connection: Connection) {
        val sqlStream = javaClass.classLoader.getResourceAsStream("create_duplicates_table.sql")
            ?: throw FileNotFoundException("create_duplicates_table.sql not found in resources")

        val createTableSQL = sqlStream.bufferedReader().use { it.readText() }
        connection.createStatement().use { statement ->
            statement.execute(createTableSQL)
        }
        logger.info("Created duplicates table")
    }

    @Throws(SQLException::class)
    private fun populateDuplicatesTable(
        connection: Connection,
        matchedInfo: List<MerchantLocationMerger.MatchInfo>?,
        ctxData: List<MerchantData>,
        piggyCardsData: List<MerchantData>
    ) {
        if (matchedInfo == null) {
            logger.info("No match info available, skipping duplicates table population")
            return
        }

        val insertSQL = """
            INSERT INTO duplicates (
                CTX_merchantId, CTX_name, CTX_address1, CTX_address2, CTX_address3, CTX_address4,
                CTX_latitude, CTX_longitude, CTX_website, CTX_phone, CTX_territory, CTX_city,
                CTX_source, CTX_sourceId,
                PiggyCards_merchantId, PiggyCards_name, PiggyCards_address1, PiggyCards_address2, PiggyCards_address3, PiggyCards_address4,
                PiggyCards_latitude, PiggyCards_longitude, PiggyCards_territory, PiggyCards_city,
                PiggyCards_source, PiggyCards_sourceId
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent()

        connection.prepareStatement(insertSQL).use { prepStatement ->
            for (match in matchedInfo) {
                val ctxMerchant = ctxData[match.ctxIndex]
                val piggyMerchant = piggyCardsData[match.piggyIndex]

                // CTX data
                prepStatement.setString(1, ctxMerchant.merchantId)
                prepStatement.setString(2, ctxMerchant.name)
                prepStatement.setString(3, ctxMerchant.address1)
                prepStatement.setString(4, ctxMerchant.address2)
                prepStatement.setString(5, ctxMerchant.address3)
                prepStatement.setString(6, ctxMerchant.address4)
                ctxMerchant.latitude?.let { prepStatement.setDouble(7, it) } ?: prepStatement.setNull(7, java.sql.Types.REAL)
                ctxMerchant.longitude?.let { prepStatement.setDouble(8, it) } ?: prepStatement.setNull(8, java.sql.Types.REAL)
                prepStatement.setString(9, ctxMerchant.website)
                prepStatement.setString(10, ctxMerchant.phone)
                prepStatement.setString(11, ctxMerchant.territory)
                prepStatement.setString(12, ctxMerchant.city)
                prepStatement.setString(13, ctxMerchant.source)
                prepStatement.setString(14, ctxMerchant.sourceId)

                // PiggyCards data
                prepStatement.setString(15, piggyMerchant.merchantId)
                prepStatement.setString(16, piggyMerchant.name)
                prepStatement.setString(17, piggyMerchant.address1)
                prepStatement.setString(18, piggyMerchant.address2)
                prepStatement.setString(19, piggyMerchant.address3)
                prepStatement.setString(20, piggyMerchant.address4)
                piggyMerchant.latitude?.let { prepStatement.setDouble(21, it) } ?: prepStatement.setNull(21, java.sql.Types.REAL)
                piggyMerchant.longitude?.let { prepStatement.setDouble(22, it) } ?: prepStatement.setNull(22, java.sql.Types.REAL)
                prepStatement.setString(23, piggyMerchant.territory)
                prepStatement.setString(24, piggyMerchant.city)
                prepStatement.setString(25, piggyMerchant.source)
                prepStatement.setString(26, piggyMerchant.sourceId)

                prepStatement.addBatch()
            }
            prepStatement.executeBatch()
        }
        logger.info("Populated duplicates table with ${matchedInfo.size} records")
    }
}