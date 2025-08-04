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
class SyncProcessor(private val mode: OperationMode) {
    companion object {
        const val CURRENT_VERSION = 4
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
        slackMessenger.postSlackMessage("### Sync started for v$CURRENT_VERSION ### - $mode", logger)

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

            val dbFileName = when (mode) {
                OperationMode.PRODUCTION -> "${dbFile.nameWithoutExtension}-v$CURRENT_VERSION-uncompressed.db"
                OperationMode.TESTNET -> "${dbFile.nameWithoutExtension}-v$CURRENT_VERSION-testnet-uncompressed.db"
                OperationMode.DEVNET -> "${dbFile.nameWithoutExtension}-v$CURRENT_VERSION-devnet-uncompressed.db"
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
                val dbFileWithName = File(workingDir, dbFileName)
                dbFile.copyTo(dbFileWithName, overwrite = true)
                gcManager.uploadObject(dbFileWithName, timestamp, dbFileChecksum)
            } else {
                logger.notice("No changes were detected, updating canceled")
                slackMessenger.postSlackMessage("No changes detected, updating canceled", logger)
            }

            slackMessenger.postSlackMessage("### Sync finished ###", logger)

            gcManager.deleteLockFile()
        } catch (ex: InterruptedException) {
            slackMessenger.postSlackMessage("!!! Sync canceled !!!", logger)
            gcManager.deleteLockFile()
        } catch (ex: Exception) {
            logger.error(ex.message, ex)
            slackMessenger.postSlackMessage("### Sync failed ### ${ex.message}", logger)
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
        val ctxData = CTXSpendDataSource(slackMessenger).getDataList()
        saveMerchantDataToCsv(ctxData, "ctx.csv")
        val piggyCardsData = PiggyCardsDataSource(slackMessenger).getDataList()
        saveMerchantDataToCsv(piggyCardsData, "piggycards.csv")
        
        val dbConnection = DriverManager.getConnection("jdbc:sqlite:${dbFile.path}")
        try {
            // merchant table
            var prepStatement = dbConnection.prepareStatement(MerchantData.INSERT_STATEMENT)
            val dcgDataFlow = DCGDataSource(mode != OperationMode.PRODUCTION, slackMessenger).getData(prepStatement)
            val merger = MerchantLocationMerger()
            val combinedMerchants = merger.combineMerchants(listOf(ctxData, piggyCardsData))
            val combinedMerchantsFlow = combinedMerchants.first.asFlow().transform { data ->
                data.transferInto(prepStatement)
                emit(data)
            }
            
            val merchantDataFlow = flowOf(dcgDataFlow, combinedMerchantsFlow).flattenConcat()
            syncData(merchantDataFlow, prepStatement)

            // gift_card_provider table
            prepStatement = dbConnection.prepareStatement(GiftCardProvider.INSERT_STATEMENT)
            val giftCardProviderFlow = combinedMerchants.second.asFlow().transform { data ->
                data.transferInto(prepStatement)
                emit(data)
            }
            syncData(giftCardProviderFlow, prepStatement)

            // atm table
            prepStatement = dbConnection.prepareStatement(AtmLocation.INSERT_STATEMENT)
            val coinAtmRadarDataFlow = CoinAtmRadarDataSource(slackMessenger).getData(prepStatement)
            val atmDataFlow = flowOf(coinAtmRadarDataFlow).flattenConcat()
            syncData(atmDataFlow, prepStatement)
        } catch (ex: SQLException) {
            logger.error(ex.message, ex)
            throw ex
        } finally {
            if (!dbConnection.isClosed) {
                dbConnection.close()
            }
        }

        // Process locations database
        val locationsDbConnection = DriverManager.getConnection("jdbc:sqlite:${locationsDbFile.path}")
        try {
            createLocationsTable(locationsDbConnection)
            populateLocationsTable(locationsDbConnection, ctxData, piggyCardsData)
        } catch (ex: SQLException) {
            logger.error(ex.message, ex)
            throw ex
        } finally {
            if (!locationsDbConnection.isClosed) {
                locationsDbConnection.close()
            }
        }
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
        val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.getDefault())
        val today = dateFormat.format(Date())
        val dbFile = File(workingDir, "locations-$today.db")
        if (dbFile.exists()) {
            dbFile.delete()
        }
        dbFile.createNewFile()
        logger.debug("Creating locations DB ${dbFile.absolutePath}")
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

            logger.debug("Table sync complete ($totalRecords records)")
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
    private fun createLocationsTable(connection: Connection) {
        val sqlStream = javaClass.classLoader.getResourceAsStream("create_locations_table.sql")
            ?: throw FileNotFoundException("create_locations_table.sql not found in resources")
        
        val createTableSQL = sqlStream.bufferedReader().use { it.readText() }
        connection.createStatement().use { statement ->
            statement.execute(createTableSQL)
        }
        logger.debug("Created locations table")
    }

    @Throws(SQLException::class)
    private fun populateLocationsTable(connection: Connection, ctxData: List<MerchantData>, piggyCardsData: List<MerchantData>) {
        val insertSQL = """
            INSERT INTO locations (
                merchantId,
                active, name, address1, address2, address3, address4,
                latitude, longitude, website, phone, territory, city,
                source, sourceId,
                type, redeemType, savingsPercentage, denominationsType
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent()

        connection.prepareStatement(insertSQL).use { prepStatement ->
            for (merchant in ctxData + piggyCardsData) {
                if (merchant.type == "physical") {
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
            }
            prepStatement.executeBatch()
        }
        logger.debug("Populated locations table with ${ctxData.size + piggyCardsData.size} records")
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
}