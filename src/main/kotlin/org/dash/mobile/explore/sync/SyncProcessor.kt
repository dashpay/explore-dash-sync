package org.dash.mobile.explore.sync

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.combine
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
import org.dash.mobile.explore.sync.slack.SlackMessenger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.zip.CheckedInputStream
import kotlin.math.log

@FlowPreview
class SyncProcessor(private val mode: OperationMode) {

    private val logger = LoggerFactory.getLogger(SyncProcessor::class.java)!!

    private val slackMessenger = SlackMessenger()

    private lateinit var dbFile: File

    private val gcManager by lazy {
        GCManager(mode)
    }

    @FlowPreview
    suspend fun syncData(workingDir: File, forceUpload: Boolean, quietMode: Boolean) {
        slackMessenger.quietMode = quietMode
        slackMessenger.postSlackMessage("### Sync started ### - $mode", logger)

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
            importData(dbFile)

            val dbFileChecksum = calculateChecksum(dbFile)
            logger.debug("DB file checksum $dbFileChecksum")

            val dbZipFileName = when (mode) {
                OperationMode.PRODUCTION -> "${dbFile.nameWithoutExtension}-v4.zip"
                OperationMode.TESTNET -> "${dbFile.nameWithoutExtension}-v4-testnet.zip"
                OperationMode.DEVNET -> "${dbFile.nameWithoutExtension}-v4-devnet.zip"
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
                gcManager.uploadObject(dbZipFile, timestamp, dbFileChecksum)
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
    private suspend fun importData(dbFile: File) {
        val dbConnection = DriverManager.getConnection("jdbc:sqlite:${dbFile.path}")
        try {
            var prepStatement = dbConnection.prepareStatement(MerchantData.INSERT_STATEMENT)
            val dcgDataFlow = DCGDataSource(mode != OperationMode.PRODUCTION, slackMessenger).getData(prepStatement)
            //val ctxDataFlow = CTXSpendDataSource(slackMessenger).getData(prepStatement)
            val ctxData = CTXSpendDataSource(slackMessenger).getDataList()
            //val piggyCardsDataFlow = PiggyCardsDataSource(slackMessenger).getData(prepStatement)
            val piggyCardsData = PiggyCardsDataSource(slackMessenger).getDataList()
            val combinedMerchants = combineMerchants(listOf(ctxData, piggyCardsData))
            val combinedFlow = combinedMerchants.asFlow().transform { data ->
                data.transferInto(prepStatement)
                emit(data)
            }
            
            val merchantDataFlow = flowOf(dcgDataFlow, combinedFlow).flattenConcat()
            syncData(merchantDataFlow, prepStatement)

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
    }

    private fun combineMerchants(
        lists: List<List<MerchantData>>
    ): List<MerchantData> {
        if (lists.isEmpty()) return emptyList()
        
        val result = mutableListOf<MerchantData>()
        val locationMap = mutableMapOf<String, MerchantData>()
        
        // Process each list in order, prioritizing earlier lists
        lists.forEach { merchantList ->
            merchantList.forEach { merchant ->
                val locationKey = createLocationKey(merchant.latitude, merchant.longitude)
                
                // Only add if we haven't seen this location before (first list wins)
                if (!locationMap.containsKey(locationKey)) {
                    locationMap[locationKey] = merchant
                    result.add(merchant)
                }
            }
        }
        var count = 0
        lists.forEach { count += it.size }
        logger.info("combining {} -> {}", count, result.size)
        return result
    }
    
    private fun createLocationKey(latitude: Double?, longitude: Double?): String {
        if (latitude == null || longitude == null) {
            // For merchants without coordinates, use a unique key based on object identity
            return "null_coords_${System.identityHashCode(latitude)}_${System.identityHashCode(longitude)}"
        }
        
        // Round to 4 decimal places
        val roundedLat = "%.4f".format(latitude)
        val roundedLng = "%.4f".format(longitude)
        
        return "${roundedLat}_${roundedLng}"
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
