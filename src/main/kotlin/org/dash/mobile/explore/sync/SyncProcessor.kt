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
            // merchant table
            var prepStatement = dbConnection.prepareStatement(MerchantData.INSERT_STATEMENT)
            val dcgDataFlow = DCGDataSource(mode != OperationMode.PRODUCTION, slackMessenger).getData(prepStatement)
            //val ctxDataFlow = CTXSpendDataSource(slackMessenger).getData(prepStatement)
            val ctxData = CTXSpendDataSource(slackMessenger).getDataList()
            saveMerchantDataToCsv(ctxData, "ctx.csv")
            //val piggyCardsDataFlow = PiggyCardsDataSource(slackMessenger).getData(prepStatement)
            val piggyCardsData = PiggyCardsDataSource(slackMessenger).getDataList()
            saveMerchantDataToCsv(piggyCardsData, "piggycards.csv")
            val merger = MerchantLocationMerger()
            val combinedMerchants = merger.combineMerchants(listOf(ctxData, piggyCardsData))
            val combinedMerchantsFlow = combinedMerchants.first.asFlow().transform { data ->
                data.transferInto(prepStatement)
                emit(data)
            }
            
            val merchantDataFlow = flowOf(dcgDataFlow, combinedMerchantsFlow).flattenConcat()
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


    @Throws(IOException::class)
    fun saveMerchantDataToCsv(merchantData: List<MerchantData>, fileName: String) {
        val file = File(fileName)
        
        file.bufferedWriter().use { writer ->
            // Write CSV header
            writer.write("deeplink,plusCode,addDate,updateDate,paymentMethod,merchantId,id,active,name,address1,address2,address3,address4,latitude,longitude,website,phone,territory,city,source,sourceId,logoLocation,googleMaps,coverImage,type,redeemType,savingsPercentage,denominationsType,instagram,twitter,delivery,monOpen,monClose,tueOpen,tueClose,wedOpen,wedClose,thuOpen,thuClose,friOpen,friClose,satOpen,satClose,sunOpen,sunClose")
            writer.newLine()
            
            // Write data rows
            merchantData.forEach { merchant ->
                val row = listOf(
                    escapeCsvField(merchant.deeplink),
                    escapeCsvField(merchant.plusCode),
                    escapeCsvField(merchant.addDate),
                    escapeCsvField(merchant.updateDate),
                    escapeCsvField(merchant.paymentMethod),
                    escapeCsvField(merchant.merchantId),
                    merchant.id?.toString() ?: "",
                    merchant.active?.toString() ?: "",
                    escapeCsvField(merchant.name),
                    escapeCsvField(merchant.address1),
                    escapeCsvField(merchant.address2),
                    escapeCsvField(merchant.address3),
                    escapeCsvField(merchant.address4),
                    merchant.latitude?.toString() ?: "",
                    merchant.longitude?.toString() ?: "",
                    escapeCsvField(merchant.website),
                    escapeCsvField(merchant.phone),
                    escapeCsvField(merchant.territory),
                    escapeCsvField(merchant.city),
                    escapeCsvField(merchant.source),
                    escapeCsvField(merchant.sourceId),
                    escapeCsvField(merchant.logoLocation),
                    escapeCsvField(merchant.googleMaps),
                    escapeCsvField(merchant.coverImage),
                    escapeCsvField(merchant.type),
                    escapeCsvField(merchant.redeemType),
                    merchant.savingsPercentage?.toString() ?: "",
                    escapeCsvField(merchant.denominationsType),
                    escapeCsvField(merchant.instagram),
                    escapeCsvField(merchant.twitter),
                    escapeCsvField(merchant.delivery),
                    escapeCsvField(merchant.monOpen),
                    escapeCsvField(merchant.monClose),
                    escapeCsvField(merchant.tueOpen),
                    escapeCsvField(merchant.tueClose),
                    escapeCsvField(merchant.wedOpen),
                    escapeCsvField(merchant.wedClose),
                    escapeCsvField(merchant.thuOpen),
                    escapeCsvField(merchant.thuClose),
                    escapeCsvField(merchant.friOpen),
                    escapeCsvField(merchant.friClose),
                    escapeCsvField(merchant.satOpen),
                    escapeCsvField(merchant.satClose),
                    escapeCsvField(merchant.sunOpen),
                    escapeCsvField(merchant.sunClose)
                ).joinToString(",")
                
                writer.write(row)
                writer.newLine()
            }
        }
        
        logger.info("Saved ${merchantData.size} merchant records to $fileName")
    }

    private fun escapeCsvField(field: String?): String {
        if (field == null) return ""
        
        val escaped = field.replace("\"", "\"\"")
        
        return if (escaped.contains(",") || escaped.contains("\"") || escaped.contains("\n") || escaped.contains("\r")) {
            "\"$escaped\""
        } else {
            escaped
        }
    }
}

private fun MerchantData.debug(): String {
    return "$name: $address1 [$latitude, $longitude]"
}
