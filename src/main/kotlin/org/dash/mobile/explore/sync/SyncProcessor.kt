package org.dash.mobile.explore.sync

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.model.enums.CompressionLevel
import net.lingala.zip4j.model.enums.EncryptionMethod
import org.dash.mobile.explore.sync.process.CoinFlipDataSource
import org.dash.mobile.explore.sync.process.DCGDataSource
import org.dash.mobile.explore.sync.process.DashDirectDataSource
import org.dash.mobile.explore.sync.process.data.AtmData
import org.dash.mobile.explore.sync.process.data.Crc32c
import org.dash.mobile.explore.sync.process.data.Data
import org.dash.mobile.explore.sync.process.data.MerchantData
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException
import java.util.*
import java.util.zip.CheckedInputStream

@Suppress("BlockingMethodInNonBlockingContext")
@FlowPreview
class SyncProcessor {

    private val logger = LoggerFactory.getLogger(SyncProcessor::class.java)!!

    private lateinit var dbFile: File

    @FlowPreview
    suspend fun syncData(workingDir: File, srcDev: Boolean, forceUpload: Boolean) {

        logger.notice("### Sync started ###")

        try {
            dbFile = createEmptyDB(workingDir)
            importData(dbFile, srcDev)

            val dbFileChecksum = calculateChecksum(dbFile)
            logger.debug("DB file checksum $dbFileChecksum")

            val gcManager = GCManager()

            val dbZipFile = File(workingDir, "${dbFile.nameWithoutExtension}.zip")

            val remoteChecksum = gcManager.remoteChecksum(dbZipFile)
            val changesDetected = dbFileChecksum != remoteChecksum

            if (changesDetected || forceUpload) {
                if (changesDetected) {
                    if (remoteChecksum != null) {
                        logger.notice("Changes detected ($dbFileChecksum vs $remoteChecksum) - updating")
                    } else {
                        logger.notice("No remote data - uploading")
                    }
                } else {
                    logger.notice("Force upload active - updating")
                }

                val timestamp = Calendar.getInstance().timeInMillis
                val password = dbFileChecksum.toCharArray()
                compress(dbFile, dbZipFile, password, timestamp, dbFileChecksum)

                gcManager.uploadObject(dbZipFile, timestamp, dbFileChecksum)

            } else {
                logger.notice("No changes were detected, updating canceled")
            }

        } catch (ex: Exception) {
            logger.error(ex.message, ex)
        }

        logger.notice("### Sync finished ###")
    }

    @Throws(SQLException::class)
    private suspend fun importData(dbFile: File, srcDev: Boolean) {
        val dbConnection = DriverManager.getConnection("jdbc:sqlite:${dbFile.path}")
        try {
            var prepStatement = dbConnection.prepareStatement(MerchantData.INSERT_STATEMENT)
            val dashDirectDataFlow = DashDirectDataSource(srcDev).getData(prepStatement)
            val dcgDataFlow = DCGDataSource().getData(prepStatement)

            val merchantDataFlow = flowOf(dcgDataFlow, dashDirectDataFlow).flattenConcat()
            syncData(merchantDataFlow, prepStatement)

            prepStatement = dbConnection.prepareStatement(AtmData.INSERT_STATEMENT)
            val coinFlipDataFlow = CoinFlipDataSource().getData(prepStatement)
            val atmDataFlow = flowOf(coinFlipDataFlow).flattenConcat()
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

            logger.debug("Table sync complete ($totalRecords records)")

        }.collect {

            prepStatement.addBatch()
            batchSize++
            totalRecords++

            if (batchSize == 128) {
                prepStatement.executeBatch()
                batchSize = 0
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
