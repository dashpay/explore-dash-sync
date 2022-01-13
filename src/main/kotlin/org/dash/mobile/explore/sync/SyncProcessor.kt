package org.dash.mobile.explore.sync

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.protobuf.Int32Value
import net.lingala.zip4j.io.outputstream.ZipOutputStream
import org.dash.mobile.explore.sync.process.CoinFlipImporter
import org.dash.mobile.explore.sync.process.SpreadsheetImporter
import org.dash.wallet.features.exploredash.data.model.Protos
import org.slf4j.LoggerFactory
import java.io.*
import java.util.zip.CRC32
import java.util.zip.CheckedOutputStream
import net.lingala.zip4j.model.ZipParameters
import net.lingala.zip4j.model.enums.CompressionLevel
import net.lingala.zip4j.model.enums.EncryptionMethod
import org.dash.mobile.explore.sync.process.CREDENTIALS_FILE_PATH

import java.io.IOException


const val GC_PROJECT_ID = "dash-wallet-firebase"
const val GCS_BUCKET_NAME = "explore-dash-sync"
const val OUTPUT_FILE = "explore.dat"

const val CHECKSUM_META_KEY = "Data-Checksum"

class SyncProcessor(val archivePath: String) {

    private val logger = LoggerFactory.getLogger(SyncProcessor::class.java)!!

    fun syncData(forceArchive: Boolean, upload: Boolean, srcDev: Boolean) {

        logger.notice("Sync started")

        ByteArrayOutputStream().use { buffer ->
            CheckedOutputStream(buffer, CRC32()).use { dataBuffer ->

                val dataVersionP = Int32Value.newBuilder().setValue(1).build()
                dataVersionP.writeDelimitedTo(dataBuffer)

                val atmData = CoinFlipImporter().import()
                val atmDataSize = Int32Value.newBuilder().setValue(atmData.size).build()
                atmDataSize.writeDelimitedTo(dataBuffer)
                logger.info("CoinFlip data size ${atmDataSize.value}")
                for (item in atmData) {
                    item.writeDelimitedTo(dataBuffer)
                }

                val merchantData = mutableListOf<Protos.MerchantData>()

                val spreadsheetData = SpreadsheetImporter().import()
                logger.info("Spreadsheet data size ${spreadsheetData.size}")
                merchantData.addAll(spreadsheetData)

//                val dashDirectData = DashDirectImporter(srcDev).import()
//                logger.info("DashDirect data size ${dashDirectData.size}")
//                merchantData.addAll(dashDirectData)

                val merchantDataSize = Int32Value.newBuilder().setValue(merchantData.size).build()
                merchantDataSize.writeDelimitedTo(dataBuffer)
                logger.info("Merchant data size ${merchantDataSize.value}")

                for (item in merchantData) {
                    item.writeDelimitedTo(dataBuffer)
                }

                val checksum = dataBuffer.checksum.value
                logger.info("Data checksum $checksum")

                process(forceArchive, upload, checksum, buffer)
            }
        }

//        FileInputStream("./exploredata.bin").use { inFile ->
//            val updateDate = Int64Value.parseDelimitedFrom(inFile)
//            val dataSize = Int32Value.parseDelimitedFrom(inFile)
//            println("updateDate $updateDate, dataSize $dataSize")
//            var count = 0
//            var merchant: Protos.MerchantData?
//            while (Protos.MerchantData.parseDelimitedFrom(inFile).also { merchant = it } != null) {
////                println("count: $count")
//                count++
//            }
//            println("count: $count")
//        }

        logger.notice("Sync finished")
    }

    private fun process(
        forceArchive: Boolean,
        upload: Boolean,
        newDataChecksum: Long,
        dataBuffer: ByteArrayOutputStream
    ) {
//        val inFile = ZipFile("./exploredata.zip")
//        if (inFile.file.exists() && inFile.comment == newDataChecksum.toString()) {
//            logger.info("No changes, updating canceled")
//        } else {
//            logger.info("Data has changed, updating file")
//        }
        val gcStorage = createStorage()
        val existingDataChecksum =
            gcStorage.get(GCS_BUCKET_NAME)?.get(OUTPUT_FILE)?.metadata?.get(CHECKSUM_META_KEY)
        val newDataChecksumHex = newDataChecksum.toString(16)

        val dataChanged = existingDataChecksum != newDataChecksumHex

        if (dataChanged || forceArchive) {

            if (!forceArchive) {
                logger.notice("Data has changed ($existingDataChecksum vs $newDataChecksumHex), updating")
            }

            val dataFile = File(archivePath)
            val password = newDataChecksumHex.hashCode().toString().reversed()
            saveArchive(newDataChecksum, dataBuffer, dataFile, password.toCharArray())

            if (upload) {
                uploadObject(gcStorage, GCS_BUCKET_NAME, dataFile, newDataChecksum)
            }
        } else {
            logger.notice("No changes were detected nor force upload activated, updating canceled")
        }
    }

    @Throws(IOException::class)
    private fun saveArchive(
        checksum: Long,
        dataBuffer: ByteArrayOutputStream,
        outFile: File,
        password: CharArray
    ) {
        logger.info("Saving data to $outFile")
        val zipParameters = ZipParameters()
        zipParameters.isEncryptFiles = true
        zipParameters.encryptionMethod = EncryptionMethod.AES
        zipParameters.compressionLevel = CompressionLevel.NORMAL
        ZipOutputStream(FileOutputStream(outFile), password).use { outStream ->
            zipParameters.fileNameInZip = "${outFile.nameWithoutExtension}.bin"
            outStream.putNextEntry(zipParameters)
            dataBuffer.writeTo(outStream)
            outStream.setComment(checksum.toString())
        }
        logger.info("Data saved $outFile.")
    }

    @Throws(IOException::class)
    fun uploadObject(storage: Storage, bucketName: String, dataFile: File, checksum: Long) {
        logger.info("Uploading data to GC Storage")
        val objectName = dataFile.name
        val blobId = BlobId.of(bucketName, objectName)
        val blobInfo = BlobInfo.newBuilder(blobId)
            .setMetadata(mapOf(CHECKSUM_META_KEY to checksum.toString(16)))
            .build()
        storage.create(blobInfo, dataFile.readBytes())
        logger.info("File $dataFile uploaded to bucket $bucketName as $objectName")
    }

    private fun createStorage(): Storage {
        val serviceAccount =
            javaClass.classLoader.getResourceAsStream("dash-wallet-firebase-3dcb5c05f13e.json")
                ?: throw FileNotFoundException(
                    "Google API credentials ($CREDENTIALS_FILE_PATH) not found." +
                            "You can download it from https://console.cloud.google.com/apis/credentials"
                )

        val credentials = GoogleCredentials.fromStream(serviceAccount)
//            .createScoped(arrayListOf("https://www.googleapis.com/auth/cloud-platform"))
        return StorageOptions.newBuilder()
            .setProjectId(GC_PROJECT_ID)
            .setCredentials(credentials)
            .build().service
    }
}