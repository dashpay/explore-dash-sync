package org.dash.mobile.explore.sync

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import org.dash.mobile.explore.sync.process.CREDENTIALS_FILE_PATH
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException

private const val GC_PROJECT_ID = "dash-wallet-firebase"

private const val GCS_BUCKET_NAME = "dash-wallet-firebase.appspot.com"

const val CHECKSUM_META_KEY = "Data-Checksum"

class GCManager {

    private val logger = LoggerFactory.getLogger(GCManager::class.java)!!

    private val gcStorage by lazy {
        createStorage()
    }

    fun remoteChecksum1(dataFile: File): String? {
        val remoteData = gcStorage.get(GCS_BUCKET_NAME)?.get("explore/${dataFile.name}")
        return remoteData?.crc32cToHexString
    }

    fun remoteChecksum(dataFile: File): String? {
        return gcStorage.get(GCS_BUCKET_NAME)?.get("explore/${dataFile.name}")?.metadata?.get(CHECKSUM_META_KEY)
    }

    @Throws(IOException::class)
    fun uploadObject(dataFile: File, checksumHex: String) {
        logger.info("Uploading data to GC Storage")
        val objectName = dataFile.name
        val blobId = BlobId.of(GCS_BUCKET_NAME, "explore/$objectName")
        val blobInfo = BlobInfo.newBuilder(blobId)
            .setMetadata(mapOf(CHECKSUM_META_KEY to checksumHex))
            .build()
        gcStorage.create(blobInfo, dataFile.readBytes())
        logger.info("File $dataFile uploaded to bucket $GCS_BUCKET_NAME as $objectName")
    }

    private fun createStorage(): Storage {
        val serviceAccount =
            javaClass.classLoader.getResourceAsStream("dash-wallet-firebase-619341caf23e.json")
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
