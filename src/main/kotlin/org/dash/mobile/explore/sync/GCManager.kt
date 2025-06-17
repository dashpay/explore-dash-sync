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
private const val GCS_LOCK_FILE_NAME = "explore/v3.lock"

const val CHECKSUM_META_KEY = "Data-Checksum"
const val TIMESTAMP_META_KEY = "Data-Timestamp"

class GCManager(private val mode: OperationMode) {

    private val logger = LoggerFactory.getLogger(GCManager::class.java)!!

    private val gcStorage by lazy {
        createStorage()
    }

    private val lockFileName by lazy {
        "$GCS_LOCK_FILE_NAME-$mode"
    }

    fun remoteChecksum(dataFile: File): String? {
        return gcStorage.get(GCS_BUCKET_NAME)?.get(
            "explore/${dataFile.nameWithoutExtension}.db"
        )?.metadata?.get(CHECKSUM_META_KEY)
    }

    @Throws(IOException::class)
    fun uploadObject(dataFile: File, timestamp: Long, checksumHex: String) {
        logger.info("Uploading data to GC Storage")
        val objectName = "${dataFile.nameWithoutExtension}.db"
        val blobId = BlobId.of(GCS_BUCKET_NAME, "explore/$objectName")
        val blobInfo = BlobInfo.newBuilder(blobId)
            .setMetadata(
                mapOf(
                    TIMESTAMP_META_KEY to timestamp.toString(),
                    CHECKSUM_META_KEY to checksumHex
                )
            )
            .build()
        gcStorage.create(blobInfo, dataFile.readBytes())
        logger.info("File $dataFile uploaded to bucket $GCS_BUCKET_NAME as $objectName")
    }

    private fun createStorage(): Storage {
        val serviceAccount =
            javaClass.classLoader.getResourceAsStream("credentials.json")
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

    @Throws(IOException::class)
    fun checkLock(): Pair<Long, String?> {
        val lockFile = gcStorage.get(GCS_BUCKET_NAME, lockFileName)
        return lockFile?.run {
            Pair(createTime, metadata["mode"])
        } ?: Pair(-1, null)
    }

    @Throws(IOException::class)
    fun cancelRequested(): Boolean {
        val lockFile = gcStorage.get(GCS_BUCKET_NAME, lockFileName)
        return lockFile.metadata["cancel"] == "true"
    }

    @Throws(IOException::class)
    fun createLockFile(mode: String) {
        logger.debug("Creating .lock file")
        val blobId = BlobId.of(GCS_BUCKET_NAME, lockFileName)
        val blobInfo = BlobInfo.newBuilder(blobId)
            .setMetadata(mapOf("mode" to mode)).build()
        gcStorage.create(blobInfo)
        logger.info(".lock file created ($mode)")
    }

    @Throws(IOException::class)
    fun deleteLockFile() {
        logger.debug("Deleting .lock file")
        val blobId = BlobId.of(GCS_BUCKET_NAME, lockFileName)
        gcStorage.delete(blobId)
        logger.info(".lock file deleted")
    }
}
