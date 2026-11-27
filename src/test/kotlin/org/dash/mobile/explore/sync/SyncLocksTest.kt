package org.dash.mobile.explore.sync

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.io.File
import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

@FlowPreview
class SyncLocksTest {

    private class FakeGCManager : GCManager(OperationMode.TESTNET) {
        val events = arrayListOf<String>()
        var canceled: (ExploreVersion) -> Boolean = { false }
        var failingLock: ExploreVersion? = null

        override fun checkLock(version: ExploreVersion): Pair<Long, String?> = Pair(-1, null)

        override fun createLockFile(version: ExploreVersion, mode: String) {
            if (version == failingLock) {
                throw IOException("lock not created")
            }
            events.add("lock v${version.number}")
        }

        override fun deleteLockFile(version: ExploreVersion) {
            events.add("unlock v${version.number}")
        }

        override fun cancelRequested(version: ExploreVersion) = canceled(version)

        override fun remoteChecksum(dataFile: File): String? = null

        override fun uploadObject(dataFile: File, timestamp: Long, checksumHex: String) {
            events.add("upload ${dataFile.name}")
        }
    }

    @TempDir
    lateinit var tempDir: File

    private val gcManager = FakeGCManager()
    private val today = SimpleDateFormat("yyyy-MM-dd", Locale.US).format(Date())
    private val v4Uploads = listOf("upload explore-v4-testnet.zip", "upload locations-TESTNET-$today.db")
    private val v5Uploads = listOf("upload explore-v5-testnet.zip", "upload locations-v5-TESTNET-$today.db")
    private var imports = 0

    private fun sync() {
        val ctxData = RegionFixtures.load("ctx.tsv")
        val piggyCardsData = RegionFixtures.load("piggycards.tsv")
        val data = SyncProcessor.ImportedData(
            ctxData,
            piggyCardsData,
            RegionFixtures.load("dcg.tsv"),
            RegionFixtures.atmData,
            MerchantLocationMerger(false).combineByRegion(ctxData, piggyCardsData)
        )
        runBlocking {
            SyncProcessor(OperationMode.TESTNET, gcManager = gcManager).syncData(tempDir, false, quietMode = true) {
                imports++
                it()
                data
            }
        }
    }

    @Test
    fun eachLockIsReleasedOnceItsVersionIsPublished() {
        sync()

        assertEquals(
            listOf("lock v4", "lock v5") + v4Uploads + "unlock v4" + v5Uploads + "unlock v5",
            gcManager.events
        )
    }

    @ParameterizedTest
    @EnumSource(ExploreVersion::class)
    fun cancelOnEitherLockStopsTheImportAndPublishesNothing(version: ExploreVersion) {
        gcManager.canceled = { it == version }

        sync()

        assertEquals(1, imports)
        assertEquals(listOf("lock v4", "lock v5", "unlock v4", "unlock v5"), gcManager.events)
    }

    @Test
    fun cancelOfOneVersionAfterTheImportStopsThatVersionOnly() {
        gcManager.canceled = { it == ExploreVersion.V5 && gcManager.events.contains("unlock v4") }

        sync()

        assertEquals(listOf("lock v4", "lock v5") + v4Uploads + "unlock v4" + "unlock v5", gcManager.events)
    }

    @Test
    fun failingToTakeTheSecondLockReleasesTheFirst() {
        gcManager.failingLock = ExploreVersion.V5

        sync()

        assertEquals(0, imports)
        assertEquals(listOf("lock v4", "unlock v4"), gcManager.events)
    }
}
