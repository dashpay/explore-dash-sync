package org.dash.mobile.explore.sync

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.runBlocking
import net.lingala.zip4j.ZipFile
import org.dash.mobile.explore.sync.RegionFixtures.MERCHANT_COLUMNS
import org.dash.mobile.explore.sync.RegionFixtures.PROVIDER_COLUMNS
import org.dash.mobile.explore.sync.RegionFixtures.dump
import org.dash.mobile.explore.sync.RegionFixtures.query
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

/**
 * regions/ctx.tsv holds US merchants taken from ctx.csv plus hand-made merchants of other countries.
 * regions/master-v4-*.tsv is what the code before explore v5 wrote for the US merchants of these fixtures.
 */
@FlowPreview
class ExploreRegionsTest {

    @TempDir
    lateinit var tempDir: File

    private val ctxData = RegionFixtures.load("ctx.tsv")
    private val piggyCardsData = RegionFixtures.load("piggycards.tsv")
    private val dcgData = RegionFixtures.load("dcg.tsv")
    private val atmData = RegionFixtures.atmData

    private fun merge() = MerchantLocationMerger(false).combineByRegion(ctxData, piggyCardsData)

    private fun write(version: ExploreVersion, merchants: RegionalResult = merge(), dirName: String = "run"): File {
        val processor = SyncProcessor(OperationMode.TESTNET, offlineMode = true)
        val dbFile = processor.createEmptyDB(File(tempDir, dirName).apply { mkdirs() }, version)
        processor.writeExploreDb(version, dbFile, dcgData, merchants.byVersion.getValue(version), atmData)
        return dbFile
    }

    private fun golden(table: String) = javaClass.classLoader.getResource("regions/master-v4-$table.tsv")!!
        .readText().lines().filter { it.isNotEmpty() }

    private fun ctxId(name: String, country: String, currency: String) =
        ctxData.first { it.name == name && it.country == country && it.currency == currency }.merchantId!!

    private fun merchantsNamed(dbFile: File, name: String) = query(
        dbFile,
        "SELECT DISTINCT merchantId, country, currency FROM merchant " +
            "WHERE name = '$name' AND paymentMethod = 'gift card' ORDER BY country, currency"
    )

    private fun providersOf(dbFile: File, merchantId: String) = query(
        dbFile,
        "SELECT provider, currency, sourceId FROM gift_card_providers " +
            "WHERE merchantId = '$merchantId' ORDER BY provider"
    )

    @Test
    fun oneImportIsPublishedAsOneFilePerVersion() {
        val merchants = merge()
        val data = SyncProcessor.ImportedData(ctxData, piggyCardsData, dcgData, atmData, merchants)
        val processor = SyncProcessor(OperationMode.TESTNET, offlineMode = true)
        runBlocking {
            ExploreVersion.values().forEach { processor.publish(it, data, tempDir, forceUpload = false) }
        }
        val today = SimpleDateFormat("yyyy-MM-dd", Locale.US).format(Date())

        assertEquals(
            listOf(
                "explore-v4-testnet.zip",
                "explore-v5-testnet.zip",
                "explore-v5.db",
                "explore.db",
                "locations-TESTNET-$today.db",
                "locations-v5-TESTNET-$today.db"
            ),
            tempDir.list()!!.filter { it.endsWith(".db") || it.endsWith(".zip") }.sorted()
        )
        // wallets open the entry by this name
        ExploreVersion.values().forEach {
            val zipFile = ZipFile(File(tempDir, "explore-v${it.number}-testnet.zip"))
            assertEquals(listOf("explore.db"), zipFile.fileHeaders.map { header -> header.fileName })
        }
        assertEquals("4", query(File(tempDir, "explore.db"), "PRAGMA user_version")[0][0])
        assertEquals("5", query(File(tempDir, "explore-v5.db"), "PRAGMA user_version")[0][0])
        val atms = "SELECT manufacturer, name, address1, territory, city, source, sourceId, type FROM atm"
        val atm = listOf(listOf("Operator", "Kiosk", "1 Main St", "VT", "Burlington", "coinatmradar", "1", "Buy Only"))
        assertEquals(atm, query(File(tempDir, "explore.db"), atms))
        assertEquals(atm, query(File(tempDir, "explore-v5.db"), atms))
        val finalRows = "SELECT count(*) FROM final"
        assertEquals(
            merchants.byVersion.getValue(ExploreVersion.V4).merchants.size.toString(),
            query(File(tempDir, "locations-TESTNET-$today.db"), finalRows)[0][0]
        )
        assertEquals(
            merchants.byVersion.getValue(ExploreVersion.V5).merchants.size.toString(),
            query(File(tempDir, "locations-v5-TESTNET-$today.db"), finalRows)[0][0]
        )
    }

    @Test
    fun eachVersionHasItsOwnLock() {
        val gcManager = GCManager(OperationMode.PRODUCTION)

        assertEquals("explore/v4.lock-PRODUCTION", gcManager.lockFileName(ExploreVersion.V4))
        assertEquals("explore/v5.lock-PRODUCTION", gcManager.lockFileName(ExploreVersion.V5))
    }

    @Test
    fun v4MatchesWhatMasterWroteForTheUsMerchants() {
        val dbFile = write(ExploreVersion.V4)

        assertEquals(golden("merchant"), dump(dbFile, "merchant", MERCHANT_COLUMNS))
        assertEquals(golden("gift_card_providers"), dump(dbFile, "gift_card_providers", PROVIDER_COLUMNS))
    }

    @Test
    fun v4HoldsOnlyUsDollarGiftCardMerchantsAndKeepsDcg() {
        val dbFile = write(ExploreVersion.V4)
        val usIds = ctxData.filter { it.country == "US" && it.currency == "USD" }.map { it.merchantId }.toSet()
        val otherIds = ctxData.map { it.merchantId }.toSet() - usIds
        assertEquals(10, otherIds.size)

        val ctxMerchantIds = query(dbFile, "SELECT DISTINCT merchantId FROM merchant WHERE source = 'CTX'")
            .map { it[0] }
        val ctxSourceIds = query(dbFile, "SELECT sourceId FROM gift_card_providers WHERE provider = 'CTX'")
            .map { it[0] }
        assertTrue(ctxMerchantIds.isNotEmpty())
        assertEquals(emptyList<String?>(), ctxMerchantIds.filter { it !in usIds })
        assertEquals(emptyList<String?>(), ctxSourceIds.filter { it in otherIds })
        assertEquals("0", query(dbFile, "SELECT count(*) FROM merchant WHERE name IN ('Euro Outlet', 'IKEA')")[0][0])
        assertEquals("4", query(dbFile, "SELECT count(*) FROM merchant WHERE source = 'DCG'")[0][0])
        val columns = query(dbFile, "SELECT name FROM pragma_table_info('merchant')").map { it[0] }
        assertFalse(columns.contains("country") || columns.contains("currency"))
    }

    @Test
    fun v5KeepsTheSameNameApartPerCountryAndCurrency() {
        val dbFile = write(ExploreVersion.V5)
        val amazonGb = ctxId("Amazon", "GB", "GBP")
        val amazonDe = ctxId("Amazon", "DE", "EUR")
        val amazonUs = merchantsNamed(dbFile, "Amazon").single { it[1] == "US" }[0]!!

        assertEquals(
            listOf(listOf(amazonDe, "DE", "EUR"), listOf(amazonGb, "GB", "GBP"), listOf(amazonUs, "US", "USD")),
            merchantsNamed(dbFile, "Amazon")
        )
        assertEquals(listOf(listOf("CTX", "GBP", amazonGb)), providersOf(dbFile, amazonGb))
        assertEquals(listOf(listOf("CTX", "EUR", amazonDe)), providersOf(dbFile, amazonDe))
        assertEquals(listOf("PiggyCards", "USD"), providersOf(dbFile, amazonUs).single().take(2))

        val ikeaAed = ctxId("IKEA", "AE", "AED")
        val ikeaUsd = ctxId("IKEA", "AE", "USD")
        assertEquals(listOf(listOf(ikeaAed, "AE", "AED"), listOf(ikeaUsd, "AE", "USD")), merchantsNamed(dbFile, "IKEA"))
        assertEquals(listOf(listOf("CTX", "AED", ikeaAed)), providersOf(dbFile, ikeaAed))
        assertEquals(listOf(listOf("CTX", "USD", ikeaUsd)), providersOf(dbFile, ikeaUsd))

        val euroOutlet = ctxId("Euro Outlet", "US", "EUR")
        assertEquals(listOf(listOf(euroOutlet, "US", "EUR")), merchantsNamed(dbFile, "Euro Outlet"))
    }

    @Test
    fun v5MerchantIdIsTheFirstCtxIdOfTheNameWithinItsCountry() {
        val dbFile = write(ExploreVersion.V5)
        // the Canadian merchants come first in the fixture and must not lend their id to the US one
        val burgerKingCa = ctxId("BURGER KING®", "CA", "CAD")
        val burgerKingUs = ctxId("BURGER KING®", "US", "USD")

        assertEquals(
            listOf(listOf(burgerKingCa, "CA", "CAD"), listOf(burgerKingUs, "US", "USD")),
            merchantsNamed(dbFile, "BURGER KING")
        )
        assertEquals(emptyList<List<String?>>(), merchantsNamed(dbFile, "Burger King"))
        assertEquals(listOf(listOf("CTX", "CAD")), providersOf(dbFile, burgerKingCa).map { it.take(2) })
        assertEquals(
            listOf(listOf("CTX", "USD"), listOf("PiggyCards", "USD")),
            providersOf(dbFile, burgerKingUs).map { it.take(2) }
        )
    }

    @Test
    fun v5GivesUsGiftCardRowsTheRowsOfV4() {
        val merchants = merge()
        val columns = MERCHANT_COLUMNS.removePrefix("id, ")

        assertEquals(
            dump(write(ExploreVersion.V4, merchants), "merchant", columns, "paymentMethod = 'gift card'"),
            dump(write(ExploreVersion.V5, merchants), "merchant", columns, "country = 'US' AND currency = 'USD'")
        )
    }

    @Test
    fun v5KeepsDcgRowsWithoutCountryOrCurrency() {
        val dbFile = write(ExploreVersion.V5)

        assertEquals(
            listOf(listOf("4", null, null)),
            query(dbFile, "SELECT count(*), max(country), max(currency) FROM merchant WHERE source = 'DCG'")
        )
        val incomplete = "SELECT count(*) FROM merchant WHERE paymentMethod = 'gift card' " +
            "AND (country IS NULL OR currency IS NULL)"
        assertEquals("0", query(dbFile, incomplete)[0][0])
    }

    @Test
    fun v5AttachesPiggyCardsOnlyToUsMerchants() {
        val dbFile = write(ExploreVersion.V5)
        val regions = query(
            dbFile,
            "SELECT DISTINCT m.country, m.currency, p.currency FROM gift_card_providers p " +
                "JOIN merchant m ON m.merchantId = p.merchantId AND m.paymentMethod = 'gift card' " +
                "WHERE p.provider = 'PiggyCards'"
        )
        assertEquals(listOf(listOf("US", "USD", "USD")), regions)

        // Airbnb is sold by PiggyCards alone
        assertEquals(listOf("US", "USD"), merchantsNamed(dbFile, "Airbnb eGift Card").single().drop(1))
        assertEquals(listOf("US", "USD"), merchantsNamed(dbFile, "Target").single().drop(1))
    }

    @Test
    fun piggyCardsMerchantsOfAnotherCountryOrCurrencyReachNoFile() {
        val merchants = merge()
        assertEquals(
            listOf(
                "Tim Hortons (country=CA, currency=CAD)" to PiggyCardsNote.NOT_US,
                "Euro Gifts (country=US, currency=EUR)" to PiggyCardsNote.NOT_USD,
                "Amazon (country=GB, currency=GBP)" to PiggyCardsNote.NOT_US,
                "Maple Cards (country=CA, currency=USD)" to PiggyCardsNote.NOT_US
            ),
            merchants.piggyCardsNotes.map { it.toString() to it.reason }
        )
        assertEquals(mapOf("not US, left out" to 3, "not USD, left out" to 1), merchants.piggyCardsReasons)
        val summary = "PiggyCards: 14 merchants kept as US/USD, {not US, left out=3, not USD, left out=1} [Tim Hortons "
        assertEquals(summary, merchants.piggyCardsSummary.take(summary.length))
        assertNull(merchants.piggyCardsWarning)

        for (version in ExploreVersion.values()) {
            val dbFile = write(version, merchants)
            val rows = "SELECT count(*) FROM merchant " +
                "WHERE source = 'PiggyCards' AND sourceId IN (900, 901, 903, 904)"
            val providers = "SELECT count(*) FROM gift_card_providers " +
                "WHERE provider = 'PiggyCards' AND sourceId IN (900, 901, 903, 904)"
            assertEquals("0", query(dbFile, rows)[0][0])
            assertEquals("0", query(dbFile, providers)[0][0])
            val kept = "SELECT count(*) FROM gift_card_providers WHERE provider = 'PiggyCards'"
            assertEquals("13", query(dbFile, kept)[0][0])
        }
    }

    @Test
    fun piggyCardsCurrencyIsUsdUnlessItIsAnotherCurrencyCode() {
        val brand = piggyCardsData.first()
        fun regionAndReason(country: String?, currency: String?): Pair<List<String?>, String?> {
            val merchants = MerchantLocationMerger(false)
                .combineByRegion(emptyList(), listOf(brand.copy(country = country, currency = currency)))
            val kept = merchants.byVersion.getValue(ExploreVersion.V5)
            assertEquals(kept.merchants, merchants.byVersion.getValue(ExploreVersion.V4).merchants)
            assertEquals(kept.merchants.map { it.currency }, kept.giftCardProviders.map { it.currency })
            val region = kept.merchants.flatMap { listOf(it.country, it.currency) }
            return region to merchants.piggyCardsNotes.singleOrNull()?.reason
        }
        val usd = listOf<String?>("US", "USD")
        val leftOut = emptyList<String?>()

        assertEquals(usd to null, regionAndReason("US", "USD"))
        assertEquals(usd to null, regionAndReason(" us ", " usd "))
        for (currency in listOf("EUR", "cad", " gbp ")) {
            assertEquals(leftOut to PiggyCardsNote.NOT_USD, regionAndReason("US", currency), "$currency")
        }
        for (currency in listOf(null, "", "  ", "US Dollar", "$", "840", "US", "USDT", "U\$D")) {
            assertEquals(usd to PiggyCardsNote.UNRECOGNISED, regionAndReason("US", currency), "$currency")
        }
        for (currency in listOf("USD", "CAD", null, "$")) {
            assertEquals(leftOut to PiggyCardsNote.NOT_US, regionAndReason("CA", currency), "$currency")
            assertEquals(leftOut to PiggyCardsNote.NOT_US, regionAndReason(null, currency), "$currency")
        }
    }

    @Test
    fun v4MatchesMasterWhenPiggyCardsCurrenciesAreUnrecognised() {
        val currencies = listOf(null, "", "US Dollar", "$", "840")
        val unrecognised = piggyCardsData.map {
            if (it.currency == "USD") it.copy(currency = currencies[it.name!!.length % currencies.size]) else it
        }
        val merchants = MerchantLocationMerger(false).combineByRegion(ctxData, unrecognised)
        val dbFile = write(ExploreVersion.V4, merchants)

        assertEquals(14, merchants.piggyCardsReasons[PiggyCardsNote.UNRECOGNISED])
        assertEquals(golden("merchant"), dump(dbFile, "merchant", MERCHANT_COLUMNS))
        assertEquals(golden("gift_card_providers"), dump(dbFile, "gift_card_providers", PROVIDER_COLUMNS))
        val currencyOfProviders = "SELECT DISTINCT currency FROM gift_card_providers WHERE provider = 'PiggyCards'"
        assertEquals(listOf(listOf("USD")), query(write(ExploreVersion.V5, merchants), currencyOfProviders))
    }

    private fun piggyCardsWarning(usd: Int, notUsd: Int = 0, unrecognised: Int = 0, country: String = "US"): String? {
        val brand = piggyCardsData.first()
        val brands = (1..usd).map { brand.copy(name = "Usd $it", country = country, currency = "USD") } +
            (1..notUsd).map { brand.copy(name = "Euro $it", country = country, currency = "EUR") } +
            (1..unrecognised).map { brand.copy(name = "Other $it", country = country, currency = "US Dollar") }
        // a brand has one row per location
        return MerchantLocationMerger(false).combineByRegion(emptyList(), brands + brands).piggyCardsWarning
    }

    @Test
    fun warnsWhenMoreThanHalfOfTheUsPiggyCardsMerchantsAreLeftOutOrUnrecognised() {
        val leftOut = "US PiggyCards merchants have a card in another currency than USD and were left out of v4 and v5"
        val keptAsUsd = "US PiggyCards merchants have a card without a recognisable currency and were kept as USD"

        assertNull(piggyCardsWarning(usd = 3))
        assertNull(piggyCardsWarning(usd = 2, notUsd = 2))
        assertNull(piggyCardsWarning(usd = 2, unrecognised = 2))
        assertNull(piggyCardsWarning(usd = 2, notUsd = 2, unrecognised = 2))
        assertEquals(
            "!!! WARNING: 3 of 5 $leftOut - check the PiggyCards line !!!",
            piggyCardsWarning(usd = 2, notUsd = 3)
        )
        assertEquals(
            "!!! WARNING: 3 of 5 $keptAsUsd - check the PiggyCards line !!!",
            piggyCardsWarning(usd = 2, unrecognised = 3)
        )
        assertEquals(
            "!!! WARNING: 4 of 7 $keptAsUsd - check the PiggyCards line !!!",
            piggyCardsWarning(usd = 1, notUsd = 2, unrecognised = 4)
        )
        assertEquals("!!! WARNING: 1 of 1 $keptAsUsd - check the PiggyCards line !!!", piggyCardsWarning(0, 0, 1))
        // a sync for another country leaves all of them out on purpose
        assertNull(piggyCardsWarning(usd = 2, notUsd = 3, unrecognised = 4, country = "CA"))
    }

    @Test
    fun v5LeavesOutCtxMerchantsWithoutCountryOrCurrency() {
        val merchants = merge()
        val dbFile = write(ExploreVersion.V5, merchants)

        assertEquals(listOf("No Country Shop", "No Currency Shop"), merchants.skippedMerchants)
        assertEquals("0", query(dbFile, "SELECT count(*) FROM merchant WHERE name LIKE 'No C%'")[0][0])
        val skippedIds = ctxData.filter { it.name in merchants.skippedMerchants }.joinToString { "'${it.merchantId}'" }
        val skippedProviders = "SELECT count(*) FROM gift_card_providers WHERE sourceId IN ($skippedIds)"
        assertEquals("0", query(dbFile, skippedProviders)[0][0])
    }

    @Test
    fun aRunDoesNotDependOnTheRunsBeforeIt() {
        val merger = MerchantLocationMerger(false)
        val merchantIds = "SELECT DISTINCT name, merchantId FROM merchant WHERE paymentMethod = 'gift card' ORDER BY 1, 2"
        for (version in ExploreVersion.values()) {
            val columns = if (version.hasRegions) "$MERCHANT_COLUMNS, country, currency" else MERCHANT_COLUMNS
            val providerColumns = if (version.hasRegions) "$PROVIDER_COLUMNS, currency" else PROVIDER_COLUMNS
            val first = write(version, merger.combineByRegion(ctxData, piggyCardsData), "first")
            // in another order other merchants are the first of their name, which the next run must not inherit
            val reversed = write(version, merger.combineByRegion(ctxData.reversed(), piggyCardsData), "reversed")
            val second = write(version, merger.combineByRegion(ctxData, piggyCardsData), "second")

            assertNotEquals(query(first, merchantIds), query(reversed, merchantIds))
            assertEquals(dump(first, "merchant", columns), dump(second, "merchant", columns))
            assertEquals(
                dump(first, "gift_card_providers", providerColumns),
                dump(second, "gift_card_providers", providerColumns)
            )
        }
    }
}
