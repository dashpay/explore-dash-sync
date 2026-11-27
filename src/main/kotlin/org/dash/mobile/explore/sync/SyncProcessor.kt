package org.dash.mobile.explore.sync

import kotlinx.coroutines.FlowPreview
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
import java.sql.SQLException
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CheckedInputStream

@FlowPreview
class SyncProcessor(
    private val mode: OperationMode,
    private val debug: Boolean = false,
    private val offlineMode: Boolean = false,
    private val gcManager: GCManager = GCManager(mode)
) {
    companion object {
        const val BUILD = 9

        // Prevents two concurrent invocations in the same JVM (Cloud Function container)
        // from racing on /tmp files (e.g. SQLITE_READONLY_DBMOVED when one deletes
        // /tmp/explore.db while the other holds an open JDBC connection).
        private val isRunning = AtomicBoolean(false)

        // every wallet release opens this entry, whatever the version of the file
        private const val DB_NAME_IN_ZIP = "explore.db"
    }

    internal class ImportedData(
        val ctxData: List<MerchantData>,
        val piggyCardsData: List<MerchantData>,
        val dcgData: List<MerchantData>,
        val atmData: List<AtmLocation>,
        val merchants: RegionalResult
    )

    private val logger = LoggerFactory.getLogger(SyncProcessor::class.java)!!

    private val slackMessenger = SlackMessenger()

    @FlowPreview
    suspend fun syncData(workingDir: File, forceUpload: Boolean, quietMode: Boolean) =
        syncData(workingDir, forceUpload, quietMode, ::importData)

    /** [import] is given a check to call between its steps, which throws once the sync is canceled */
    internal suspend fun syncData(
        workingDir: File,
        forceUpload: Boolean,
        quietMode: Boolean,
        import: suspend (throwIfCanceled: () -> Unit) -> ImportedData
    ) {
        slackMessenger.quietMode = quietMode

        if (!isRunning.compareAndSet(false, true)) {
            slackMessenger.postSlackMessage(
                "Sync skipped: another invocation is still running in this instance - $mode",
                logger
            )
            return
        }

        val allVersions = ExploreVersion.values().joinToString(" + ") { "v${it.number}" }
        slackMessenger.postSlackMessage("### Sync started for $allVersions ($BUILD) ### - $mode", logger)

        if (offlineMode) {
            logger.notice("Offline mode: skipping all Google Cloud Storage operations")
        }

        val lockedVersions = arrayListOf<ExploreVersion>()
        try {
            for (version in ExploreVersion.values()) {
                if (offlineMode || acquireLock(version)) {
                    lockedVersions.add(version)
                }
            }
            if (lockedVersions.isEmpty()) {
                return
            }

            // the sources are fetched and merged once, then written once per version
            val data = import { lockedVersions.forEach { throwIfCanceled(it) } }
            for (version in lockedVersions.toList()) {
                publish(version, data, workingDir, forceUpload)
                releaseLock(version, lockedVersions)
            }

            slackMessenger.postSlackMessage("### Sync finished ###", logger)
        } catch (ex: InterruptedException) {
            slackMessenger.postSlackMessage("!!! Sync canceled !!!", logger)
        } catch (ex: Exception) {
            logger.error(ex.message, ex)
            slackMessenger.postSlackMessage("### Sync failed ### ${ex.message}")
        } finally {
            try {
                lockedVersions.toList().forEach { releaseLock(it, lockedVersions) }
            } finally {
                isRunning.set(false)
            }
        }
    }

    private fun releaseLock(version: ExploreVersion, lockedVersions: MutableList<ExploreVersion>) {
        lockedVersions.remove(version)
        if (!offlineMode) {
            gcManager.deleteLockFile(version)
        }
    }

    private suspend fun acquireLock(version: ExploreVersion): Boolean {
        val syncLock = gcManager.checkLock(version)
        val syncLockCreateTime = syncLock.first
        // lock expires after 120 minutes if it wasn't removed for any reason
        val lockValid = System.currentTimeMillis() < (syncLockCreateTime + TimeUnit.MINUTES.toMillis(2 * 60))
        if (lockValid) {
            slackMessenger.postSlackMessage(
                "Sync of v${version.number} already in progress (${syncLock.second}): this run does not publish it",
                logger
            )
            return false
        }
        gcManager.createLockFile(version, mode.name)
        return true
    }

    internal suspend fun publish(version: ExploreVersion, data: ImportedData, workingDir: File, forceUpload: Boolean) {
        val combinedMerchants = data.merchants.byVersion.getValue(version)
        val dbFile = createEmptyDB(workingDir, version)
        writeExploreDb(version, dbFile, data.dcgData, combinedMerchants, data.atmData)
        val locationsDbFile = createLocationsDB(workingDir, version)
        writeLocationsDb(locationsDbFile, data, combinedMerchants)

        val dbFileChecksum = calculateChecksum(dbFile)
        logger.debug("v${version.number} DB file checksum $dbFileChecksum")

        val dbZipFileName = when (mode) {
            OperationMode.PRODUCTION -> "explore-v${version.number}.zip"
            OperationMode.TESTNET -> "explore-v${version.number}-testnet.zip"
            OperationMode.DEVNET -> "explore-v${version.number}-devnet.zip"
        }

        val dbZipFile = File(workingDir, dbZipFileName)

        if (offlineMode) {
            // Offline mode: still produce the zip locally, just skip remote checksum and uploads
            val timestamp = Calendar.getInstance().timeInMillis
            val password = dbFileChecksum.toCharArray()
            compress(dbFile, dbZipFile, password, timestamp, dbFileChecksum)
            logger.notice("Offline mode: created ${dbZipFile.absolutePath}")
            return
        }

        val remoteChecksum = gcManager.remoteChecksum(dbZipFile)
        val changesDetected = dbFileChecksum != remoteChecksum

        if (changesDetected || forceUpload) {
            if (changesDetected) {
                if (remoteChecksum != null) {
                    slackMessenger.postSlackMessage(
                        "v${version.number}: changes detected ($dbFileChecksum vs $remoteChecksum) - updating",
                        logger
                    )
                } else {
                    logger.notice("v${version.number}: no remote data - uploading")
                }
            } else {
                logger.notice("v${version.number}: force upload active - updating")
            }

            throwIfCanceled(version)
            val timestamp = Calendar.getInstance().timeInMillis
            val password = dbFileChecksum.toCharArray()
            compress(dbFile, dbZipFile, password, timestamp, dbFileChecksum)

            throwIfCanceled(version)
            // upload the zipped database
            gcManager.uploadObject(dbZipFile, timestamp, dbFileChecksum)
            // copy and upload the uncompressed database
            val locationsDbChecksum = calculateChecksum(locationsDbFile)
            gcManager.uploadObject(locationsDbFile, timestamp, locationsDbChecksum)
        } else {
            logger.notice("v${version.number}: no changes were detected, updating canceled")
            slackMessenger.postSlackMessage("v${version.number}: no changes detected, updating canceled")
        }
    }

    @Throws(InterruptedException::class)
    private fun throwIfCanceled(version: ExploreVersion) {
        if (!offlineMode && gcManager.cancelRequested(version)) {
            throw InterruptedException("Sync canceled")
        }
    }

    private suspend fun importData(throwIfCanceled: () -> Unit): ImportedData {
        val ctxDataSource = CTXSpendDataSource(slackMessenger, mode, debug)
        val ctxData = ctxDataSource.getDataList()
        val ctxReport = ctxDataSource.getReport()
        throwIfCanceled()
        val piggyCardsDataSource = PiggyCardsDataSource(slackMessenger, mode, debug)
        val piggyCardsData = piggyCardsDataSource.getDataList()
        val piggyCardsReport = piggyCardsDataSource.getReport()
        throwIfCanceled()

        // Generate HTML reports
        listOf(ctxDataSource, piggyCardsDataSource).forEach { dataSource ->
            val htmlFileName = dataSource.generateHtmlFile()
            if (htmlFileName != null && !offlineMode) {
                val htmlFile = File(htmlFileName)
                if (htmlFile.exists()) {
                    logger.info("Uploading HTML file to Google Cloud: $htmlFileName")
                    gcManager.uploadObject(htmlFile, Calendar.getInstance().timeInMillis, "")
                }
            }
        }

        val report = SyncReport(listOf(ctxReport, piggyCardsReport))
        if (debug) {
            saveMerchantDataToCsv(ctxData, "ctx.csv")
            saveMerchantDataToCsv(piggyCardsData, "piggycards.csv")
        }

        val dcgData = DCGDataSource(mode != OperationMode.PRODUCTION, slackMessenger, debug).getDataList()
        throwIfCanceled()
        val atmData = CoinAtmRadarDataSource(slackMessenger, debug).getDataList()
        throwIfCanceled()

        val merchants = MerchantLocationMerger(debug).combineByRegion(ctxData, piggyCardsData)
        // the report describes the v4 (US) file
        val combinedMerchants = merchants.byVersion.getValue(ExploreVersion.V4)
        logger.info("Duplicate locations: ${combinedMerchants.matchInfo.size}")
        val merchantSet = hashSetOf<String>()
        combinedMerchants.giftCardProviders.forEach { merchantSet.add(it.merchantId!!) }
        logger.info("Total merchants: ${merchantSet.size}")
        logger.info("Total Locations: ${combinedMerchants.merchants.size}")
        report.mergedLocations = combinedMerchants.matchInfo.size
        report.totalMerchants = merchantSet.size
        report.totalLocations = combinedMerchants.merchants.size

        // Compare to the previously created database
        val previousLocationsFile = if (!offlineMode) gcManager.downloadMostRecentLocationsDb() else null
        
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

        slackMessenger.postSlackMessage(report.toString(), logger)
        slackMessenger.postSlackMessage(regionsSummary(merchants), logger)
        val skipped = merchants.skippedMerchants
        slackMessenger.postSlackMessage(
            "CTX: ${skipped.size} merchants without a country or currency left out of v4 and v5 $skipped",
            logger
        )
        slackMessenger.postSlackMessage(merchants.piggyCardsSummary, logger)
        merchants.piggyCardsWarning?.let { slackMessenger.postSlackMessage(it, logger) }
        throwIfCanceled()
        return ImportedData(ctxData, piggyCardsData, dcgData, atmData, merchants)
    }

    private fun regionsSummary(merchants: RegionalResult): String {
        val all = merchants.byVersion.getValue(ExploreVersion.V5)
        val countries = all.merchants.mapNotNull { it.country }.toSortedSet()
        return "v5: ${all.giftCardProviders.map { it.merchantId }.toSet().size} merchants, " +
            "${all.merchants.size} locations, ${countries.size} countries $countries"
    }

    @Throws(SQLException::class)
    internal fun writeExploreDb(
        version: ExploreVersion,
        dbFile: File,
        dcgData: List<MerchantData>,
        combinedMerchants: CombinedResult,
        atmData: List<AtmLocation>
    ) {
        val dbConnection = DriverManager.getConnection("jdbc:sqlite:${dbFile.path}")
        try {
            val merchantData = dcgData + combinedMerchants.merchants
            insertAll(dbConnection, MerchantData.insertStatement(version), merchantData, version)
            insertAll(
                dbConnection,
                GiftCardProvider.insertStatement(version),
                combinedMerchants.giftCardProviders.toList(),
                version
            )
            insertAll(dbConnection, AtmLocation.INSERT_STATEMENT, atmData, version)
        } catch (ex: SQLException) {
            logger.error(ex.message, ex)
            throw ex
        } finally {
            if (!dbConnection.isClosed) {
                dbConnection.close()
            }
        }
    }

    @Throws(SQLException::class)
    private fun writeLocationsDb(locationsDbFile: File, data: ImportedData, combinedMerchants: CombinedResult) {
        val locationsDbConnection = DriverManager.getConnection("jdbc:sqlite:${locationsDbFile.path}")
        try {
            createLocationsTable(locationsDbConnection, "from_providers")
            populateLocationsTable(locationsDbConnection, "from_providers", data.ctxData + data.piggyCardsData)
            createDuplicatesTable(locationsDbConnection)
            populateDuplicatesTable(
                locationsDbConnection,
                combinedMerchants.matchInfo,
                data.merchants.usCtxData,
                data.merchants.piggyCardsData
            )
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
    }

    @Throws(IOException::class)
    internal fun createEmptyDB(workingDir: File, version: ExploreVersion): File {
        val dbFile = File(workingDir, version.dbFileName)
        if (dbFile.exists()) {
            dbFile.delete()
        }
        dbFile.createNewFile()
        logger.debug("Creating empty DB ${dbFile.absolutePath}")

        val emptyDBStream = javaClass.classLoader.getResourceAsStream(version.emptyDbResource)
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
    private fun createLocationsDB(workingDir: File, version: ExploreVersion): File {
        val dateFormat = SimpleDateFormat("yyyy-MM-dd", Locale.US)
        val today = dateFormat.format(Date())
        val dbFile = File(workingDir, "locations${version.fileSuffix}-$mode-$today.db")
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

    private fun insertAll(connection: Connection, insertStatement: String, rows: List<Data>, version: ExploreVersion) {
        if (rows.isEmpty()) {
            throw IllegalStateException("No data received")
        }

        connection.prepareStatement(insertStatement).use { prepStatement ->
            rows.forEachIndexed { index, row ->
                row.transferInto(prepStatement)
                prepStatement.addBatch()

                if ((index + 1) % 128 == 0) {
                    prepStatement.executeBatch()
                }

                if ((index + 1) % 20000 == 0) {
                    throwIfCanceled(version)
                }
            }
            prepStatement.executeBatch()
        }
        logger.info("Table sync complete (${rows.size} records)")
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
        zipParameters.fileNameInZip = DB_NAME_IN_ZIP
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