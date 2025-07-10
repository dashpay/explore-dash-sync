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
import kotlin.math.*

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
            saveMerchantDataToCsv(ctxData, "ctx.csv")
            //val piggyCardsDataFlow = PiggyCardsDataSource(slackMessenger).getData(prepStatement)
            val piggyCardsData = PiggyCardsDataSource(slackMessenger).getDataList()
            saveMerchantDataToCsv(piggyCardsData, "piggycards.csv")
            val combinedMerchants = combineMerchants(listOf(ctxData, piggyCardsData))
            val combinedFlow = combinedMerchants.first.asFlow().transform { data ->
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
    ): Pair<List<MerchantData>, Collection<GiftCardProvider>> {
        if (lists.isEmpty()) return Pair(emptyList(), emptyList())
        
        val locations = mutableListOf<MerchantData>()
        val merchantInfoList = hashMapOf<String, GiftCardProvider>()
//        val locationMap = mutableMapOf<String, MerchantData>()
//        val onlineMap = mutableMapOf<String, MerchantData>()
        // Process each list in order, prioritizing earlier lists
//        lists.forEach { merchantList ->
//            merchantList.forEach { merchant ->
//                if (merchant.type == "online") {
//                    val onlineKey = createOnlineKey(merchant)
//                    if (!onlineMap.containsKey(onlineKey)) {
//                        onlineMap[onlineKey] = merchant
//                        locations.add(merchant)
//                    }
//                } else {
//                    // Only add if we haven't seen this location before (first list wins)
//                    val locationKey = createLocationKey(merchant.latitude, merchant.longitude)
//                    if (!locationMap.containsKey(locationKey)) {
//                        locationMap[locationKey] = merchant
//                        locations.add(merchant)
//                    }
//                }
//                if (!merchantInfoList.containsKey(merchant.merchantId)) {
//                    val merchantInfo = MerchantInfo(
//                        merchantId = merchant.merchantId,
//                        name = merchant.name,
//                        active = merchant.active,
//                        source = merchant.source,
//                        sourceId = merchant.sourceId,
//                        logoLocation = merchant.logoLocation,
//                        coverImage = merchant.coverImage,
//                        type = merchant.type,
//                        redeemType = merchant.redeemType,
//                        savingsPercentage = merchant.savingsPercentage,
//                        denominationsType = merchant.denominationsType
//                    )
//                    merchantInfoList.put(merchant.merchantId!!, merchantInfo)
//                }
//            }
//        }

        val matched = findMatchesAdvanced(
            lists[1],
            lists[0],
            MatchingParameters(
                maxDistance = 0.2,
                minNameSimilarity = 0.90,
                minConfidence = 0.80,
                includeAddress = true,
                ignoreZip = true,
                ignoreState = true,
                ignoreCity = true,
                ignoreName = true,
                showAllMatches = true
            )
        )

        val resultsNew = arrayListOf<MerchantData>()
        matched.forEach { match ->
            //val ctxItem = lists[0][match.ctxIndex]
            //val pgItem = lists[1][match.piggyIndex]
            // println("ctx: ${match.ctxIndex} ${ctxItem.debug()}\npg:  ${match.piggyIndex} ${pgItem.debug()}")
            if (match.ctxIndex != -1) {
                resultsNew.add(lists[0][match.ctxIndex])
            } else if (match.piggyIndex != -1) {
                resultsNew.add(lists[1][match.piggyIndex])
            } else {
                logger.error("not adding")
            }

        }
        logger.info("matched items: {}", matched.size)
        lists[0].forEachIndexed { index, ctxItem ->
            if (matched.none { it.ctxIndex == index }) {
            //if (resultsNew.none { it == ctxItem }) {
                resultsNew.add(ctxItem)
            }
        }

        lists[1].forEachIndexed { index, piggyCardsItem ->
            if (piggyCardsItem.merchantId == "18" && piggyCardsItem.address1?.contains("1 Gerald") == true) {
                logger.info("item at {} = {}, exists {}", index, piggyCardsItem, matched.any { it.piggyIndex == index })
            }
            if (matched.none { it.piggyIndex == index }) {
                resultsNew.add(piggyCardsItem)
            }
        }
        var count = 0
        lists.forEach { count += it.size }
        logger.info("combining {} -> {} & {}", count, locations.size, resultsNew.size)
        saveMerchantDataToCsv(resultsNew, "dashspend.csv")
        return Pair(resultsNew, merchantInfoList.values)
    }

    private fun createOnlineKey(merchant: MerchantData): String {
        return merchant.name ?: "no_name"
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

    data class CoordinateMatch(
        val piggyIndex: Int,
        val ctxIndex: Int,
        val distanceMiles: Double,
        val matchType: String,
        val coordinatePrecision: Int
    )

    data class MatchInfo(
        val piggyIndex: Int,
        val ctxIndex: Int,
        val distanceMiles: Double,
        val nameSimilarity: Double,
        val addressSimilarity: Double,
        val confidence: Double,
        val reasons: String,
        val cityMatch: Boolean,
        val stateMatch: Boolean,
        val geographicWarning: String
    )

    data class MatchingParameters(
        val maxDistance: Double = 0.5,
        val minNameSimilarity: Double = 0.5,
        val minConfidence: Double = 0.7,
        val includeAddress: Boolean = true,
        val showAllMatches: Boolean = false,
        val coordinatePrecision: Int = 4,
        val ignoreState: Boolean = false,
        val ignoreCity: Boolean = false,
        val ignoreZip: Boolean = false,
        val ignoreName: Boolean = false
    )

    private fun calculateConfidenceScoreNew(
        distance: Double,
        nameSim: Double,
        streetAddrSim: Double,
        piggyRow: MerchantData,
        ctxRow: MerchantData,
        ignoreName: Boolean = false,
        ignoreCity: Boolean = false,
        ignoreState: Boolean = false,
        ignoreZip: Boolean = false
    ): Double {
        val coordinateScore = when {
            distance <= 0.01 -> 1.0    // ~50 feet
            distance <= 0.03 -> 0.95   // ~150 feet
            distance <= 0.05 -> 0.85   // ~250 feet
            distance <= 0.1 -> 0.7     // ~500 feet
            distance <= 0.2 -> 0.5     // ~1000 feet
            distance <= 0.5 -> 0.3     // ~2500 feet
            else -> 0.1
        }

        val nameScore = if (ignoreName) 1.0 else nameSim
        
        val streetScore = if ((ignoreCity || ignoreState || ignoreZip) && streetAddrSim > 0) {
            streetAddrSim
        } else {
            0.0
        }

        val cityMatch = if (ignoreCity) true else citiesMatch(piggyRow.city, ctxRow.city)
        val stateMatch = if (ignoreState) true else statesMatch(piggyRow.territory, ctxRow.territory)
        val zipMatch = if (ignoreZip) true else zipCodesMatch(piggyRow.address1, ctxRow.address1)

        val confidence = if (ignoreName) {
            if (ignoreCity || ignoreState || ignoreZip) {
                coordinateScore * 0.7 + streetScore * 0.3
            } else {
                coordinateScore
            }
        } else {
            if (ignoreCity || ignoreState || ignoreZip) {
                coordinateScore * 0.5 + nameScore * 0.3 + streetScore * 0.2
            } else {
                coordinateScore * 0.6 + nameScore * 0.4
            }
        }

        var geoBonus = 0.0
        if (!ignoreCity && cityMatch) geoBonus += 0.05
        if (!ignoreState && stateMatch) geoBonus += 0.05
        if (!ignoreZip && zipMatch) geoBonus += 0.02

        var finalConfidence = minOf(confidence + geoBonus, 1.0)

        if (coordinateScore < 0.5) {
            finalConfidence = minOf(finalConfidence, 0.4)
        } else if (coordinateScore < 0.7) {
            finalConfidence = minOf(finalConfidence, 0.6)
        }

        return finalConfidence
    }

    private fun coordinatePriorityMatching(
        piggyData: List<MerchantData>,
        ctxData: List<MerchantData>,
        coordinatePrecision: Int,
        maxDistanceMiles: Double = 0.5
    ): List<CoordinateMatch> {
        val coordinateMatches = mutableListOf<CoordinateMatch>()

        val ctxCoordLookup = mutableMapOf<Pair<Double, Double>, MutableList<Int>>()
        
        ctxData.forEachIndexed { index, row ->
            val truncatedLat = truncateCoordinate(row.latitude, coordinatePrecision)
            val truncatedLon = truncateCoordinate(row.longitude, coordinatePrecision)
            
            if (truncatedLat != null && truncatedLon != null) {
                val coordKey = Pair(truncatedLat, truncatedLon)
                ctxCoordLookup.getOrPut(coordKey) { mutableListOf() }.add(index)
            }
        }

        piggyData.forEachIndexed { piggyIndex, piggyRow ->
            val truncatedLat = truncateCoordinate(piggyRow.latitude, coordinatePrecision)
            val truncatedLon = truncateCoordinate(piggyRow.longitude, coordinatePrecision)
            
            if (truncatedLat != null && truncatedLon != null) {
                val coordKey = Pair(truncatedLat, truncatedLon)
                ctxCoordLookup[coordKey]?.forEach { ctxIndex ->
                    val ctxRow = ctxData[ctxIndex]
                    
                    val distance = haversineDistance(
                        piggyRow.latitude ?: 0.0,
                        piggyRow.longitude ?: 0.0,
                        ctxRow.latitude ?: 0.0,
                        ctxRow.longitude ?: 0.0
                    )
                    
                    if (distance <= maxDistanceMiles) {
                        coordinateMatches.add(
                            CoordinateMatch(
                                piggyIndex = piggyIndex,
                                ctxIndex = ctxIndex,
                                distanceMiles = distance,
                                matchType = "COORDINATE_EXACT",
                                coordinatePrecision = coordinatePrecision
                            )
                        )
                    }
                }
            }
        }

        return coordinateMatches
    }

    private fun streetAddressSimilarity(addr1: String?, addr2: String?): Double {
        if (addr1.isNullOrBlank() || addr2.isNullOrBlank()) return 0.0

        fun extractStreetAddress(address: String): String {
            var addr = address.trim()
            
            if (addr.contains(",")) {
                val parts = addr.split(",").toMutableList()
                
                val lastPart = parts.last().trim()
                if (lastPart.replace("-", "").all { it.isDigit() } && 
                    (lastPart.length == 5 || lastPart.length == 10)) {
                    parts.removeAt(parts.size - 1)
                }
                
                if (parts.isNotEmpty()) {
                    val secondLast = parts.last().trim()
                    if (secondLast.length == 2 && secondLast.all { it.isLetter() }) {
                        parts.removeAt(parts.size - 1)
                    }
                }
                
                addr = parts.joinToString(",")
            }
            
            return addr.split(",")[0].trim()
        }

        val street1 = extractStreetAddress(addr1)
        val street2 = extractStreetAddress(addr2)

        if (street1.isBlank() || street2.isBlank()) return 0.0

        val normalized1 = street1.lowercase().replace(Regex("[^\\w\\s]"), " ")
            .replace(Regex("\\s+"), " ").trim()
        val normalized2 = street2.lowercase().replace(Regex("[^\\w\\s]"), " ")
            .replace(Regex("\\s+"), " ").trim()

        return calculateSimilarity(normalized1, normalized2)
    }

    private fun truncateCoordinate(coord: Double?, precision: Int): Double? {
        if (coord == null) return null
        val factor = 10.0.pow(precision)
        return floor(coord * factor) / factor
    }

    private fun haversineDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double {
        val earthRadius = 3959.0 // miles
        val dLat = Math.toRadians(lat2 - lat1)
        val dLon = Math.toRadians(lon2 - lon1)
        val a = sin(dLat / 2).pow(2) + cos(Math.toRadians(lat1)) * cos(Math.toRadians(lat2)) * sin(dLon / 2).pow(2)
        val c = 2 * asin(sqrt(a))
        return earthRadius * c
    }

    private fun citiesMatch(city1: String?, city2: String?): Boolean {
        if (city1.isNullOrBlank() || city2.isNullOrBlank()) return false
        return city1.trim().lowercase() == city2.trim().lowercase()
    }

    private fun statesMatch(state1: String?, state2: String?): Boolean {
        if (state1.isNullOrBlank() || state2.isNullOrBlank()) return false
        return state1.trim().lowercase() == state2.trim().lowercase()
    }

    private fun zipCodesMatch(zip1: String?, zip2: String?): Boolean {
        if (zip1.isNullOrBlank() || zip2.isNullOrBlank()) return false
        val normalized1 = zip1.trim().take(5)
        val normalized2 = zip2.trim().take(5)
        return normalized1 == normalized2
    }

    private fun calculateSimilarity(str1: String, str2: String): Double {
        val len1 = str1.length
        val len2 = str2.length
        
        if (len1 == 0 && len2 == 0) return 1.0
        if (len1 == 0 || len2 == 0) return 0.0
        
        val dp = Array(len1 + 1) { IntArray(len2 + 1) }
        
        for (i in 0..len1) dp[i][0] = i
        for (j in 0..len2) dp[0][j] = j
        
        for (i in 1..len1) {
            for (j in 1..len2) {
                val cost = if (str1[i - 1] == str2[j - 1]) 0 else 1
                dp[i][j] = minOf(
                    dp[i - 1][j] + 1,      // deletion
                    dp[i][j - 1] + 1,      // insertion
                    dp[i - 1][j - 1] + cost // substitution
                )
            }
        }
        
        val maxLen = maxOf(len1, len2)
        return 1.0 - (dp[len1][len2].toDouble() / maxLen)
    }

    private fun findMatchesAdvanced(
        piggyData: List<MerchantData>,
        ctxData: List<MerchantData>,
        parameters: MatchingParameters = MatchingParameters()
    ): List<MatchInfo> {
        logger.info("Starting coordinate-priority matching algorithm...")
        
        // STEP 1: PRIMARY COORDINATE MATCHING (always first)
        logger.info("Step 1: Finding truncated coordinate matches (precision: ${parameters.coordinatePrecision} decimal places)")
        val exactCoordinateMatches = coordinatePriorityMatching(
            piggyData, 
            ctxData, 
            parameters.coordinatePrecision, 
            parameters.maxDistance
        )
        
        val allMatches = mutableListOf<MatchInfo>()
        val matchedPiggyIndices = mutableSetOf<Int>()
        val matchedCtxIndices = mutableSetOf<Int>()
        
        // Process exact coordinate matches
        exactCoordinateMatches.forEach { coordMatch ->
            val piggyIdx = coordMatch.piggyIndex
            val ctxIdx = coordMatch.ctxIndex
            val distance = coordMatch.distanceMiles
            
            val piggyRow = piggyData[piggyIdx]
            val ctxRow = ctxData[ctxIdx]
            
            // Calculate name similarity (if not ignored)
            val nameSim = if (parameters.ignoreName) 0.0 else {
                advancedNameSimilarity(piggyRow.name, ctxRow.name)
            }
            
            // Calculate street address similarity (only when ignoring geographic components)
            val streetAddrSim = if (parameters.includeAddress && 
                (parameters.ignoreCity || parameters.ignoreState || parameters.ignoreZip)) {
                streetAddressSimilarity(piggyRow.address1, ctxRow.address1)
            } else {
                0.0
            }
            
            // Calculate confidence using new algorithm
            val confidence = calculateConfidenceScoreNew(
                distance, nameSim, streetAddrSim, piggyRow, ctxRow,
                parameters.ignoreName, parameters.ignoreCity, 
                parameters.ignoreState, parameters.ignoreZip
            )
            
            // Apply minimum thresholds
            if (!parameters.ignoreName && nameSim < parameters.minNameSimilarity) {
                return@forEach
            }
            
            if (confidence < parameters.minConfidence) {
                return@forEach
            }
            
            // Create match record
            val matchInfo = MatchInfo(
                piggyIndex = piggyIdx,
                ctxIndex = ctxIdx,
                distanceMiles = distance,
                nameSimilarity = nameSim,
                addressSimilarity = streetAddrSim,
                confidence = confidence,
                reasons = "truncated_coordinates_${parameters.coordinatePrecision}dp, coordinate_priority_match",
                cityMatch = !parameters.ignoreCity,
                stateMatch = !parameters.ignoreState,
                geographicWarning = ""
            )
            
            allMatches.add(matchInfo)
            matchedPiggyIndices.add(piggyIdx)
            matchedCtxIndices.add(ctxIdx)
        }
        
        logger.info("Found ${exactCoordinateMatches.size} truncated coordinate matches")
        
        // STEP 2: PROXIMITY MATCHING for remaining locations (only if needed)
        val remainingPiggy = piggyData.size - matchedPiggyIndices.size
        val remainingCtx = ctxData.size - matchedCtxIndices.size
        
        if (remainingPiggy > 0 && remainingCtx > 0) {
            logger.info("Step 2: Processing $remainingPiggy remaining locations")
            
            // Create filtered datasets
            val remainingPiggyData = piggyData.filterIndexed { index, _ -> 
                !matchedPiggyIndices.contains(index) 
            }
            val remainingCtxData = ctxData.filterIndexed { index, _ -> 
                !matchedCtxIndices.contains(index) 
            }
            
            // Process remaining locations with strict coordinate priority
            remainingPiggyData.forEachIndexed { piggyIdx, piggyRow ->
                if (piggyRow.latitude == null || piggyRow.longitude == null) {
                    return@forEachIndexed
                }
                
                // Very restrictive spatial filtering for coordinate priority
                val nearbyCtx = spatialIndexFilter(
                    piggyRow.latitude!!, 
                    piggyRow.longitude!!, 
                    remainingCtxData, 
                    parameters.maxDistance
                )
                
                if (nearbyCtx.isEmpty()) {
                    return@forEachIndexed
                }
                
                val locationMatches = mutableListOf<MatchInfo>()
                
                nearbyCtx.forEach { ctxRow ->
                    val distance = haversineDistance(
                        piggyRow.latitude!!, piggyRow.longitude!!,
                        ctxRow.latitude ?: 0.0, ctxRow.longitude ?: 0.0
                    )
                    
                    if (distance > parameters.maxDistance) {
                        return@forEach
                    }
                    
                    // Name similarity (if not ignored)
                    val nameSim = if (parameters.ignoreName) 0.0 else {
                        advancedNameSimilarity(piggyRow.name, ctxRow.name)
                    }
                    
                    // Apply name similarity threshold (if not ignored)
                    if (!parameters.ignoreName && nameSim < parameters.minNameSimilarity) {
                        return@forEach
                    }
                    
                    // Street address similarity (only when ignoring geographic components)
                    val streetAddrSim = if (parameters.includeAddress && 
                        (parameters.ignoreCity || parameters.ignoreState || parameters.ignoreZip)) {
                        streetAddressSimilarity(piggyRow.address1, ctxRow.address1)
                    } else {
                        0.0
                    }
                    
                    // Calculate confidence with coordinate priority
                    val confidence = calculateConfidenceScoreNew(
                        distance, nameSim, streetAddrSim, piggyRow, ctxRow,
                        parameters.ignoreName, parameters.ignoreCity, 
                        parameters.ignoreState, parameters.ignoreZip
                    )
                    
                    if (confidence < parameters.minConfidence) {
                        return@forEach
                    }
                    
                    // Create match record
                    val matchInfo = MatchInfo(
                        piggyIndex = piggyData.indexOf(piggyRow),
                        ctxIndex = ctxData.indexOf(ctxRow),
                        distanceMiles = distance,
                        nameSimilarity = nameSim,
                        addressSimilarity = streetAddrSim,
                        confidence = confidence,
                        reasons = "coordinate_priority_proximity, distance_${String.format("%.3f", distance)}mi",
                        cityMatch = !parameters.ignoreCity,
                        stateMatch = !parameters.ignoreState,
                        geographicWarning = ""
                    )
                    // (matchInfo)
                    locationMatches.add(matchInfo)
                }
                
                // Sort by confidence and add matches
                if (locationMatches.isNotEmpty()) {
                    locationMatches.sortByDescending { it.confidence }
                    
                    if (parameters.showAllMatches) {
                        allMatches.addAll(locationMatches)
                    } else {
                        allMatches.add(locationMatches[0])
                    }
                }
            }
        }
        
        logger.info("Total matches found: ${allMatches.size}")
        return allMatches
    }

    private fun spatialIndexFilter(
        lat: Double, 
        lon: Double, 
        ctxData: List<MerchantData>, 
        maxDistance: Double
    ): List<MerchantData> {
        return ctxData.filter { ctxRow ->
            if (ctxRow.latitude == null || ctxRow.longitude == null) {
                false
            } else {
                val distance = haversineDistance(lat, lon, ctxRow.latitude!!, ctxRow.longitude!!)
                distance <= maxDistance
            }
        }
    }

    private fun advancedNameSimilarity(name1: String?, name2: String?): Double {
        if (name1.isNullOrBlank() || name2.isNullOrBlank()) return 0.0
        
        val normalized1 = name1.trim().lowercase()
        val normalized2 = name2.trim().lowercase()
        
        return calculateSimilarity(normalized1, normalized2)
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
