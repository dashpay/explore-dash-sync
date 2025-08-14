package org.dash.mobile.explore.sync

import org.dash.mobile.explore.sync.process.MerchantNameNormalizer
import org.dash.mobile.explore.sync.process.data.MerchantData
import org.dash.mobile.explore.sync.process.data.GiftCardProvider
import org.dash.mobile.explore.sync.utils.CSVExporter.saveMerchantDataToCsv
import org.slf4j.LoggerFactory
import java.util.Locale
import kotlin.math.*

data class CombinedResult(
    val merchants: List<MerchantData>,
    val giftCardProviders: Collection<GiftCardProvider>,
    val matchInfo: List<MerchantLocationMerger.MatchInfo>
)

class MerchantLocationMerger(private val debug: Boolean) {
    private val logger = LoggerFactory.getLogger(MerchantLocationMerger::class.java)!!

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
        val minNameSimilarity: Double = 0.9,
        val minConfidence: Double = 0.7,
        val includeAddress: Boolean = true,
        val showAllMatches: Boolean = false,
        val coordinatePrecision: Int = 4,
        val ignoreState: Boolean = false,
        val ignoreCity: Boolean = false,
        val ignoreZip: Boolean = false,
        val ignoreName: Boolean = false
    )

    fun combineMerchants(
        lists: List<List<MerchantData>>,
        matchingParameters: MatchingParameters = MatchingParameters(
            maxDistance = 0.2,
            minNameSimilarity = 0.9,
            minConfidence = 0.80,
            includeAddress = true,
            showAllMatches = true,
            coordinatePrecision = 4,
            ignoreState = true,
            ignoreCity = true,
            ignoreZip = true,
            ignoreName = false
        )
    ): CombinedResult {
        if (lists.isEmpty()) return CombinedResult(emptyList(), emptyList(), emptyList())
        val merchantProviderMap = mutableMapOf<String, GiftCardProvider>() // Key: merchantId_provider
        
        if (lists.size == 1) {
            lists.first().forEach {
                addGiftcardProvider(it, it.merchantId!!, merchantProviderMap)
            }
            return CombinedResult(lists.first(), merchantProviderMap.values, emptyList())
        }

        val matched = findMatchesAdvanced(
            lists[1],
            lists[0],
            matchingParameters
        )

        val resultsNew = arrayListOf<MerchantData>()
        matched.forEach { match ->
            val ctxData = lists[0][match.ctxIndex]
            val piggyCardsData = lists[1][match.piggyIndex]
            val maxSavings = max(ctxData.savingsPercentage ?: 0, piggyCardsData.savingsPercentage ?: 0)
            val mergedData = ctxData.copy(
                merchantId = ctxData.merchantId,
                savingsPercentage = maxSavings,
                plusCode = "merged"
            )
            
            MerchantNameNormalizer.add(mergedData.name, mergedData.logoLocation, ctxData.merchantId)
            
            // Add GiftCardProvider entries for both CTX and PiggyCards
            addGiftcardProvider(ctxData, ctxData.merchantId!!, merchantProviderMap)
            addGiftcardProvider(
                piggyCardsData.copy(merchantId = ctxData.merchantId),
                piggyCardsData.merchantId!!,
                merchantProviderMap
            )

            resultsNew.add(mergedData)
        }
        
        logger.info("matched items: {}", matched.size)
        logger.info("Merchants with duplicates between CTX and PiggyCards")
        resultsNew.map { it.name }.toSet().forEach {
            logger.info("  $it")
        }
        logger.info("Merchants with duplicates between CTX and PiggyCards from matches")
        matched.map { lists[0][it.ctxIndex].name }.toSet().forEach {
            logger.info("  $it")
        }
        if (debug) {
            saveMerchantDataToCsv(resultsNew, "dashspend-matched.csv")
        }
        lists[0].forEachIndexed { index, ctxItem ->
            if (matched.none { it.ctxIndex == index }) {
                MerchantNameNormalizer.add(ctxItem.name, ctxItem.logoLocation, ctxItem.merchantId)
                val newItem = ctxItem.copy(
                    merchantId = MerchantNameNormalizer.getUniqueId(ctxItem.name!!),
                    name = MerchantNameNormalizer.getNormalizedName(ctxItem.name),
                    logoLocation = MerchantNameNormalizer.getLogo(ctxItem.name)
                )
                resultsNew.add(newItem)
                addGiftcardProvider(newItem, ctxItem.merchantId!!, merchantProviderMap)
            }
        }

        lists[1].forEachIndexed { index, piggyCardsItem ->
            if (matched.none { it.piggyIndex == index }) {
                MerchantNameNormalizer.add(piggyCardsItem.name, piggyCardsItem.logoLocation, null)
                val newItem: MerchantData = piggyCardsItem.copy(
                    name = MerchantNameNormalizer.getNormalizedName(piggyCardsItem.name),
                    merchantId = MerchantNameNormalizer.getUniqueId(piggyCardsItem.name!!),
                    logoLocation = MerchantNameNormalizer.getLogo(piggyCardsItem.name)
                )
                if (piggyCardsItem.type == "online") {
                    val existingOnlineItem = resultsNew.find {
                        it.name == MerchantNameNormalizer.getNormalizedName(piggyCardsItem.name) &&
                        it.type == "online"
                    }
                    if (existingOnlineItem != null) {
                        // Update existing online merchant with max savings
                        val maxSavings = max(
                            piggyCardsItem.savingsPercentage ?: 0,
                            existingOnlineItem.savingsPercentage ?: 0
                        )
                        resultsNew.remove(existingOnlineItem)
                        resultsNew.add(existingOnlineItem.copy(
                            name = MerchantNameNormalizer.getNormalizedName(piggyCardsItem.name),
                            merchantId = MerchantNameNormalizer.getUniqueId(piggyCardsItem.name!!),
                            logoLocation = MerchantNameNormalizer.getLogo(piggyCardsItem.name),
                            savingsPercentage = maxSavings
                        ))
                        // Add PiggyCards provider entry with the same merchantId
                        addGiftcardProvider(
                            newItem.copy(merchantId = existingOnlineItem.merchantId),
                            piggyCardsItem.merchantId!!,
                            merchantProviderMap
                        )
                    } else {
                        resultsNew.add(newItem)
                        addGiftcardProvider(newItem, piggyCardsItem.merchantId!!, merchantProviderMap)
                    }
                } else {
                    resultsNew.add(newItem)
                    addGiftcardProvider(newItem, piggyCardsItem.merchantId!!, merchantProviderMap)
                }
            }
        }
        
        var count = 0
        lists.forEach { count += it.size }
        logger.info("combining {} -> {}", count,  resultsNew.size)
        if (debug) {
            saveMerchantDataToCsv(resultsNew, "dashspend.csv")
        }
        // Deduplicate by composite key: normalized name + 4dp lat/lon + type
        val seen = HashSet<String>()
        val deduped = resultsNew.filter { m ->
            val normName = MerchantNameNormalizer.getNormalizedName(m.name) ?: ""
            val lat = m.latitude?.let { "%.4f".format(Locale.US, it) } ?: "null"
            val lon = m.longitude?.let { "%.4f".format(Locale.US, it) } ?: "null"
            val key = "$normName|$lat|$lon|${m.type}"
            val result = seen.add(key)
            if (!result) {
                logger.info("  non-matched duplicate found: {}, ({}, {}) from {}", normName, lat, lon, m.source)
            }
            result
        }
        logger.info("deduped count: {} vs original count: {}", deduped.size, resultsNew.size)
        return CombinedResult(resultsNew, merchantProviderMap.values, matched)
    }

    private fun addGiftcardProvider(
        merchant: MerchantData,
        merchantId: String,
        merchantProviderMap: MutableMap<String, GiftCardProvider>
    ) {
        if (merchant.merchantId == null || merchant.source == null) return
        
        val providerKey = "${merchant.merchantId}_${merchant.source}"
        
        val merchantInfo = GiftCardProvider(
            merchantId = merchant.merchantId,
            active = merchant.active,
            provider = merchant.source,
            sourceId = merchantId,
            redeemType = merchant.redeemType,
            savingsPercentage = merchant.savingsPercentage,
            denominationsType = merchant.denominationsType
        )
        merchantProviderMap[providerKey] = merchantInfo
    }

    private fun createOnlineKey(merchant: MerchantData): String {
        return merchant.name ?: "no_name"
    }

    private fun createLocationKey(latitude: Double?, longitude: Double?): String {
        if (latitude == null || longitude == null) {
            return "null_coords_${System.identityHashCode(latitude)}_${System.identityHashCode(longitude)}"
        }
        
        val roundedLat = "%.4f".format(latitude)
        val roundedLng = "%.4f".format(longitude)
        
        return "${roundedLat}_${roundedLng}"
    }

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
        maxDistanceMiles: Double = 0.5,
        ignoreName: Boolean,
        nameSimularity: Double
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

                    val thisNameSimilarity = advancedNameSimilarity(ctxRow.name, piggyRow.name)
                    val meetsNameRequirements = if (!ignoreName) thisNameSimilarity >= nameSimularity else true

                    if (distance <= maxDistanceMiles && meetsNameRequirements) {
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
            parameters.maxDistance,
            parameters.ignoreName,
            parameters.minNameSimilarity
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
                        reasons = "coordinate_priority_proximity, distance_${String.format(Locale.US, "%.3f", distance)}mi",
                        cityMatch = !parameters.ignoreCity,
                        stateMatch = !parameters.ignoreState,
                        geographicWarning = ""
                    )
                    
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
        
        val normalized1 = MerchantNameNormalizer.removeSuffix(name1).lowercase()
        val normalized2 = MerchantNameNormalizer.removeSuffix(name2).lowercase()
        
        return calculateSimilarity(normalized1, normalized2)
    }
}