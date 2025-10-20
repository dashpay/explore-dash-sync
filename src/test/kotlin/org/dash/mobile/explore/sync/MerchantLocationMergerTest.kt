package org.dash.mobile.explore.sync

import org.dash.mobile.explore.sync.process.data.MerchantData
import org.dash.mobile.explore.sync.utils.CSVExporter.saveMerchantDataToCsv
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path

class MerchantLocationMergerTest {

    private lateinit var merger: MerchantLocationMerger
    
    @TempDir
    lateinit var tempDir: Path

    @BeforeEach
    fun setUp() {
        merger = MerchantLocationMerger(false)
    }

    @Test
    fun testCombineMerchantsWithCsvFiles() {
        // Load CSV data from test resources
        val ctxData = loadMerchantDataFromCsv("ctx.csv")
        val piggyCardsData = loadMerchantDataFromCsv("piggycards.csv")
        
        // Combine merchants
        val result = merger.combineMerchants(listOf(ctxData, piggyCardsData))
        val combinedMerchants = result.merchants
        
        // Save result to project root directory
        //val outputFile = tempDir.resolve("actual_output.csv")
        val outputFile = File("actual_output.csv")
        saveMerchantDataToCsv(combinedMerchants, outputFile.absolutePath)
        
        // Load expected result
        val expectedData = loadMerchantDataFromCsv("dashspend.csv")
        
        // Compare sizes
        assertNotNull(combinedMerchants)
        assertTrue(combinedMerchants.isNotEmpty(), "Combined merchants should not be empty")
        
        // Verify that we have merchants from both sources
        val ctxMerchants = combinedMerchants.filter { it.source == "CTX" }
        val piggyMerchants = combinedMerchants.filter { it.source == "PiggyCards" }
        
        assertTrue(ctxMerchants.isNotEmpty(), "Should have CTXSpend merchants")
        assertTrue(piggyMerchants.isNotEmpty(), "Should have PiggyCards merchants")
        
        // Verify total count is reasonable (should be less than or equal to sum of both sources)
        val totalInputMerchants = ctxData.size + piggyCardsData.size
        assertTrue(combinedMerchants.size <= totalInputMerchants, 
            "Combined count (${combinedMerchants.size}) should not exceed total inputs ($totalInputMerchants)")
        
        // Compare with expected output structure
        if (expectedData.isNotEmpty()) {
            // Verify similar data structure
            val firstExpected = expectedData.first()
            val firstActual = combinedMerchants.first()
            
            // Both should have same fields populated
            assertNotNull(firstActual.source)
            assertNotNull(firstActual.name)
            assertNotNull(firstActual.paymentMethod)
            assertEquals(expectedData.size, combinedMerchants.size, findMerchantDataDifferences(expectedData, combinedMerchants, "all").toString())
        }
        
        println("Test completed successfully:")
        println("  CTX merchants: ${ctxData.size}")
        println("  PiggyCards merchants: ${piggyCardsData.size}")
        println("  Combined merchants: ${combinedMerchants.size}")
        println("  Expected merchants: ${expectedData.size}")
        println("  CTX in result: ${ctxMerchants.size}")
        println("  PiggyCards in result: ${piggyMerchants.size}")
    }

    @Test
    fun testCombineMerchantsWithEmptyLists() {
        val result = merger.combineMerchants(emptyList())
        assertEquals(0, result.merchants.size)
        assertEquals(0, result.matchInfo.size)
    }

    @Test
    fun testCombineMerchantsWithSingleList() {
        val ctxData = loadMerchantDataFromCsv("ctx.csv").take(5)
        
        val result = merger.combineMerchants(listOf(ctxData))
        
        // Should return the single list as-is
        assertTrue(result.merchants.isNotEmpty())
        assertEquals(ctxData.size, result.merchants.size)
    }

    @Test
    fun testHaversineDistance() {
        // Test known distance between two points
        // Distance between NYC (40.7128, -74.0060) and LA (34.0522, -118.2437) is approximately 2445 miles
        val distance = merger.haversineDistance(40.7128, -74.0060, 34.0522, -118.2437)
        
        assertTrue(distance > 2400 && distance < 2500, "Distance should be approximately 2445 miles, got $distance")
    }

    @Test
    fun testStreetAddressSimilarity() {
        // Test address similarity with same street
        val similarity1 = merger.streetAddressSimilarity("123 Main St", "123 Main Street")
        assertTrue(similarity1 > 0.7, "Similar addresses should have high similarity")
        
        // Test completely different addresses
        val similarity2 = merger.streetAddressSimilarity("123 Main St", "456 Oak Ave")
        assertTrue(similarity2 < 0.5, "Different addresses should have low similarity")
        
        // Test null/empty addresses
        val similarity3 = merger.streetAddressSimilarity(null, "123 Main St")
        assertEquals(0.0, similarity3, "Null address should return 0.0 similarity")
    }

    @Test
    fun testCalculateSimilarity() {
        // Test exact match
        val exactMatch = merger.calculateSimilarity("test", "test")
        assertEquals(1.0, exactMatch, 0.001)
        
        // Test no match
        val noMatch = merger.calculateSimilarity("abc", "xyz")
        assertTrue(noMatch < 1.0)
        
        // Test empty strings
        val emptyMatch = merger.calculateSimilarity("", "")
        assertEquals(1.0, emptyMatch, 0.001)
    }

    @Test
    fun testAdvancedNameSimilarity() {
        // Test exact match (case insensitive)
        val exactMatch = merger.advancedNameSimilarity("McDonald's", "McDonald's")
        assertEquals(1.0, exactMatch, 0.001)
        
        // Test case insensitive match
        val caseMatch = merger.advancedNameSimilarity("McDONALD'S", "mcdonald's")
        assertEquals(1.0, caseMatch, 0.001)
        
        // Test similar names
        val similarMatch = merger.advancedNameSimilarity("Starbucks", "Starbucks Coffee")
        assertTrue(similarMatch > 0.5, "Similar names should have high similarity")
        
        // Test whitespace handling
        val whitespaceMatch = merger.advancedNameSimilarity("  Target  ", "Target")
        assertEquals(1.0, whitespaceMatch, 0.001)
        
        // Test completely different names
        val differentMatch = merger.advancedNameSimilarity("Apple Store", "Best Buy")
        assertTrue(differentMatch < 0.5, "Different names should have low similarity")
        
        // Test null/empty names
        val nullMatch = merger.advancedNameSimilarity(null, "Target")
        assertEquals(0.0, nullMatch, 0.001)
        
        val emptyMatch = merger.advancedNameSimilarity("", "Target")
        assertEquals(0.0, emptyMatch, 0.001)
        
        val bothNullMatch = merger.advancedNameSimilarity(null, null)
        assertEquals(0.0, bothNullMatch, 0.001)
        
        // Test real-world examples
        val realWorldMatch1 = merger.advancedNameSimilarity("Walmart Supercenter", "Walmart")
        assertTrue(realWorldMatch1 > 0.3, "Walmart variations should match well")
        
        val realWorldMatch2 = merger.advancedNameSimilarity("Home Depot", "The Home Depot")
        assertTrue(realWorldMatch2 > 0.7, "Home Depot variations should match well")
        
        val realWorldMatch3 = merger.advancedNameSimilarity("CVS Pharmacy", "CVS")
        assertTrue(realWorldMatch3 > 0.2, "CVS variations should have decent similarity")
    }

    @Test
    fun testFindListDifferences() {
        // Create test data
        val list1 = listOf(
            MerchantData().apply { merchantId = "1"; name = "McDonald's" },
            MerchantData().apply { merchantId = "2"; name = "Starbucks" },
            MerchantData().apply { merchantId = "3"; name = "Walmart" }
        )
        
        val list2 = listOf(
            MerchantData().apply { merchantId = "2"; name = "Starbucks" },
            MerchantData().apply { merchantId = "3"; name = "Walmart" },
            MerchantData().apply { merchantId = "4"; name = "Target" }
        )
        
        // Test by merchantId (default)
        val (onlyInList1, onlyInList2) = findListDifferences(list1, list2)
        
        assertEquals(1, onlyInList1.size, "Should find 1 item only in list1")
        assertEquals("1", onlyInList1[0].merchantId, "Should find McDonald's only in list1")
        
        assertEquals(1, onlyInList2.size, "Should find 1 item only in list2")  
        assertEquals("4", onlyInList2[0].merchantId, "Should find Target only in list2")
    }

    @Test
    fun testFindMerchantDataDifferencesByName() {
        // Create test data with same names but different IDs
        val list1 = listOf(
            MerchantData().apply { merchantId = "ctx-1"; name = "McDonald's" },
            MerchantData().apply { merchantId = "ctx-2"; name = "Starbucks" }
        )
        
        val list2 = listOf(
            MerchantData().apply { merchantId = "pc-1"; name = "mcdonald's" }, // Same name, different case
            MerchantData().apply { merchantId = "pc-3"; name = "Target" }
        )
        
        // Test by name - should find McDonald's as duplicate due to case-insensitive comparison
        val (onlyInList1, onlyInList2) = findMerchantDataDifferences(list1, list2, "name")
        
        assertEquals(1, onlyInList1.size, "Should find 1 item only in list1")
        assertEquals("Starbucks", onlyInList1[0].name, "Should find Starbucks only in list1")
        
        assertEquals(1, onlyInList2.size, "Should find 1 item only in list2")
        assertEquals("Target", onlyInList2[0].name, "Should find Target only in list2")
    }

    @Test
    fun testFindListDifferencesWithCustomKeySelector() {
        val list1 = listOf("apple", "banana", "cherry")
        val list2 = listOf("BANANA", "cherry", "date")
        
        // Test with case-insensitive comparison
        val (onlyInList1, onlyInList2) = findListDifferences(list1, list2) { it.lowercase() }
        
        assertEquals(1, onlyInList1.size, "Should find 1 item only in list1") 
        assertEquals("apple", onlyInList1[0], "Should find 'apple' only in list1")
        
        assertEquals(1, onlyInList2.size, "Should find 1 item only in list2")
        assertEquals("date", onlyInList2[0], "Should find 'date' only in list2")
    }

    // Helper function to access private methods for testing
    private fun MerchantLocationMerger.haversineDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double {
        // Use reflection to access private method
        val method = MerchantLocationMerger::class.java.getDeclaredMethod(
            "haversineDistance", Double::class.java, Double::class.java, Double::class.java, Double::class.java)
        method.isAccessible = true
        return method.invoke(this, lat1, lon1, lat2, lon2) as Double
    }

    private fun MerchantLocationMerger.streetAddressSimilarity(addr1: String?, addr2: String?): Double {
        val method = MerchantLocationMerger::class.java.getDeclaredMethod(
            "streetAddressSimilarity", String::class.java, String::class.java)
        method.isAccessible = true
        return method.invoke(this, addr1, addr2) as Double
    }

    private fun MerchantLocationMerger.calculateSimilarity(str1: String, str2: String): Double {
        val method = MerchantLocationMerger::class.java.getDeclaredMethod(
            "calculateSimilarity", String::class.java, String::class.java)
        method.isAccessible = true
        return method.invoke(this, str1, str2) as Double
    }

    private fun MerchantLocationMerger.advancedNameSimilarity(name1: String?, name2: String?): Double {
        val method = MerchantLocationMerger::class.java.getDeclaredMethod(
            "advancedNameSimilarity", String::class.java, String::class.java)
        method.isAccessible = true
        return method.invoke(this, name1, name2) as Double
    }

    private fun loadMerchantDataFromCsv(fileName: String): List<MerchantData> {
        val resource = javaClass.classLoader.getResource(fileName)
            ?: throw IllegalArgumentException("Resource $fileName not found")
        
        val lines = File(resource.toURI()).readLines()
        if (lines.isEmpty()) return emptyList()
        
        val header = lines[0].split(",")
        val merchants = mutableListOf<MerchantData>()
        
        for (i in 1 until lines.size) {
            val values = parseCsvLine(lines[i])
            if (values.size >= header.size) {
                val merchant = MerchantData().apply {
                    deeplink = getValueOrNull(values, header, "deeplink")
                    plusCode = getValueOrNull(values, header, "plusCode")
                    addDate = getValueOrNull(values, header, "addDate")
                    updateDate = getValueOrNull(values, header, "updateDate")
                    paymentMethod = getValueOrNull(values, header, "paymentMethod")
                    merchantId = getValueOrNull(values, header, "merchantId")
                    id = getValueOrNull(values, header, "id")?.toIntOrNull()
                    active = getValueOrNull(values, header, "active")?.toBooleanStrictOrNull()
                    name = getValueOrNull(values, header, "name")
                    address1 = getValueOrNull(values, header, "address1")
                    address2 = getValueOrNull(values, header, "address2")
                    address3 = getValueOrNull(values, header, "address3")
                    address4 = getValueOrNull(values, header, "address4")
                    latitude = getValueOrNull(values, header, "latitude")?.toDoubleOrNull()
                    longitude = getValueOrNull(values, header, "longitude")?.toDoubleOrNull()
                    website = getValueOrNull(values, header, "website")
                    phone = getValueOrNull(values, header, "phone")
                    territory = getValueOrNull(values, header, "territory")
                    city = getValueOrNull(values, header, "city")
                    source = getValueOrNull(values, header, "source")
                    sourceId = getValueOrNull(values, header, "sourceId")
                    logoLocation = getValueOrNull(values, header, "logoLocation")
                    googleMaps = getValueOrNull(values, header, "googleMaps")
                    coverImage = getValueOrNull(values, header, "coverImage")
                    type = getValueOrNull(values, header, "type")
                    redeemType = getValueOrNull(values, header, "redeemType")
                    savingsPercentage = getValueOrNull(values, header, "savingsPercentage")?.toIntOrNull()
                    denominationsType = getValueOrNull(values, header, "denominationsType")
                    instagram = getValueOrNull(values, header, "instagram")
                    twitter = getValueOrNull(values, header, "twitter")
                    delivery = getValueOrNull(values, header, "delivery")
                    monOpen = getValueOrNull(values, header, "monOpen")
                    monClose = getValueOrNull(values, header, "monClose")
                    tueOpen = getValueOrNull(values, header, "tueOpen")
                    tueClose = getValueOrNull(values, header, "tueClose")
                    wedOpen = getValueOrNull(values, header, "wedOpen")
                    wedClose = getValueOrNull(values, header, "wedClose")
                    thuOpen = getValueOrNull(values, header, "thuOpen")
                    thuClose = getValueOrNull(values, header, "thuClose")
                    friOpen = getValueOrNull(values, header, "friOpen")
                    friClose = getValueOrNull(values, header, "friClose")
                    satOpen = getValueOrNull(values, header, "satOpen")
                    satClose = getValueOrNull(values, header, "satClose")
                    sunOpen = getValueOrNull(values, header, "sunOpen")
                    sunClose = getValueOrNull(values, header, "sunClose")
                }
                merchants.add(merchant)
            }
        }
        
        return merchants
    }

    private fun parseCsvLine(line: String): List<String> {
        val result = mutableListOf<String>()
        var current = StringBuilder()
        var inQuotes = false
        var i = 0
        
        while (i < line.length) {
            val char = line[i]
            when {
                char == '"' && (i == 0 || line[i-1] == ',') -> {
                    inQuotes = true
                }
                char == '"' && inQuotes && (i + 1 < line.length && line[i+1] == '"') -> {
                    current.append('"')
                    i++ // Skip next quote
                }
                char == '"' && inQuotes -> {
                    inQuotes = false
                }
                char == ',' && !inQuotes -> {
                    result.add(current.toString())
                    current = StringBuilder()
                }
                else -> {
                    current.append(char)
                }
            }
            i++
        }
        result.add(current.toString())
        
        return result
    }

    private fun getValueOrNull(values: List<String>, header: List<String>, fieldName: String): String? {
        val index = header.indexOf(fieldName)
        if (index == -1 || index >= values.size) return null
        val value = values[index].trim()
        return if (value.isEmpty()) null else value
    }

    /**
     * Efficiently finds differences between two lists of MerchantData using HashMap lookups.
     * Time complexity: O(n + m) where n and m are the sizes of the input lists.
     * Space complexity: O(n + m) for the HashMaps.
     *
     * @param list1 First list of MerchantData
     * @param list2 Second list of MerchantData  
     * @param keySelector Function to extract a unique key from MerchantData (defaults to merchantId)
     * @return Pair of (items only in list1, items only in list2)
     */
    fun <T> findListDifferences(
        list1: List<T>, 
        list2: List<T>, 
        keySelector: (T) -> String? = { 
            when (it) {
                is MerchantData -> it.merchantId
                else -> it.toString()
            }
        }
    ): Pair<List<T>, List<T>> {
        // Create HashMaps for O(1) lookup - filter out null keys
        val map1 = list1.mapNotNull { item ->
            keySelector(item)?.let { key -> key to item }
        }.toMap()
        
        val map2 = list2.mapNotNull { item ->
            keySelector(item)?.let { key -> key to item }
        }.toMap()
        
        // Find items only in list1 (not in list2)
        val onlyInList1 = list1.filter { item ->
            val key = keySelector(item)
            key != null && !map2.containsKey(key)
        }
        
        // Find items only in list2 (not in list1) 
        val onlyInList2 = list2.filter { item ->
            val key = keySelector(item)
            key != null && !map1.containsKey(key)
        }
        
        return Pair(onlyInList1, onlyInList2)
    }

    /**
     * Specialized version for MerchantData that compares by multiple criteria
     * for more accurate duplicate detection.
     * 
     * @param list1 First list of MerchantData
     * @param list2 Second list of MerchantData
     * @param compareBy Comparison strategy: "merchantId", "name", "location", or "composite"
     * @return Pair of (items only in list1, items only in list2)
     */
    fun findMerchantDataDifferences(
        list1: List<MerchantData>,
        list2: List<MerchantData>,
        compareBy: String = "composite"
    ): Pair<List<MerchantData>, List<MerchantData>> {
        val keySelector: (MerchantData) -> String? = when (compareBy) {
            "merchantId" -> { merchant -> merchant.merchantId }
            "name" -> { merchant -> merchant.name?.lowercase()?.trim() }
            "location" -> { merchant -> 
                "${merchant.latitude}_${merchant.longitude}"
            }
            "composite" -> { merchant ->
                // Composite key: name + approximate location (rounded to avoid floating point issues)
                val lat = merchant.latitude?.let { "%.4f".format(it) } ?: "null"
                val lon = merchant.longitude?.let { "%.4f".format(it) } ?: "null"
                "${merchant.name?.lowercase()?.trim()}_${lat}_${lon}"
            }
            "all" -> { merchant -> merchant.toString() }
            else -> { merchant -> merchant.merchantId }
        }
        
        return findListDifferences(list1, list2, keySelector)
    }
}