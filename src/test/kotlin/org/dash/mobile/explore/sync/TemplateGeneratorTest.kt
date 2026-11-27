package org.dash.mobile.explore.sync

import com.google.gson.JsonParser
import org.dash.mobile.explore.sync.RegionFixtures.query
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class TemplateGeneratorTest {

    @TempDir
    lateinit var tempDir: File

    private fun shipped(version: ExploreVersion): File {
        val dbFile = File(tempDir, "shipped-${version.emptyDbResource}")
        javaClass.classLoader.getResourceAsStream(version.emptyDbResource)!!.use { input ->
            dbFile.outputStream().use { input.copyTo(it) }
        }
        return dbFile
    }

    private fun generated(version: ExploreVersion): File {
        val dbFile = File(tempDir, "generated-${version.number}.db")
        TemplateGenerator.generate(File("schemas/${version.number}.json"), dbFile)
        return dbFile
    }

    /** Everything a wallet can see of the schema; the order of columns and the wording of the SQL are left out */
    private fun describe(dbFile: File): List<String> {
        val objects = query(dbFile, "SELECT type, name, tbl_name FROM sqlite_master ORDER BY type, name")
        val columns = objects.filter { it[0] == "table" }.flatMap { table ->
            val info = "SELECT name, type, \"notnull\", dflt_value, pk FROM pragma_table_info('${table[1]}') ORDER BY name"
            query(dbFile, info).map { "column ${table[1]}.$it" }
        }
        val indexColumns = objects.filter { it[0] == "index" }.flatMap { index ->
            query(dbFile, "SELECT name FROM pragma_index_info('${index[1]}') ORDER BY seqno")
                .map { "index ${index[1]} on $it" }
        }
        val triggers = query(dbFile, "SELECT name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name")
            .map { "trigger ${it[0]}: ${it[1]!!.replace("IF NOT EXISTS ", "")}" }
        val settings = listOf(
            "PRAGMA user_version",
            "PRAGMA auto_vacuum",
            "PRAGMA journal_mode",
            "PRAGMA encoding",
            "SELECT id || ' ' || identity_hash FROM room_master_table",
            "SELECT locale FROM android_metadata",
            "SELECT count(*) FROM merchant",
            "SELECT count(*) FROM atm",
            "SELECT count(*) FROM gift_card_providers"
        ).map { "$it: ${query(dbFile, it)}" }
        return objects.map { it.toString() } + columns + indexColumns + triggers + settings
    }

    private fun schema(version: ExploreVersion) =
        JsonParser.parseString(File("schemas/${version.number}.json").readText()).asJsonObject["database"].asJsonObject

    @Test
    fun regeneratedV4IsEquivalentToTheShippedTemplate() {
        // the gift_card_providers table of the shipped template was written by hand and is looser than Room's
        val roomDeclarations = mapOf(
            "column gift_card_providers.[sourceId, INTEGER, 1, null, 0]" to
                "column gift_card_providers.[sourceId, TEXT, 1, null, 0]",
            "column gift_card_providers.[id, INTEGER, 0, null, 1]" to
                "column gift_card_providers.[id, INTEGER, 1, null, 1]"
        )
        val shipped = describe(shipped(ExploreVersion.V4))
        val generated = describe(generated(ExploreVersion.V4))

        assertTrue(shipped.containsAll(roomDeclarations.keys))
        assertTrue(generated.contains("PRAGMA user_version: [[4]]"))
        val identityHash = schema(ExploreVersion.V4)["identityHash"].asString
        assertEquals("b571284317035b59474823f11d5bfcf5", identityHash)
        assertTrue(generated.contains("SELECT id || ' ' || identity_hash FROM room_master_table: [[42 $identityHash]]"))
        assertEquals(shipped.map { roomDeclarations[it] ?: it }, generated)
    }

    @Test
    fun shippedV5TemplateIsTheOneGeneratedFromItsSchema() {
        val shipped = describe(shipped(ExploreVersion.V5))
        val identityHash = schema(ExploreVersion.V5)["identityHash"].asString

        assertEquals(describe(generated(ExploreVersion.V5)), shipped)
        assertEquals(5, schema(ExploreVersion.V5)["version"].asInt)
        assertTrue(shipped.contains("PRAGMA user_version: [[5]]"))
        assertTrue(shipped.contains("SELECT id || ' ' || identity_hash FROM room_master_table: [[42 $identityHash]]"))
        assertTrue(shipped.contains("column merchant.[country, TEXT, 0, null, 0]"))
        assertTrue(shipped.contains("column merchant.[currency, TEXT, 0, null, 0]"))
        assertTrue(shipped.contains("column gift_card_providers.[currency, TEXT, 0, null, 0]"))
    }

    @Test
    fun v5AddsOnlyTheRegionColumnsToV4() {
        val added = describe(generated(ExploreVersion.V5)).filter { it.startsWith("column ") } -
            describe(generated(ExploreVersion.V4)).filter { it.startsWith("column ") }

        assertEquals(
            listOf(
                "column gift_card_providers.[currency, TEXT, 0, null, 0]",
                "column merchant.[country, TEXT, 0, null, 0]",
                "column merchant.[currency, TEXT, 0, null, 0]"
            ),
            added
        )
    }
}
