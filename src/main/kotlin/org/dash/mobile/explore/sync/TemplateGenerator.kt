package org.dash.mobile.explore.sync

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import java.io.File
import java.sql.DriverManager
import kotlin.system.exitProcess

/**
 * Builds the empty explore database the sync fills in, from a schema JSON exported by Room in Dash Wallet
 * (features/exploredash/schemas). Room accepts the file only with the identity hash and version of that schema.
 */
object TemplateGenerator {

    fun generate(schemaJson: File, output: File) {
        val database = JsonParser.parseString(schemaJson.readText()).asJsonObject["database"].asJsonObject
        require(database["views"].asJsonArray.size() == 0) { "views are not supported" }

        val statements = arrayListOf<String>()
        database["entities"].asJsonArray.map { it.asJsonObject }.forEach { entity ->
            val tableName = entity["tableName"].asString
            statements.add(entity.createSql(tableName))
            entity["indices"].asJsonArray.forEach { statements.add(it.asJsonObject.createSql(tableName)) }
            entity["contentSyncTriggers"]?.asJsonArray?.forEach { statements.add(it.asString) }
        }
        database["setupQueries"].asJsonArray.forEach { statements.add(it.asString) }

        output.delete()
        DriverManager.getConnection("jdbc:sqlite:${output.path}").use { connection ->
            connection.createStatement().use { statement ->
                // as in a database created by Android, which is how the first template was made
                statement.execute("PRAGMA auto_vacuum = FULL")
                statement.execute("PRAGMA journal_mode = WAL")
                statement.execute("CREATE TABLE android_metadata (locale TEXT)")
                statement.execute("INSERT INTO android_metadata VALUES ('en_US')")
                statements.forEach { statement.execute(it) }
                statement.execute("PRAGMA user_version = ${database["version"].asInt}")
            }
        }
    }

    private fun JsonObject.createSql(tableName: String) =
        this["createSql"].asString.replace("\${TABLE_NAME}", tableName)
}

fun main(args: Array<String>) {
    if (args.size != 2) {
        println("usage: generateTemplate --args=\"<room schema json> <output db>\"")
        exitProcess(1)
    }
    TemplateGenerator.generate(File(args[0]), File(args[1]))
    println("${args[1]} generated from ${args[0]}")
}
