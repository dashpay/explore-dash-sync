package org.dash.mobile.explore.sync

import org.dash.mobile.explore.sync.process.data.AtmLocation
import org.dash.mobile.explore.sync.process.data.MerchantData
import java.io.File
import java.sql.DriverManager

object RegionFixtures {
    const val MERCHANT_COLUMNS = "id, deeplink, plusCode, addDate, updateDate, paymentMethod, merchantId, active, " +
        "name, address1, address2, address3, address4, latitude, longitude, website, phone, territory, city, source, " +
        "sourceId, logoLocation, googleMaps, coverImage, type, redeemType, savingsPercentage, denominationsType"
    const val PROVIDER_COLUMNS = "id, merchantId, provider, redeemType, savingsPercentage, active, " +
        "denominationsType, sourceId"

    val atmData = listOf(
        AtmLocation(
            1, "https://example.org/atm", "", "ATM", "Operator", null, null, "Kiosk", "US", "VT", "Burlington",
            "1 Main St", 1, null, 44.4759, -73.2121, 1, 0, 0, null, null, null, null, null
        )
    )

    /** Tab separated, no quoting; an empty cell is null */
    fun load(fileName: String): List<MerchantData> {
        val lines = javaClass.classLoader.getResource("regions/$fileName")!!.readText().lines()
            .filter { it.isNotEmpty() }
        val header = lines.first().split("\t")
        return lines.drop(1).map { line ->
            val row = header.zip(line.split("\t")).toMap().mapValues { it.value.ifEmpty { null } }
            MerchantData(
                deeplink = null,
                plusCode = null,
                addDate = null,
                updateDate = null,
                paymentMethod = row["paymentMethod"],
                merchantId = row["merchantId"],
                name = row["name"],
                address1 = row["address1"],
                address2 = row["address2"],
                address3 = row["address3"],
                address4 = null,
                latitude = row["latitude"]?.toDouble(),
                longitude = row["longitude"]?.toDouble(),
                website = row["website"],
                phone = row["phone"],
                territory = row["territory"],
                city = row["city"],
                source = row["source"],
                sourceId = row["sourceId"],
                logoLocation = row["logoLocation"],
                googleMaps = null,
                coverImage = row["coverImage"],
                type = row["type"],
                redeemType = row["redeemType"],
                savingsPercentage = row["savingsPercentage"]?.toInt(),
                denominationsType = row["denominationsType"],
                country = row["country"],
                currency = row["currency"]
            )
        }
    }

    fun query(dbFile: File, sql: String): List<List<String?>> {
        return DriverManager.getConnection("jdbc:sqlite:${dbFile.path}").use { connection ->
            connection.createStatement().use { statement ->
                val rows = statement.executeQuery(sql)
                val count = rows.metaData.columnCount
                generateSequence { if (rows.next()) (1..count).map { rows.getString(it) } else null }.toList()
            }
        }
    }

    /** One line per row, values tab separated with their SQLite storage class, in insertion order */
    fun dump(dbFile: File, table: String, columns: String, where: String = "1"): List<String> {
        val typed = columns.split(", ").joinToString(", ") { "typeof($it) || ':' || ifnull($it, '')" }
        return query(dbFile, "SELECT $typed FROM $table WHERE $where ORDER BY id").map { it.joinToString("\t") }
    }
}
