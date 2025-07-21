package org.dash.mobile.explore.sync.utils

import org.dash.mobile.explore.sync.process.data.MerchantData
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import kotlin.collections.forEach


object CSVExporter {
    private val logger = LoggerFactory.getLogger(CSVExporter::class.java)!!

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