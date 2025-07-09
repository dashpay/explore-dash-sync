package org.dash.mobile.explore.sync.process.data

import java.sql.PreparedStatement

data class MerchantInfo(
    var id: Int? = null, // leave null for auto increment
    var merchantId: String? = "",
    var active: Boolean? = true,
    var name: String? = "",
    var source: String? = "",
    var sourceId: String? = "",
    var logoLocation: String? = "",
    var googleMaps: String? = "",
    var coverImage: String? = "",
    var type: String? = "",
    var redeemType: String? = "",
    var savingsPercentage: Int? = 0,
    var denominationsType: String? = "",
    var instagram: String? = "",
    var twitter: String? = "",
    var delivery: String? = "",
) : Data {

    companion object {

        const val INSERT_STATEMENT = "INSERT INTO merchant_info values(?,?,?,?,?,?,?,?,?,?,?,?,?)"

        const val ID_COL = 1
        const val MERCHANT_ID_COL = 2
        const val ACTIVE_COL = 3
        const val NAME_COL = 4
        const val SOURCE_COL = 5
        const val SOURCE_ID_COL = 6
        const val LOGO_LOCATION_COL = 7
        const val GOOGLE_MAPS_COL = 8
        const val COVER_IMAGE_COL = 9
        const val TYPE_COL = 10
        const val REDEEM_TYPE_COL = 11
        const val SAVINGS_PERCENTAGE_COL = 12
        const val DENOMINATION_TYPE_COL = 13
    }

    override fun transferInto(statement: PreparedStatement): PreparedStatement {
        return statement.apply {
            setString(MERCHANT_ID_COL, merchantId ?: "")
            setBoolean(ACTIVE_COL, active ?: false)
            setString(NAME_COL, name)
            setString(SOURCE_COL, source)
            setString(SOURCE_ID_COL, sourceId ?: "")
            setString(LOGO_LOCATION_COL, logoLocation)
            setString(GOOGLE_MAPS_COL, googleMaps)
            setString(COVER_IMAGE_COL, coverImage)
            setString(TYPE_COL, type)
            setString(REDEEM_TYPE_COL, redeemType ?: "none")
            setInt(SAVINGS_PERCENTAGE_COL, savingsPercentage ?: 0)
            setString(DENOMINATION_TYPE_COL, denominationsType)
        }
    }
}
