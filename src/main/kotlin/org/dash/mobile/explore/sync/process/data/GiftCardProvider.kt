package org.dash.mobile.explore.sync.process.data

import java.sql.PreparedStatement

/**
 * Gift card provider
 *
 * CREATE TABLE gift_card_providers (
 *     id INTEGER PRIMARY KEY,
 *     merchantId TEXT NOT NULL,
 *     provider TEXT NOT NULL,
 *     redeemType TEXT NOT NULL,
 *     savingsPercentage INTEGER NOT NULL,
 *     active INTEGER NOT NULL,
 *     denominationsType TEXT NOT NULL,
 *     sourceId INTEGER NOT NULL
 * );
 */

data class GiftCardProvider(
    var id: Int? = null, // leave null for auto increment
    var merchantId: String? = "",
    var active: Boolean? = true,
    var provider: String? = "",
    var sourceId: String? = "",
    var redeemType: String? = "",
    var savingsPercentage: Int? = 0,
    var denominationsType: String? = "",
) : Data {

    companion object {

        const val INSERT_STATEMENT = "INSERT INTO gift_card_providers values(?,?,?,?,?,?,?,?)"

        const val ID_COL = 1
        const val MERCHANT_ID_COL = 2
        const val PROVIDER_COL = 3
        const val REDEEM_TYPE_COL = 4
        const val SAVINGS_PERCENTAGE_COL = 5
        const val ACTIVE_COL = 6
        const val DENOMINATION_TYPE_COL = 7
        const val SOURCE_ID_COL = 8
    }

    override fun transferInto(statement: PreparedStatement): PreparedStatement {
        return statement.apply {
            setNull(ID_COL, java.sql.Types.INTEGER)
            setString(MERCHANT_ID_COL, merchantId ?: "")
            setBoolean(ACTIVE_COL, active ?: false)
            setString(PROVIDER_COL, provider)
            setString(SOURCE_ID_COL, sourceId ?: "")
            setString(REDEEM_TYPE_COL, redeemType ?: "none")
            setInt(SAVINGS_PERCENTAGE_COL, savingsPercentage ?: 0)
            setString(DENOMINATION_TYPE_COL, denominationsType)
        }
    }
}
