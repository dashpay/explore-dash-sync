package org.dash.mobile.explore.sync.process.data

import com.google.gson.annotations.SerializedName
import java.sql.PreparedStatement

// CoinATMRadar data structure.
data class AtmLocation(
    val id: Int,
    val url: String,
    @SerializedName("image_url")
    val imageUrl: String,
    val type: String,
    val operator: String,
    @SerializedName("operator_logo")
    val operatorLogo: String?,
    @SerializedName("operator_phone")
    val operatorPhone: String?,
    val location: String,
    val country: String,
    var state: String?,
    val city: String,
    val address: String,
    @SerializedName("is_247")
    val is24by7: Int,
    @SerializedName("open_hours")
    val openHours: String?,
    val lat: Double?,
    val lng: Double?,
    val buy: Int,
    val sell: Int,
    @SerializedName("updated_online")
    val updatedOnline: Long,
    @SerializedName("buy_fee_perc")
    val buyFeePerc: String?,
    @SerializedName("sell_fee_perc")
    val sellFeePerc: String?,
    val fiat: String?,
    @SerializedName("buy_fee_amt")
    val buyFeeAmt: String?,
    @SerializedName("sell_fee_amt")
    val sellFeeAmt: String?
): Data {
    companion object {
        const val INSERT_STATEMENT = "INSERT INTO atm values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

        const val POSTCODE_COL = 1
        const val MANUFACTURER_COL = 2
        const val ID_COL = 3
        const val ACTIVE_COL = 4
        const val NAME_COL = 5
        const val ADDRESS1_COL = 6
        const val ADDRESS2_COL = 7
        const val ADDRESS3_COL = 8
        const val ADDRESS4_COL = 9
        const val LATITUDE_COL = 10
        const val LONGITUDE_COL = 11
        const val WEBSITE_COL = 12
        const val PHONE_COL = 13
        const val TERRITORY_COL = 14
        const val CITY_COL = 15
        const val SOURCE_COL = 16
        const val SOURCE_ID_COL = 17
        const val LOGO_LOCATION_COL = 18
        const val GOOGLE_MAPS_COL = 19
        const val COVER_IMAGE_COL = 20
        const val TYPE_COL = 21
    }

    override fun transferInto(statement: PreparedStatement): PreparedStatement {
        return statement.apply {
            setString(POSTCODE_COL, "")
            setString(MANUFACTURER_COL, operator)
            setBoolean(ACTIVE_COL, true)
//            setInt(ID_COL, null)  // auto increment
            setString(NAME_COL, location)
            setString(ADDRESS1_COL, address)
            setString(ADDRESS2_COL, "")
            setString(ADDRESS3_COL, "")
            setString(ADDRESS4_COL, "")
            setDouble(LATITUDE_COL, lat ?: 0.0)
            setDouble(LONGITUDE_COL, lng ?: 0.0)
            setString(WEBSITE_COL, url)
            setString(PHONE_COL, operatorPhone)
            setString(TERRITORY_COL, if (state.isNullOrEmpty()) city else state)
            setString(CITY_COL, city)
            setString(SOURCE_COL, "coinatmradar")
            setString(SOURCE_ID_COL, id.toString())
            setString(LOGO_LOCATION_COL, operatorLogo)
            setString(GOOGLE_MAPS_COL, "")
            setString(COVER_IMAGE_COL, "")
            setString(TYPE_COL, getType(buy == 1, sell == 1))
        }
    }

    private fun getType(buy: Boolean, sell: Boolean): String {
        return when {
            buy && sell -> "Buy and Sell"
            sell -> "Sell Only"
            else -> "Buy Only"
        }
    }
}
