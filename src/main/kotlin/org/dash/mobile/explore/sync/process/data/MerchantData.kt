package org.dash.mobile.explore.sync.process.data

import java.sql.PreparedStatement

data class MerchantData(
    var deeplink: String? = "",
    var plusCode: String? = "",
    var addDate: String? = "",
    var updateDate: String? = "",
    var paymentMethod: String? = "",
    var merchantId: Long? = null,
    var id: Int? = null,        // leave null for auto increment
    var active: Boolean? = true,
    var name: String? = "",
    var address1: String? = "",
    var address2: String? = "",
    var address3: String? = "",
    var address4: String? = "",
    var latitude: Double? = 0.0,
    var longitude: Double? = 0.0,
    var website: String? = "",
    var phone: String? = "",
    var territory: String? = "",
    var city: String? = "",
    var source: String? = "",
    var sourceId: Int? = -1,
    var logoLocation: String? = "",
    var googleMaps: String? = "",
    var coverImage: String? = "",
    var type: String? = "",
    var instagram: String? = "",
    var twitter: String? = "",
    var delivery: String? = "",

    var monOpen: String? = "",
    var monClose: String? = "",
    var tueOpen: String? = "",
    var tueClose: String? = "",
    var wedOpen: String? = "",
    var wedClose: String? = "",
    var thuOpen: String? = "",
    var thuClose: String? = "",
    var friOpen: String? = "",
    var friClose: String? = "",
    var satOpen: String? = "",
    var satClose: String? = "",
    var sunOpen: String? = "",
    var sunClose: String? = "",

    var minCardPurchase: Double? = 0.0,
    var maxCardPurchase: Double? = 0.0
) : Data {

    companion object {

        const val INSERT_STATEMENT = "INSERT INTO merchant values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

        const val DEEP_LINK_COL = 1
        const val PLUS_CODE_COL = 2
        const val ADD_DATE_COL = 3
        const val UPDATE_DATE_COL = 4
        const val PAYMENT_METHOD_COL = 5
        const val MERCHANT_ID_COL = 6
        const val ID_COL = 7
        const val ACTIVE_COL = 8
        const val NAME_COL = 9
        const val ADDRESS1_COL = 10
        const val ADDRESS2_COL = 11
        const val ADDRESS3_COL = 12
        const val ADDRESS4_COL = 13
        const val LATITUDE_COL = 14
        const val LONGITUDE_COL = 15
        const val WEBSITE_COL = 16
        const val PHONE_COL = 17
        const val TERRITORY_COL = 18
        const val CITY_COL = 19
        const val SOURCE_COL = 20
        const val SOURCE_ID_COL = 21
        const val LOGO_LOCATION_COL = 22
        const val GOOGLE_MAPS_COL = 23
        const val COVER_IMAGE_COL = 24
        const val TYPE_COL = 25
        const val MIN_CARD_PURCHASE_COL = 26
        const val MAX_CARD_PURCHASE_COL = 27
    }

    override fun transferInto(statement: PreparedStatement): PreparedStatement {
        return statement.apply {
            setString(DEEP_LINK_COL, deeplink)
            setString(PLUS_CODE_COL, plusCode)
            setString(ADD_DATE_COL, addDate)
            setString(UPDATE_DATE_COL, updateDate)
            setString(PAYMENT_METHOD_COL, paymentMethod)
            setLong(MERCHANT_ID_COL, merchantId ?: 0L)
            setBoolean(ACTIVE_COL, active ?: false)
            setString(NAME_COL, name)
            setString(ADDRESS1_COL, address1)
            setString(ADDRESS2_COL, address2)
            setString(ADDRESS3_COL, address3)
            setString(ADDRESS4_COL, address4)
            setDouble(LATITUDE_COL, latitude ?: 0.0)
            setDouble(LONGITUDE_COL, longitude ?: 0.0)
            setString(WEBSITE_COL, website)
            setString(PHONE_COL, phone)
            setString(TERRITORY_COL, territory)
            setString(CITY_COL, city)
            setString(SOURCE_COL, source)
            setInt(SOURCE_ID_COL, sourceId ?: 0)
            setString(LOGO_LOCATION_COL, logoLocation)
            setString(GOOGLE_MAPS_COL, googleMaps)
            setString(COVER_IMAGE_COL, coverImage)
            setString(TYPE_COL, type)
            setDouble(MIN_CARD_PURCHASE_COL, minCardPurchase ?: 0.0)
            setDouble(MAX_CARD_PURCHASE_COL, maxCardPurchase ?: 0.0)
        }
    }
}