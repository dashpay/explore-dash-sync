package org.dash.mobile.explore.sync.process.data

import java.sql.PreparedStatement

data class AtmData(
    var postcode: String? = "",
    var manufacturer: String? = "",
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
    var sunClose: String? = ""
) : Data {

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
            setString(POSTCODE_COL, postcode)
            setString(MANUFACTURER_COL, manufacturer)
            setBoolean(ACTIVE_COL, active ?: false)
//            setInt(ID_COL, null)  // auto increment
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
        }
    }
}