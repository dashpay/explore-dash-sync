package org.dash.mobile.explore.sync

class Mapping {

    companion object {
        val coinFlip = mapOf(
            "id" to "id",
            "name" to "name",
            "address1" to "address",
            "address2" to "address_line_1",
            "latitude" to "lat",
            "longitude" to "lng",
            "territory" to "country",
            "logo_location" to "cover_image",
            "phone" to "phone",
            "mon_open" to "mon",
            "mon_close" to "mon",
            "tue_open" to "tue",
            "tue_close" to "tue",
            "wed_open" to "wed",
            "wed_close" to "wed",
            "thu_open" to "thu",
            "thu_close" to "thu",
            "fri_open" to "fri",
            "fri_close" to "fri",
            "sat_open" to "sat",
            "sat_close" to "sat",
            "sun_open" to "sun",
            "sun_close" to "sun",
            "instagram" to "instagram",
            "twitter" to "twitter"
        )

        val dashDirect = mapOf(
            "id" to "Id",
            "name" to "LegalName",
            "address1" to "",
            "address2" to "",
            "address3" to "",
            "address4" to "",
            "latitude" to "",
            "longitude" to "",
            "plus_code" to "",
            "territory" to "",
            "google_maps" to "",
            "logo_location" to "LogoUrl",
            "website" to "Website",
            "payment_method" to "PaymentInstructions",
            "type" to "",
            "phone" to "",
            "mon_open" to "",
            "mon_close" to "",
            "tue_open" to "",
            "tue_close" to "",
            "wed_open" to "",
            "wed_close" to "",
            "thu_open" to "",
            "thu_close" to "",
            "fri_open" to "",
            "fri_close" to "",
            "sat_open" to "",
            "sat_close" to "",
            "sun_open" to "",
            "sun_close" to "",
            "instagram" to "",
            "twitter" to "",
            "delivery" to ""
        )
    }
}