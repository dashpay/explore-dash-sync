package org.dash.mobile.explore.sync.process

import com.google.gson.JsonParser
import org.dash.mobile.explore.sync.OperationMode
import org.dash.mobile.explore.sync.slack.SlackMessenger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class CTXSpendDataSourceTest {

    private val dataSource = CTXSpendDataSource(SlackMessenger(), OperationMode.TESTNET, false)

    private fun convert(merchantFields: String, territory: String) = dataSource.convert(
        JsonParser.parseString(
            """{"id": "m1", "name": "Williams Sonoma®", "type": "physical", "redeemType": "barcode",
                "savingsPercentage": 500, "denominationsType": "min-max" $merchantFields}"""
        ).asJsonObject,
        JsonParser.parseString(
            """{"address1": "1 Main St", "latitude": 1.5, "longitude": 2.5, "territory": "$territory", "sourceId": "l1"}"""
        ).asJsonObject
    )

    @Test
    fun readsCountryAndCurrencyInUpperCase() {
        val merchant = convert(""", "country": "gb", "currency": " gbp"""", "England")

        assertEquals("GB", merchant.country)
        assertEquals("GBP", merchant.currency)
        assertEquals("Williams Sonoma", merchant.name)
    }

    @Test
    fun leavesMissingCountryAndCurrencyEmpty() {
        val merchant = convert("", "AL")

        assertNull(merchant.country)
        assertNull(merchant.currency)
    }

    @Test
    fun expandsStateAbbreviationsForUsMerchantsOnly() {
        assertEquals("Michigan", convert(""", "country": "US", "currency": "USD"""", "MI").territory)
        assertEquals("MI", convert(""", "country": "IT", "currency": "EUR"""", "MI").territory)
    }
}
