package org.dash.mobile.explore.sync.process

import com.google.gson.Gson
import org.dash.mobile.explore.sync.process.PiggyCardsDataSource.Endpoint.Brand
import org.dash.mobile.explore.sync.process.PiggyCardsDataSource.Endpoint.Giftcard
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class PiggyCardsDataSourceTest {

    private fun card(currency: String) = Gson().fromJson(
        """{"id": 1, "name": "Card", "image": "https://example.org/card.png", "price_type": "Range",
            "discount_percentage": 5.0 $currency}""",
        Giftcard::class.java
    )

    @Test
    fun passesOnTheCountryOfTheRequestAndTheCurrencyOfTheCardAsReceived() {
        val merchant = PiggyCardsDataSource.convert(Brand("7", "Target "), card(""", "currency": " cad""""), "CA")

        assertEquals("CA", merchant.country)
        assertEquals(" cad", merchant.currency)
        assertEquals(350, merchant.savingsPercentage)
    }

    @Test
    fun leavesTheCurrencyEmptyWhenTheCardHasNone() {
        val merchant = PiggyCardsDataSource.convert(Brand("7", "Target "), card(""), "US")

        assertEquals("US", merchant.country)
        assertNull(merchant.currency)
    }
}
