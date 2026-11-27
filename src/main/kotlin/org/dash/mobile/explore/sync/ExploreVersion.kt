package org.dash.mobile.explore.sync

enum class ExploreVersion(val number: Int) {
    /** US gift card merchants only: wallets reading this file treat every gift card as USD */
    V4(4),

    /** Gift card merchants of all countries, with merchant.country/currency and gift_card_providers.currency */
    V5(5);

    val hasRegions = number >= 5

    /** v4 keeps the file names it had when it was the only version */
    val fileSuffix = if (number == 4) "" else "-v$number"
    val emptyDbResource = "explore-empty$fileSuffix.db"
    val dbFileName = "explore$fileSuffix.db"
}
