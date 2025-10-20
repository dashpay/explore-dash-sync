package org.dash.mobile.explore.sync

class SyncReport(
    dataSources: List<DataSourceReport>,
) {
    var mergedLocations: Int = 0
    var totalMerchants: Int = 0
    var totalLocations: Int = 0
    val dataSourceMap = dataSources.associateBy { it.source }.toMutableMap()
    fun updateDataSourceReport(report: DataSourceReport) {
        dataSourceMap[report.source] = report
    }
    operator fun get(source: String) = dataSourceMap[source]

    override fun toString(): String {
        val sb = StringBuilder()
        
        // Print SyncReport fields with indentation and Slack formatting
        sb.appendLine("*explore-dash-sync Report:*")
        sb.appendLine("  • *Total Merchants:* $totalMerchants")
        sb.appendLine("  • *Total Locations:* $totalLocations")
        sb.appendLine("  • *Duplicate Locations:* $mergedLocations")

        // Print DataSource map values with indentation and formatting
        sb.appendLine("  * Data Sources:*")
        dataSourceMap.values.forEach { dataSource ->
            sb.appendLine("    *${dataSource.source}:*")
            sb.appendLine("      • *Merchants:* ${dataSource.merchants}")
            sb.appendLine("      • *Locations:* ${dataSource.locations}")
            if (dataSource.disabledMerchants.isNotEmpty()) {
                sb.appendLine("      • ⚠️ *Disabled Merchants:* ${dataSource.disabledMerchants.size} ${dataSource.disabledMerchants}")
            }
            if (dataSource.newMerchants.isNotEmpty()) {
                sb.appendLine("      • ✅ *New Merchants:* ${dataSource.newMerchants.size} ${dataSource.newMerchants}")
            }
            if (dataSource.removedMerchants.isNotEmpty()) {
                sb.appendLine("      • ❌ *Removed Merchants:* ${dataSource.removedMerchants.size} ${dataSource.removedMerchants}")
            }
            if (dataSource.negativeDiscounts.isNotEmpty()) {
                sb.appendLine("      • ❌ *Negative Discount Merchants:* ${dataSource.negativeDiscounts.size} ${dataSource.negativeDiscounts}")
            }
        }
        
        return sb.toString()
    }
}

data class DataSourceReport(
    val source: String,
    val merchants: Int,
    val locations: Int,
    val disabledMerchants: List<String> = listOf(),
    val newMerchants: List<String> = listOf(),
    val removedMerchants: List<String> = listOf(),
    val negativeDiscounts: List<String> = listOf()
)
