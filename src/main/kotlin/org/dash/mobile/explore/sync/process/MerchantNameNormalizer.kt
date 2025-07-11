package org.dash.mobile.explore.sync.process

object MerchantNameNormalizer {
    private val names = hashMapOf<String, String>()
    private val logos = hashMapOf<String, String>()

    private fun getKey(name: String) = name.lowercase()

    fun removeSuffix(name: String) = when {
        name.lowercase().endsWith(" usd") -> {
            name.substring(0, name.length - 4)
        }
        name.endsWith("®") -> {
            name.removeSuffix("®")
        }
        else -> {
            name
        }
    }

    fun getNormalizedName(name: String?): String? {
        return if (name != null) {
            val name = removeSuffix(name)
            names[getKey(name)] ?: name
        } else {
            null
        }
    }

    fun add(name: String?, logo: String?) {
        name?.let {
            val name = removeSuffix(name)
            val key = getKey(name)
            if (!names.containsKey(key)) {
                names.put(key, name)
            }
            logo?.let {
                if (!logos.containsKey(key)) {
                    logos.put(key, logo)
                }
            }
        }
    }
    fun getLogo(name: String?): String? {
        return name?.let {
            val name = removeSuffix(name)
            val key = getKey(name)
            logos[key]
        }
    }
}