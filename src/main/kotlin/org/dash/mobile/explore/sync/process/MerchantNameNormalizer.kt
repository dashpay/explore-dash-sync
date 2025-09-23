package org.dash.mobile.explore.sync.process

import java.util.UUID
import java.security.MessageDigest
import java.nio.ByteBuffer

object MerchantNameNormalizer {
    private val names = hashMapOf<String, String>()
    private val logos = hashMapOf<String, String>()
    private val merchantIds = hashMapOf<String, String>()
    private val manualNames = hashMapOf(
        "Domino\'s Pizza" to "Domino's"
    )


    /** The key for a name is lower case with no ' */
    private fun getKey(merchantName: String): String {
        val name = manualNames[merchantName] ?: merchantName
        return name.lowercase().replace("\'", "")
    }

    private fun generateDeterministicUUID(name: String): UUID {
        // Use SHA-256 to create a deterministic hash of the name
        val digest = MessageDigest.getInstance("SHA-256")
        val hash = digest.digest(name.toByteArray(Charsets.UTF_8))
        
        // Use the first 16 bytes of the hash to create a UUID
        val bb = ByteBuffer.wrap(hash)
        val mostSigBits = bb.long
        val leastSigBits = bb.long
        
        return UUID(mostSigBits, leastSigBits)
    }

    fun removeSuffix(name: String) = when {
        name.lowercase().endsWith(" usd") -> {
            name.substring(0, name.length - 4)
        }
        name.lowercase().endsWith(" usa") -> {
            name.substring(0, name.length - 4)
        }
        name.lowercase().endsWith(" us") -> {
            name.substring(0, name.length - 3)
        }
        name.endsWith("®") -> {
            name.removeSuffix("®")
        }
        name.endsWith("(Same Day Delivery)") -> {
            name.removeSuffix("(Same Day Delivery)")
        }
        else -> {
            name
        }
    }.replace('’', '\'').trim()

    fun replaceHtmlEscapeSequences(name: String): String {
        return name.replace("&amp;", "&")
    }

    fun getNormalizedName(name: String?): String? {
        return if (name != null) {
            val name = replaceHtmlEscapeSequences(removeSuffix(name))
            names[getKey(name)] ?: name
        } else {
            null
        }
    }

    fun add(name: String?, logo: String?, merchantId: String?) {
        name?.let {
            val name = removeSuffix(name)
            val key = getKey(name)
            if (!names.containsKey(key)) {
                names.put(key, name)
            }
            logo?.let {
                if (!logos.containsKey(key)) {
                    logos.put(key, it)
                }
            }
            merchantId?.let {
                if (!merchantIds.containsKey(key)) {
                    merchantIds[key] = it
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

    fun getUniqueId(name: String): String {
        val normalizedName = removeSuffix(name)
        val key = getKey(normalizedName)
        val uuid = merchantIds[key]
        return if (uuid != null) {
            uuid
        } else {
            val newUUID = generateDeterministicUUID(normalizedName)
            merchantIds[key] = newUUID.toString()
            newUUID
        }.toString()
    }
}