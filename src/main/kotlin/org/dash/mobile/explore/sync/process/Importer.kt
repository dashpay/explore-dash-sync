package org.dash.mobile.explore.sync.process

import com.google.gson.*
import mu.KLogger

abstract class Importer {

    abstract val propertyName: String
    abstract val logger: KLogger

    abstract suspend fun import(save: Boolean): JsonArray

    fun addValidOrDie(inKey: String, inData: JsonObject, type: DashDirectImporter.DataType): JsonElement {
        try {
            val data = inData.get(inKey)
            if (data.isJsonNull) {
                return JsonNull.INSTANCE
            }
            val primitiveData = inData.get(inKey).asJsonPrimitive
            if (type.isEquivalentTo(primitiveData)) {
                return primitiveData
            } else {
                throw IllegalArgumentException("$inKey is not a $type ($primitiveData)")
            }
        } catch (ex: IllegalStateException) {
            logger.info("$inKey is not a JSON primitive (${inData.get(inKey)})")
            throw ex
        }
    }
}