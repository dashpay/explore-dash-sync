package org.dash.mobile.explore.sync.process

import com.google.gson.JsonArray

interface Importer {
    val propertyName: String
    suspend fun import(save: Boolean): JsonArray
}