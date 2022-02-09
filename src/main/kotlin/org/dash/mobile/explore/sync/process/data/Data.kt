package org.dash.mobile.explore.sync.process.data

import java.sql.PreparedStatement

interface Data {
    fun transferInto(statement: PreparedStatement): PreparedStatement
}