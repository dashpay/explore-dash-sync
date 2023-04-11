package org.dash.mobile.explore.sync

import ch.qos.logback.classic.spi.ILoggingEvent
import com.fasterxml.jackson.core.JsonGenerator
import net.logstash.logback.composite.JsonProvider
import net.logstash.logback.composite.JsonWritingUtils
import net.logstash.logback.composite.loggingevent.LogLevelJsonProvider
import net.logstash.logback.composite.loggingevent.LoggingEventJsonProviders
import net.logstash.logback.encoder.LogstashEncoder
import org.slf4j.Logger
import java.io.IOException

/**
 * https://cloud.google.com/functions/docs/monitoring/logging
 */
class CustomLogstashEncoder : LogstashEncoder() {

    init {
        val providers = formatter.providers
        // Remove provider that is responsible for log level appending
        removeDefaultProvider(providers)
        // Register our implementation
        providers.addLogLevel(GCLogLevelJsonProvider())
    }

    private fun removeDefaultProvider(providers: LoggingEventJsonProviders) {
        var providerToDelete: JsonProvider<ILoggingEvent?>? = null
        for (provider in providers.providers) {
            if (provider is LogLevelJsonProvider) {
                providerToDelete = provider
                break
            }
        }
        providers.removeProvider(providerToDelete)
    }
}

/**
 * https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#LogSeverity
 */
enum class GCLogSeverity {
    DEBUG,
    INFO,
    NOTICE,
    WARNING,
    ERROR,
    CRITICAL,
    ALERT,
    EMERGENCY
}

class GCLogLevelJsonProvider : LogLevelJsonProvider() {

    @Throws(IOException::class)
    override fun writeTo(generator: JsonGenerator, event: ILoggingEvent) {
        event.argumentArray?.forEach {
            if (it is GCLogSeverity) {
                JsonWritingUtils.writeStringField(generator, fieldName, it.name)
                return
            }
        }
        JsonWritingUtils.writeStringField(generator, fieldName, event.level.toString())
    }
}

/**
 * firebase-admin module generates dozens of DEBUG level logs,
 * therefore, in order to avoid displaying those trash logs
 * in Google Cloud Log viewer the root LogLevel is set to INFO.
 * We still want to be able to report our own DEBUG logs,
 * that is why gcdebug methods were added for
 */
fun Logger.gcdebug(msg: String) {
    info(msg, GCLogSeverity.DEBUG)
}

fun Logger.gcdebug(format: String, arg1: Any, arg2: Any) {
    info(format, arg1, arg2, GCLogSeverity.DEBUG)
}

fun Logger.gcdebug(format: String, vararg arguments: Any) {
    info(format, arguments, GCLogSeverity.DEBUG)
}

fun Logger.notice(msg: String) {
    info(msg, GCLogSeverity.NOTICE)
}

fun Logger.notice(format: String, arg1: Any, arg2: Any) {
    info(format, arg1, arg2, GCLogSeverity.NOTICE)
}

fun Logger.notice(format: String, vararg arguments: Any) {
    info(format, arguments, GCLogSeverity.NOTICE)
}

fun Logger.alert(msg: String) {
    info(msg, GCLogSeverity.ALERT)
}

fun Logger.alert(format: String, arg1: Any, arg2: Any) {
    info(format, arg1, arg2, GCLogSeverity.ALERT)
}

fun Logger.alert(format: String?, vararg arguments: Any) {
    info(format, arguments, GCLogSeverity.ALERT)
}

fun Logger.emergency(msg: String) {
    info(msg, GCLogSeverity.EMERGENCY)
}
