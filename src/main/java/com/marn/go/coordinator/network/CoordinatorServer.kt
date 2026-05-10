package com.marn.go.coordinator.network

import io.ktor.server.cio.CIO
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import timber.log.Timber
import java.util.concurrent.atomic.AtomicInteger

/**
 * Ktor-embedded HTTP server running on the elected coordinator device.
 *
 * Endpoints:
 *  GET [NetworkConfig.ROUTE_NEXT] → atomically increment counter, return value as plain text
 *  GET [NetworkConfig.ROUTE_LAST] → return current counter without incrementing (re-election sync)
 *
 * The [AtomicInteger] is shared between the HTTP path (remote clients) and [assignNumber]
 * (the coordinator's own orders), keeping the global sequence consistent.
 */
internal class CoordinatorServer(startingNumber: Int = 0) {

    private val counter = AtomicInteger(startingNumber)
    private var engine: EmbeddedServer<*, *>? = null

    /** Called on a Ktor worker thread each time any number is assigned. */
    var onNumberAssigned: ((Int) -> Unit)? = null

    fun start() {
        engine = embeddedServer(
            factory = CIO,
            port    = NetworkConfig.HTTP_PORT,
            host    = "0.0.0.0"
        ) {
            routing {
                get(NetworkConfig.ROUTE_NEXT) {
                    val n = counter.incrementAndGet()
                    Timber.d("Assigned #$n to ${call.request.local.remoteHost}")
                    onNumberAssigned?.invoke(n)
                    call.respondText(n.toString())
                }
                get(NetworkConfig.ROUTE_LAST) {
                    call.respondText(counter.get().toString())
                }
            }
        }.start(wait = false)

        Timber.d("Ktor server started on :${NetworkConfig.HTTP_PORT}, counter=${counter.get()}")
    }

    fun stop() {
        engine?.stop(gracePeriodMillis = 500, timeoutMillis = 1_000)
        engine = null
        Timber.d("Ktor server stopped")
    }

    /** Assigns a number directly (coordinator's own orders — no HTTP round-trip). */
    fun assignNumber(): Int {
        val n = counter.incrementAndGet()
        Timber.d("Self-assigned #$n")
        onNumberAssigned?.invoke(n)
        return n
    }

    fun getCurrentCounter(): Int = counter.get()

    fun setCounter(value: Int) {
        counter.set(value)
        Timber.d("Counter synced to $value")
    }
}
