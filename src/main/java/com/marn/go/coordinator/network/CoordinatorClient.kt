package com.marn.go.coordinator.network

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import timber.log.Timber

/**
 * Ktor HTTP client used by CLIENT devices to request order numbers from the coordinator.
 *
 * A single [HttpClient] instance is reused across all requests (connection pooling).
 * Call [close] when the owning component is destroyed.
 */
internal class CoordinatorClient {

    private val httpClient = HttpClient(CIO) {
        install(HttpTimeout) {
            requestTimeoutMillis = NetworkConfig.HTTP_TIMEOUT_MS
            connectTimeoutMillis = NetworkConfig.HTTP_TIMEOUT_MS
            socketTimeoutMillis  = NetworkConfig.HTTP_TIMEOUT_MS
        }
    }

    suspend fun requestOrderNumber(coordinatorIp: String): Result<Int> =
        runCatching {
            val url = "http://$coordinatorIp:${NetworkConfig.HTTP_PORT}${NetworkConfig.ROUTE_NEXT}"
            val n   = httpClient.get(url).bodyAsText().trim().toInt()
            Timber.d("Received #$n from $coordinatorIp")
            n
        }.also { it.onFailure { e -> Timber.e("requestOrderNumber failed: ${e.message}") } }

    suspend fun getLastNumber(coordinatorIp: String): Int? =
        runCatching {
            val url = "http://$coordinatorIp:${NetworkConfig.HTTP_PORT}${NetworkConfig.ROUTE_LAST}"
            httpClient.get(url).bodyAsText().trim().toInt()
        }.also { it.onFailure { e -> Timber.w("getLastNumber failed: ${e.message}") } }
         .getOrNull()

    fun close() = httpClient.close()
}
