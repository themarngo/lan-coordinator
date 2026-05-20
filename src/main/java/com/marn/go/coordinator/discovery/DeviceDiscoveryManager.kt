package com.marn.go.coordinator.discovery

import com.marn.go.coordinator.network.NetworkConfig
import timber.log.Timber
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.Inet4Address
import java.net.InetAddress
import java.net.NetworkInterface
import java.util.concurrent.atomic.AtomicInteger

/**
 * UDP-based device discovery, leader election, and counter sync.
 *
 * Responsibilities:
 *  1. Discovery  — broadcasts MSG_SEARCH_PREFIX+deviceId on start, waits
 *                  [NetworkConfig.DISCOVERY_TIMEOUT_MS].
 *  2. Tiebreaker — if two devices discover simultaneously, the one with the
 *                  lexicographically higher [myDeviceId] wins; the lower-ID
 *                  device yields, preventing split-brain.
 *  3. Sync phase — after winning election, the new coordinator broadcasts
 *                  MSG_SYNC_REQUEST and waits [NetworkConfig.SYNC_COLLECT_MS]
 *                  for all devices to reply with their last order number.
 *                  The coordinator then resumes from max(all replies), ensuring
 *                  no duplicate order numbers after a coordinator crash.
 *  4. Heartbeat  — coordinator broadcasts MSG_HEARTBEAT every
 *                  [NetworkConfig.HEARTBEAT_INTERVAL_MS].
 *  5. Watchdog   — clients detect coordinator silence and trigger re-election.
 */
internal class DeviceDiscoveryManager(
    private val listener   : DiscoveryListener,
    private val myDeviceId : String
) {
    interface DiscoveryListener {
        fun onDiscoveryStarted()
        fun onElectionYielded()
        fun onSyncStarted()
        fun onBecomeCoordinator(startingNumber: Int)
        fun onCoordinatorFound(coordinatorIp: String)
        fun onCoordinatorLost()
        fun onNetworkDisconnected()
        /** Called during sync phase — return the last order number this device received. */
        fun getLastKnownNumber(): Int
        /** Called when a coordinator heartbeat/announce carries a newer counter value. */
        fun onCoordinatorCounterObserved(lastOrderNumber: Int)
    }

    private var scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private var udpSocket            : DatagramSocket? = null
    @Volatile private var role       = Role.DISCOVERING
    private var discoveryJob         : Job?   = null
    private var heartbeatJob         : Job?   = null
    private var networkJob           : Job?   = null
    @Volatile private var lastHeartbeatMs  = 0L
    @Volatile private var coordinatorTermMs = 0L
    @Volatile private var localIps   : Set<String> = emptySet()
    private var myIp                 = ""

    @Volatile private var yieldedToHigherId = false
    @Volatile private var abortSyncForHigherId = false

    /**
     * Tracks the maximum last-order-number received during the sync phase.
     * Seeded with this device's own last number before the broadcast,
     * then updated as POS_SYNC_REPLY messages arrive.
     */
    private val syncMax = AtomicInteger(0)

    private enum class Role { DISCOVERING, SYNCING, COORDINATOR, CLIENT, DISCONNECTED }

    // ── Lifecycle ─────────────────────────────────────────────────────────

    fun start() {
        if (!scope.isActive) {
            scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        }
        discoveryJob?.cancel()
        heartbeatJob?.cancel()
        networkJob?.cancel()
        runCatching { udpSocket?.close() }
        role = Role.DISCOVERING
        yieldedToHigherId = false
        abortSyncForHigherId = false
        coordinatorTermMs = 0L
        lastHeartbeatMs = 0L
        myIp      = getDeviceIp()
        refreshLocalIps()
        startNetworkWatchdog()
        if (localIps.isEmpty()) {
            role = Role.DISCONNECTED
            Timber.w("No LAN IPv4 address available — waiting for network")
            listener.onNetworkDisconnected()
            return
        }
        udpSocket = DatagramSocket(NetworkConfig.UDP_PORT).apply { broadcast = true }
        Timber.d("Starting — myIp=$myIp  deviceId=$myDeviceId")
        listener.onDiscoveryStarted()
        scope.launch { receiverLoop() }
        startDiscovery(delayMs = 0)
    }

    fun stop() {
        role = Role.DISCOVERING
        discoveryJob?.cancel()
        heartbeatJob?.cancel()
        networkJob?.cancel()
        scope.cancel()
        runCatching { udpSocket?.close() }
        udpSocket = null
    }

    fun publishCoordinatorState(repeatCount: Int = 1) {
        if (role == Role.COORDINATOR) {
            val payload = coordinatorPayload()
            Timber.d("Publishing coordinator heartbeat payload=$payload repeats=$repeatCount")
            repeatBroadcast(NetworkConfig.MSG_HEARTBEAT_PREFIX + payload, repeatCount)
        }
    }

    private fun startDiscovery(delayMs: Long) {
        discoveryJob?.cancel()
        discoveryJob = scope.launch {
            if (delayMs > 0) delay(delayMs)
            myIp = getDeviceIp()
            refreshLocalIps()
            runDiscovery()
        }
    }

    // ── Discovery sequence ────────────────────────────────────────────────

    private suspend fun runDiscovery() {
        role              = Role.DISCOVERING
        yieldedToHigherId = false
        abortSyncForHigherId = false
        listener.onDiscoveryStarted()

        broadcast(NetworkConfig.MSG_SEARCH_PREFIX + myDeviceId)
        delay(NetworkConfig.DISCOVERY_TIMEOUT_MS)

        if (role != Role.DISCOVERING) return   // became CLIENT via announce/heartbeat

        if (yieldedToHigherId) {
            Timber.d("Yielded to higher-ID device — waiting ${NetworkConfig.YIELD_EXTRA_WAIT_MS}ms")
            listener.onElectionYielded()
            delay(NetworkConfig.YIELD_EXTRA_WAIT_MS)
        }

        if (role != Role.DISCOVERING) return   // coordinator announced during extra wait

        // Won the election — run sync phase before starting the server.
        runSyncPhase()
    }

    /**
     * Sync phase: broadcast POS_SYNC_REQUEST so every device reports its
     * last known order number. After [NetworkConfig.SYNC_COLLECT_MS] ms,
     * start the coordinator server from max(all received numbers).
     *
     * This prevents issuing duplicate order numbers when a coordinator crashes:
     * even if the winning device only knew up to N-1, another device's reply
     * of N pushes the new coordinator to resume from N, issuing N+1 next.
     */
    private suspend fun runSyncPhase() {
        role = Role.SYNCING
        listener.onSyncStarted()
        myIp = getDeviceIp()
        refreshLocalIps()
        // Seed with our own last number so we're included in the max.
        syncMax.set(listener.getLastKnownNumber())

        Timber.d("Sync phase started — myLastNumber=${syncMax.get()}")
        broadcast(NetworkConfig.MSG_SYNC_REQUEST)
        delay(NetworkConfig.SYNC_COLLECT_MS)

        if (role != Role.SYNCING || abortSyncForHigherId) {
            val abortedForHigherId = abortSyncForHigherId
            Timber.d("Sync phase aborted — role changed to $role")
            abortSyncForHigherId = false
            if (role == Role.CLIENT) return

            role = Role.DISCOVERING
            if (abortedForHigherId) {
                Timber.d("Waiting ${NetworkConfig.YIELD_EXTRA_WAIT_MS}ms for higher-ID coordinator announce")
                delay(NetworkConfig.YIELD_EXTRA_WAIT_MS)
                if (role == Role.DISCOVERING) startDiscovery(delayMs = 0)
            }
            return
        }

        val resumeFrom = syncMax.get()
        Timber.d("Sync phase done — resumeFrom=$resumeFrom → becoming coordinator")

        coordinatorTermMs = System.currentTimeMillis()
        role = Role.COORDINATOR
        listener.onBecomeCoordinator(resumeFrom)

        // Explicitly announce to ALL devices (including those in yield-wait) that
        // we are now coordinator.  Without this, yielding devices only learn of the
        // new coordinator when the first heartbeat arrives — which may be after their
        // YIELD_EXTRA_WAIT_MS expires, causing them to self-elect (split-brain).
        broadcast(NetworkConfig.MSG_ANNOUNCE_PREFIX + coordinatorPayload())

        startHeartbeating()
    }

    // ── Message dispatcher ────────────────────────────────────────────────

    private fun handleMessage(msg: String, senderIp: String) {
        if (senderIp == myIp || senderIp in localIps) return

        when {
            // ── Incoming discovery search ────────────────────────────────
            msg.startsWith(NetworkConfig.MSG_SEARCH_PREFIX) -> {
                val theirId = msg.removePrefix(NetworkConfig.MSG_SEARCH_PREFIX)
                when (role) {
                    Role.COORDINATOR ->
                        // Respond so the newcomer immediately becomes a client.
                        broadcast(NetworkConfig.MSG_ANNOUNCE_PREFIX + coordinatorPayload())

                    Role.DISCOVERING -> {
                        if (theirId > myDeviceId) {
                            yieldedToHigherId = true
                            listener.onElectionYielded()
                            Timber.d("Competing search from higher-ID $theirId — yielding")
                        } else {
                            Timber.d("Competing search from lower-ID $theirId — we win")
                        }
                    }

                    Role.SYNCING -> {
                        if (theirId > myDeviceId) {
                            yieldedToHigherId = true
                            abortSyncForHigherId = true
                            listener.onElectionYielded()
                            Timber.d("Competing search from higher-ID $theirId while syncing — yielding")
                        } else {
                            Timber.d("Competing search from lower-ID $theirId while syncing — we continue")
                        }
                    }

                    Role.CLIENT, Role.DISCONNECTED -> { /* ignore */ }
                }
            }

            // ── Coordinator announce (while discovering) ─────────────────
            msg.startsWith(NetworkConfig.MSG_ANNOUNCE_PREFIX) -> {
                val payload = msg.removePrefix(NetworkConfig.MSG_ANNOUNCE_PREFIX)
                when (role) {
                    Role.DISCOVERING, Role.SYNCING -> {
                        observeCoordinatorCounter(payload)
                        becomeClient(resolveCoordinatorIp(payload, senderIp))
                    }
                    Role.COORDINATOR -> handleCoordinatorConflict(payload, senderIp)
                    else -> Unit
                }
            }

            // ── Heartbeat received while still discovering ────────────────
            msg.startsWith(NetworkConfig.MSG_HEARTBEAT_PREFIX) -> {
                val payload = msg.removePrefix(NetworkConfig.MSG_HEARTBEAT_PREFIX)
                when (role) {
                    Role.DISCOVERING, Role.SYNCING -> {
                        observeCoordinatorCounter(payload)
                        becomeClient(resolveCoordinatorIp(payload, senderIp))
                    }
                    Role.CLIENT -> {
                        lastHeartbeatMs = System.currentTimeMillis()
                        observeCoordinatorCounter(payload)
                        Timber.d("Heartbeat received from ${resolveCoordinatorIp(payload, senderIp)} payload=$payload")
                    }
                    Role.COORDINATOR -> handleCoordinatorConflict(payload, senderIp)
                    Role.DISCONNECTED -> Unit
                }
            }

            // ── Sync request — reply with our last known order number ─────
            msg == NetworkConfig.MSG_SYNC_REQUEST && role != Role.SYNCING -> {
                val myLast = listener.getLastKnownNumber()
                Timber.d("Sync request received — replying with $myLast")
                broadcast("${NetworkConfig.MSG_SYNC_REPLY_PREFIX}$myLast")
            }

            // ── Sync reply — update running maximum ───────────────────────
            msg.startsWith(NetworkConfig.MSG_SYNC_REPLY_PREFIX) && role == Role.SYNCING -> {
                val n = msg.removePrefix(NetworkConfig.MSG_SYNC_REPLY_PREFIX).toIntOrNull() ?: 0
                val updated = syncMax.updateAndGet { maxOf(it, n) }
                Timber.d("Sync reply from $senderIp: n=$n  currentMax=$updated")
            }
        }
    }

    // ── Role transitions ──────────────────────────────────────────────────

    private fun becomeClient(coordinatorIp: String) {
        Timber.d("Coordinator found at $coordinatorIp")
        role            = Role.CLIENT
        lastHeartbeatMs = System.currentTimeMillis()
        listener.onCoordinatorFound(coordinatorIp)
        startHeartbeatWatchdog()
    }

    private fun handleCoordinatorConflict(payload: String, senderIp: String) {
        val theirTerm = parseCoordinatorTerm(payload)
        val theirDeviceId = parseCoordinatorDeviceId(payload)
        val shouldYield = when {
            theirTerm == null                                    -> true
            theirTerm > coordinatorTermMs                        -> true
            theirTerm < coordinatorTermMs                        -> false
            theirDeviceId != null && theirDeviceId != myDeviceId -> theirDeviceId > myDeviceId
            else                                                 -> senderIp > myIp
        }

        if (shouldYield) {
            val coordinatorIp = resolveCoordinatorIp(payload, senderIp)
            Timber.w("Another coordinator detected at $coordinatorIp — yielding")
            becomeClient(coordinatorIp)
        }
    }

    // ── Heartbeat broadcaster (coordinator) ──────────────────────────────

    private fun startHeartbeating() {
        heartbeatJob?.cancel()
        heartbeatJob = scope.launch {
            while (role == Role.COORDINATOR) {
                val payload = coordinatorPayload()
                Timber.d("Heartbeat broadcast payload=$payload")
                broadcast(NetworkConfig.MSG_HEARTBEAT_PREFIX + payload)
                delay(NetworkConfig.HEARTBEAT_INTERVAL_MS)
            }
        }
    }

    // ── Heartbeat watchdog (client) ───────────────────────────────────────

    private fun startHeartbeatWatchdog() {
        heartbeatJob?.cancel()
        heartbeatJob = scope.launch {
            val maxSilence = NetworkConfig.HEARTBEAT_INTERVAL_MS * NetworkConfig.MAX_MISSED_HEARTBEATS
            while (role == Role.CLIENT) {
                delay(NetworkConfig.HEARTBEAT_INTERVAL_MS)
                if (System.currentTimeMillis() - lastHeartbeatMs > maxSilence) {
                    Timber.w("Coordinator silent — re-electing")
                    role = Role.DISCOVERING
                    listener.onCoordinatorLost()
                    startDiscovery(delayMs = 500)
                    break
                }
            }
        }
    }

    private fun startNetworkWatchdog() {
        networkJob?.cancel()
        networkJob = scope.launch {
            while (isActive) {
                delay(NetworkConfig.NETWORK_WATCHDOG_INTERVAL_MS)
                refreshLocalIps()

                if (localIps.isEmpty()) {
                    if (role != Role.DISCONNECTED) {
                        Timber.w("LAN disconnected — pausing coordinator discovery")
                        disconnectFromNetwork()
                    }
                } else if (role == Role.DISCONNECTED) {
                    Timber.i("LAN connected — restarting coordinator discovery")
                    restartAfterNetworkReturn()
                }
            }
        }
    }

    private fun disconnectFromNetwork() {
        role = Role.DISCONNECTED
        discoveryJob?.cancel()
        heartbeatJob?.cancel()
        runCatching { udpSocket?.close() }
        udpSocket = null
        listener.onNetworkDisconnected()
    }

    private fun restartAfterNetworkReturn() {
        runCatching { udpSocket?.close() }
        myIp = getDeviceIp()
        refreshLocalIps()
        udpSocket = DatagramSocket(NetworkConfig.UDP_PORT).apply { broadcast = true }
        role = Role.DISCOVERING
        yieldedToHigherId = false
        abortSyncForHigherId = false
        coordinatorTermMs = 0L
        lastHeartbeatMs = 0L
        listener.onDiscoveryStarted()
        scope.launch { receiverLoop() }
        startDiscovery(delayMs = 0)
    }

    // ── UDP helpers ───────────────────────────────────────────────────────

    private fun receiverLoop() {
        val buffer = ByteArray(1024)
        val socket = udpSocket ?: return
        while (!socket.isClosed) {
            try {
                socket.soTimeout = NetworkConfig.UDP_RECEIVE_TIMEOUT_MS
                val packet = DatagramPacket(buffer, buffer.size)
                socket.receive(packet)
                val msg      = String(packet.data, 0, packet.length).trim()
                val senderIp = packet.address.hostAddress ?: continue
                handleMessage(msg, senderIp)
            } catch (_: Exception) { /* soTimeout on idle — expected */ }
        }
    }

    private fun broadcast(message: String) {
        scope.launch(Dispatchers.IO) {
            runCatching {
                val data   = message.toByteArray()
                val packet = DatagramPacket(
                    data, data.size,
                    InetAddress.getByName("255.255.255.255"),
                    NetworkConfig.UDP_PORT
                )
                udpSocket?.send(packet)
            }.onFailure { Timber.e("Broadcast error: ${it.message}") }
        }
    }

    private fun repeatBroadcast(message: String, repeatCount: Int) {
        scope.launch(Dispatchers.IO) {
            repeat(repeatCount.coerceAtLeast(1)) { index ->
                runCatching {
                    val data = message.toByteArray()
                    val packet = DatagramPacket(
                        data, data.size,
                        InetAddress.getByName("255.255.255.255"),
                        NetworkConfig.UDP_PORT
                    )
                    udpSocket?.send(packet)
                }.onFailure { Timber.e("Broadcast error: ${it.message}") }

                if (index < repeatCount - 1) {
                    delay(NetworkConfig.STATE_REBROADCAST_INTERVAL_MS)
                }
            }
        }
    }

    private fun coordinatorPayload(): String {
        val currentIp = getDeviceIp()
        if (currentIp != "0.0.0.0") myIp = currentIp
        return "$myIp|$coordinatorTermMs|${listener.getLastKnownNumber()}|$myDeviceId"
    }

    private fun parseCoordinatorIp(payload: String): String =
        payload.substringBefore('|')

    private fun resolveCoordinatorIp(payload: String, senderIp: String): String {
        val advertisedIp = parseCoordinatorIp(payload)
        return when {
            advertisedIp.isBlank() || advertisedIp == "0.0.0.0" -> senderIp
            advertisedIp != senderIp -> {
                Timber.w("Coordinator advertised $advertisedIp but packet came from $senderIp — using sender IP")
                senderIp
            }
            else -> advertisedIp
        }
    }

    private fun parseCoordinatorTerm(payload: String): Long? =
        payload.split('|').getOrNull(1)?.toLongOrNull()

    private fun observeCoordinatorCounter(payload: String) {
        parseCoordinatorLastNumber(payload)?.let {
            Timber.d("Coordinator counter observed from heartbeat/announce: $it")
            listener.onCoordinatorCounterObserved(it)
        }
    }

    private fun parseCoordinatorLastNumber(payload: String): Int? =
        payload.split('|').getOrNull(2)?.toIntOrNull()

    private fun parseCoordinatorDeviceId(payload: String): String? =
        payload.split('|').getOrNull(3)?.takeIf { it.isNotBlank() }

    private fun refreshLocalIps() {
        localIps = try {
            NetworkInterface.getNetworkInterfaces()
                ?.asSequence()
                ?.filter { it.isUp && !it.isLoopback }
                ?.flatMap { it.inetAddresses.asSequence() }
                ?.filterIsInstance<Inet4Address>()
                ?.mapNotNull { it.hostAddress }
                ?.toSet() ?: emptySet()
        } catch (_: Exception) { localIps }
    }

    private fun getDeviceIp(): String = try {
        NetworkInterface.getNetworkInterfaces()
            ?.asSequence()
            ?.filter { it.isUp && !it.isLoopback }
            ?.flatMap { it.inetAddresses.asSequence() }
            ?.filterIsInstance<Inet4Address>()
            ?.firstOrNull { !it.isLoopbackAddress }
            ?.hostAddress ?: "0.0.0.0"
    } catch (e: Exception) { "0.0.0.0" }
}
