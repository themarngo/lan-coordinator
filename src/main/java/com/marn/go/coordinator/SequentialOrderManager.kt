package com.marn.go.coordinator

import android.content.Context
import com.marn.go.coordinator.discovery.DeviceDiscoveryManager
import com.marn.go.coordinator.network.CoordinatorClient
import com.marn.go.coordinator.network.CoordinatorServer
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import timber.log.Timber
import java.net.NetworkInterface

/**
 * Public API of the lan-coordinator library.
 *
 * Coordinates sequential order-number assignment across multiple Android devices
 * on the same local WiFi network — no backend server required.
 *
 * ── Usage ────────────────────────────────────────────────────────────────────
 *
 *   val orderManager = SequentialOrderManager(context, deviceId = terminalId)
 *   orderManager.start()
 *
 *   val result = orderManager.getNextOrderNumber()
 *   result.onSuccess { number -> ... }
 *
 *   orderManager.state.collect { state -> ... }
 *   orderManager.stop()
 *
 * ── Tiebreaker / split-brain prevention ──────────────────────────────────────
 *
 *  Pass a stable unique [deviceId] (e.g. terminal ID from your backend) so that
 *  when two devices start simultaneously, the one with the lexicographically
 *  higher ID always wins the election. Falls back to MAC address if omitted.
 *
 * ── Counter continuity after coordinator crash ────────────────────────────────
 *
 *  Every order number received is persisted to a private SharedPreferences file.
 *  When a device wins re-election, it broadcasts a sync request so all other
 *  devices reply with their last known number. The new coordinator starts from
 *  max(all replies), guaranteeing no duplicate order numbers even if the crashed
 *  coordinator had issued numbers unknown to the winner.
 *
 * ── Integration note (OrderUtil / SharedPreferences) ─────────────────────────
 *
 *  This library manages its own private SharedPreferences ("lan_coordinator_prefs")
 *  independently from app-level SharedPreferences. If your existing code reads a
 *  counter from app SharedPreferences (e.g. getNewOrderNumber()), seed it yourself:
 *
 *      result.onSuccess { n ->
 *          SharedPreferencesManager.getInstance()
 *              .saveInt(PrefConstants.KEY_LAST_ORDER_NUMBER, n - 1)
 *          val order = orderUtil.prepareOrderToPunch(isHold = false)
 *      }
 *
 * ─────────────────────────────────────────────────────────────────────────────
 */
class SequentialOrderManager(
    context  : Context,
    deviceId : String = ""
) : DeviceDiscoveryManager.DiscoveryListener {

    // ── Private SharedPreferences (lib-managed) ───────────────────────────

    private val prefs = context.applicationContext
        .getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    // ── Public state ──────────────────────────────────────────────────────

    enum class DeviceRole { DISCOVERING, COORDINATOR, CLIENT }

    data class State(
        val role            : DeviceRole = DeviceRole.DISCOVERING,
        val coordinatorIp   : String     = "—",
        val lastOrderNumber : Int        = 0,
        val statusMessage   : String     = "Searching for coordinator…"
    )

    private val _state = MutableStateFlow(
        // Restore persisted last number so sync replies are accurate after restart.
        State(lastOrderNumber = prefs.getInt(PREF_KEY_LAST_NUMBER, 0))
    )

    /** Observe this to drive your UI (role badge, coordinator IP, etc.). */
    val state: StateFlow<State> = _state

    // ── Internals ─────────────────────────────────────────────────────────

    private val tag        = "SequentialOrderManager"
    private val myDeviceId = deviceId.ifBlank { detectMacAddress(context) }
    private val discovery  = DeviceDiscoveryManager(this, myDeviceId)
    private var client     = CoordinatorClient()
    private var server     : CoordinatorServer? = null

    // ── Lifecycle ─────────────────────────────────────────────────────────

    fun start() {
        Timber.d("start() — deviceId=$myDeviceId  lastKnown=${_state.value.lastOrderNumber}")
        discovery.start()
    }

    fun stop() {
        discovery.stop()
        server?.stop()
        server = null
        client.close()
        Timber.d("stop()")
    }

    // ── Core API ──────────────────────────────────────────────────────────

    /**
     * Returns the next strictly-sequential order number shared across all
     * devices in the branch.
     *
     * @return [Result.success] with the assigned [Int], or
     *         [Result.failure] if the device is still discovering or a network
     *         error occurred.
     */
    suspend fun getNextOrderNumber(): Result<Int> = when (_state.value.role) {

        DeviceRole.COORDINATOR -> {
            val srv = server
                ?: return Result.failure(IllegalStateException("Server not initialised"))
            Result.success(srv.assignNumber())
        }

        DeviceRole.CLIENT -> {
            val ip = _state.value.coordinatorIp
            client.requestOrderNumber(ip).also { result ->
                result.onSuccess { n -> persistAndUpdate(n) }
            }
        }

        DeviceRole.DISCOVERING ->
            Result.failure(IllegalStateException("Still discovering — please wait a moment"))
    }

    /**
     * Resets the current coordinator counter to [to].
     *
     * Only the active coordinator can reset the sequence. After a successful
     * reset, the next assigned order number will be [to] + 1.
     *
     * This reset is local to the current coordinator. If other devices still
     * have a higher persisted last number and later win re-election, sync will
     * resume from that higher number.
     */
    fun resetSequence(to: Int = 0): Result<Unit> {
        if (to < 0) {
            return Result.failure(IllegalArgumentException("Sequence reset value must be >= 0"))
        }

        if (_state.value.role != DeviceRole.COORDINATOR) {
            return Result.failure(IllegalStateException("Only the coordinator can reset the sequence"))
        }

        val srv = server
            ?: return Result.failure(IllegalStateException("Server not initialised"))

        srv.setCounter(to)
        persistAndUpdate(to)
        Timber.w("Sequence reset to $to; next order number will be ${to + 1}")
        return Result.success(Unit)
    }

    // ── DiscoveryListener ─────────────────────────────────────────────────

    override fun onBecomeCoordinator(startingNumber: Int) {
        // startingNumber = max(all sync replies) — already accounts for every
        // device's last known number, so no duplicates can occur.
        Timber.d("Role → COORDINATOR (resuming from $startingNumber)")
        server?.stop()
        server = CoordinatorServer(startingNumber).also { srv ->
            srv.onNumberAssigned = { n -> persistAndUpdate(n) }
            srv.start()
        }
        _state.value = _state.value.copy(
            role          = DeviceRole.COORDINATOR,
            coordinatorIp = "this device",
            statusMessage = "I am the coordinator ✦"
        )
    }

    override fun onCoordinatorFound(coordinatorIp: String) {
        Timber.d("Role → CLIENT (coordinator=$coordinatorIp)")
        server?.stop()
        server = null
        client.close()
        client = CoordinatorClient()
        _state.value = _state.value.copy(
            role          = DeviceRole.CLIENT,
            coordinatorIp = coordinatorIp,
            statusMessage = "Connected to coordinator at $coordinatorIp"
        )
    }

    override fun onCoordinatorLost() {
        Timber.w("Coordinator lost — re-electing")
        _state.value = _state.value.copy(
            role          = DeviceRole.DISCOVERING,
            coordinatorIp = "—",
            statusMessage = "Coordinator lost — re-electing…"
        )
    }

    /**
     * Called by [DeviceDiscoveryManager] during the sync phase so the new
     * coordinator can collect the true maximum last-order-number across all devices.
     */
    override fun getLastKnownNumber(): Int = _state.value.lastOrderNumber

    // ── Helpers ───────────────────────────────────────────────────────────

    /**
     * Persists [n] to the lib's private SharedPreferences AND updates the
     * in-memory state. Called on every number assigned (coordinator) or
     * received (client) so the value survives app restarts and is accurate
     * for sync replies during re-election.
     */
    private fun persistAndUpdate(n: Int) {
        prefs.edit().putInt(PREF_KEY_LAST_NUMBER, n).apply()
        _state.value = _state.value.copy(lastOrderNumber = n)
    }

    private fun detectMacAddress(context: Context): String = try {
        NetworkInterface.getNetworkInterfaces()
            ?.asSequence()
            ?.filter { it.isUp && !it.isLoopback }
            ?.mapNotNull { iface ->
                iface.hardwareAddress
                    ?.takeIf { it.isNotEmpty() }
                    ?.joinToString(":") { "%02X".format(it) }
            }
            ?.firstOrNull()
            ?: "UNKNOWN-${System.currentTimeMillis()}"
    } catch (e: Exception) {
        "UNKNOWN-${System.currentTimeMillis()}"
    }

    companion object {
        private const val PREFS_NAME        = "lan_coordinator_prefs"
        private const val PREF_KEY_LAST_NUMBER = "last_order_number"
    }
}
