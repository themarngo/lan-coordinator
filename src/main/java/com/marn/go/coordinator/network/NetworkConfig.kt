package com.marn.go.coordinator.network

/**
 * Protocol constants for the LAN coordinator library.
 *
 *  UDP 45678  → device discovery, heartbeat, and sync (broadcast)
 *  HTTP 45679 → order-number requests via Ktor (point-to-point)
 */
object NetworkConfig {

    // ── Ports ──────────────────────────────────────────────────────────────
    const val UDP_PORT  = 45678
    const val HTTP_PORT = 45679

    // ── HTTP routes ─────────────────────────────────────────────────────────
    const val ROUTE_NEXT = "/order/next"   // GET → assign & return next number
    const val ROUTE_LAST = "/order/last"   // GET → read counter without incrementing

    // ── Timing (ms) ─────────────────────────────────────────────────────────
    //
    // Total re-election time breakdown:
    //   Detection  : HEARTBEAT_INTERVAL_MS × MAX_MISSED_HEARTBEATS = 3 × 1s = 3s
    //   Discovery  : DISCOVERY_TIMEOUT_MS                          = 1.5s
    //   Sync phase : SYNC_COLLECT_MS                               = 1.5s
    //   ─────────────────────────────────────────────────────────────────────
    //   Winner ready ≈ 6s
    //
    //   Yield wait (YIELD_EXTRA_WAIT_MS) must be > DISCOVERY + SYNC = 3.0s
    //   so yielding devices don't self-elect before the winner announces.
    //   The winner broadcasts POS_ANNOUNCE at t=3.0s; yielding device receives
    //   it and becomes CLIENT immediately. The 4.0s is a UDP-loss safety net.
    //
    const val DISCOVERY_TIMEOUT_MS    = 1_500L   // 1.5s — wait for coordinator announce on startup
    const val HEARTBEAT_INTERVAL_MS   = 1_000L   // 1s   — coordinator broadcasts every second
    const val MAX_MISSED_HEARTBEATS   = 3        //       — 3 missed = 3s before re-election fires
    const val UDP_RECEIVE_TIMEOUT_MS  = 500       // 0.5s — keeps receiver loop responsive
    const val HTTP_TIMEOUT_MS         = 3_000L   // 3s   — HTTP request timeout to coordinator
    const val SYNC_COLLECT_MS         = 1_500L   // 1.5s — collect sync replies from all devices
    const val YIELD_EXTRA_WAIT_MS     = 4_000L   // 4s   — must exceed DISCOVERY_TIMEOUT + SYNC_COLLECT

    // ── UDP discovery tokens ─────────────────────────────────────────────────
    /**
     * Format: "POS_SEARCH:{deviceId}"
     * deviceId is the deterministic tiebreaker — higher ID wins election.
     */
    const val MSG_SEARCH_PREFIX       = "POS_SEARCH:"      // + deviceId
    const val MSG_ANNOUNCE_PREFIX     = "POS_ANNOUNCE:"    // + coordinator IP
    const val MSG_HEARTBEAT_PREFIX    = "POS_HEARTBEAT:"   // + coordinator IP

    /**
     * Sync phase — broadcast by the new coordinator after winning election.
     * Every device that hears this replies with its last known order number
     * so the new coordinator can resume from the true maximum.
     *
     * Format of reply: "POS_SYNC_REPLY:{lastOrderNumber}"
     */
    const val MSG_SYNC_REQUEST        = "POS_SYNC_REQUEST"
    const val MSG_SYNC_REPLY_PREFIX   = "POS_SYNC_REPLY:"  // + lastOrderNumber
}
