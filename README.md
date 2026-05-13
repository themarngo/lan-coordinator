# Module `lan-coordinator`

A zero-backend Android library that assigns **strictly sequential order numbers** across multiple POS devices on the same branch WiFi network.

---

## How it works

One device is automatically elected as the **coordinator** and runs a small HTTP counter server. All other devices become **clients** that request the next number over HTTP. Leader election, failure detection, and re-election happen automatically via UDP broadcast — no manual configuration required.

```
Device A ──► Coordinator ──► "you get 5"
Device B ──► Coordinator ──► "you get 6"
Device A ──► Coordinator ──► "you get 7"
```

---

## Quick start

Build requirements:
- JDK 17
- Android Gradle Plugin 8.11.2
- Gradle wrapper 8.13

```kotlin
// 1. Create once — inject via Hilt (see CoordinatorModule)
val manager = SequentialOrderManager(context, deviceId = terminalId)

// 2. Start discovery on app launch
manager.start()

// 3. Before punching each order
val result = manager.getNextOrderNumber()
result.onSuccess { n ->
    order.orderNo       = n   // pure sequential, no prefix
    order.transactionNo = n
}
result.onFailure {
    // Coordinator unreachable — keep order.orderNo as-is (local prefixed fallback)
}

// Optional: reset from the coordinator device only.
// After this succeeds, the next assigned number is 1.
manager.resetSequence(to = 0)

// 4. Clean up when ViewModel is cleared
manager.stop()
```

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│              SequentialOrderManager              │  ← public API
│  - start() / stop()                             │
│  - getNextOrderNumber(): Result<Int>             │
│  - state: StateFlow<State>                      │
└────────────┬──────────────────┬─────────────────┘
             │                  │
    ┌────────▼──────┐  ┌────────▼──────────┐
    │DeviceDiscovery│  │ CoordinatorServer  │  (coordinator only)
    │   Manager     │  │ Ktor HTTP :45679   │
    │ UDP :45678    │  └───────────────────┘
    │ - Election    │
    │ - Heartbeat   │  ┌────────────────────┐
    │ - Sync phase  │  │  CoordinatorClient │  (client only)
    └───────────────┘  │  Ktor HttpClient   │
                       └────────────────────┘
```

---

## Leader election (Bully Algorithm)

| Step | Description | Duration |
|------|-------------|----------|
| 1 | Device broadcasts `POS_SEARCH:{deviceId}` | — |
| 2 | Wait for a `POS_ANNOUNCE` response | 1.5 s |
| 3 | If a higher-ID device is competing, yield long enough for the winner to sync and announce | +4 s |
| 4 | **Sync phase** — winner broadcasts `POS_SYNC_REQUEST`, collects last-known numbers from all devices | 1.5 s |
| 5 | New coordinator starts counter from `max(all replies)` | — |

**Tiebreaker:** When two devices discover simultaneously, the one with the lexicographically higher `deviceId` wins. Pass the terminal ID as `deviceId` for a stable, meaningful tiebreaker. Falls back to MAC address if omitted.

---

## Failure detection & re-election

```
Coordinator crashes / leaves WiFi
        │
        ▼  3 s  (3 missed heartbeats × 1 s each)
Re-election starts
        │
        ▼  1.5 s  (POS_SEARCH broadcast + wait)
Winner elected → sync phase
        │
        ▼  1.5 s  (collect POS_SYNC_REPLY from all devices)
New coordinator ready
        │
Winner ready ≈ 6 s
```

During this window, `getNextOrderNumber()` returns `Result.failure`. The caller should fall back to a local prefixed order number rather than blocking the cashier.

---

## Counter continuity after coordinator crash

Every order number is persisted to the lib's own private `SharedPreferences` (`lan_coordinator_prefs`) — independent of app-level preferences. This survives app restarts.

During the sync phase, the new coordinator collects each device's `lastOrderNumber` and starts from `max(all replies)`. This prevents duplicate order numbers even when the crashed coordinator had issued numbers unknown to the winner.

**Example:**
```
D1 (coordinator) crashes at 100
D2 wins election — lastKnown = 99
D3 replies to sync — lastKnown = 100
New coordinator starts from max(99, 100) = 100
Next issued number = 101  ✓  no duplicate
```

### Resetting the sequence

The active coordinator can reset its counter:

```kotlin
val reset = manager.resetSequence(to = 0)
```

After a successful reset to `0`, the next assigned order number is `1`.

Reset is coordinator-only. Calling it from a client or while discovering returns
`Result.failure`. In the current implementation, reset updates the current
coordinator only. If another device still has a higher persisted last number and
later becomes coordinator, the sync phase will resume from that higher number.
For a full branch-wide reset, stop or reset all terminals before orders resume.

---

## UDP protocol

All UDP messages are broadcast on port `45678`.

| Message | Format | Sent by |
|---------|--------|---------|
| Discovery search | `POS_SEARCH:{deviceId}` | Any device starting up |
| Coordinator announce | `POS_ANNOUNCE:{ip}` | Coordinator (reply to search) |
| Heartbeat | `POS_HEARTBEAT:{ip}` | Coordinator every 1 s |
| Sync request | `POS_SYNC_REQUEST` | New coordinator after winning election |
| Sync reply | `POS_SYNC_REPLY:{lastNumber}` | Every device (reply to sync request) |

HTTP order-number requests use port `45679` (Ktor CIO server, point-to-point).

---

## Timing constants (`NetworkConfig`)

| Constant | Value | Purpose |
|----------|-------|---------|
| `HEARTBEAT_INTERVAL_MS` | 1 000 ms | How often coordinator broadcasts heartbeat |
| `MAX_MISSED_HEARTBEATS` | 3 | Missed heartbeats before re-election |
| `DISCOVERY_TIMEOUT_MS` | 1 500 ms | Wait for coordinator announce on startup |
| `YIELD_EXTRA_WAIT_MS` | 4 000 ms | Grace period for higher-ID winner to sync and announce |
| `SYNC_COLLECT_MS` | 1 500 ms | Time to collect sync replies after winning election |
| `HTTP_TIMEOUT_MS` | 3 000 ms | HTTP request timeout to coordinator |

---

## Ports used

| Port | Protocol | Purpose |
|------|----------|---------|
| 45678 | UDP broadcast | Discovery, heartbeat, sync |
| 45679 | TCP/HTTP | Order-number requests (Ktor) |

Ensure these ports are open on the branch router/AP for UDP broadcast traffic.

---

## Known limitations

- **Best-effort during re-election:** Sequential guarantee is maintained after re-election via the sync phase, but any order punched during the ~6 s re-election window uses the local prefixed fallback.
- **Same WiFi subnet only:** UDP broadcast does not cross router boundaries. All POS devices must be on the same subnet.
- **No persistent coordinator:** The coordinator role is re-elected on every app restart. There is no "preferred coordinator" configuration.
- **Split-brain risk (mitigated):** Addressed via the deterministic tiebreaker (`deviceId` comparison). A very unlikely race where both devices broadcast at the exact same millisecond before either has received the other's broadcast can still cause split-brain, but this resolves itself within one heartbeat cycle.

---

## Hilt integration

```kotlin
// di/modules/CoordinatorModule.kt
@Module
@InstallIn(SingletonComponent::class)
object CoordinatorModule {

    @Provides
    @Singleton
    fun provideSequentialOrderManager(
        @ApplicationContext context: Context
    ): SequentialOrderManager {
        val terminalId = SharedPreferencesManager.getInstance()
            .authenticatedTerminalResponseModel?.terminal?.terminalId?.toString().orEmpty()
        return SequentialOrderManager(context = context, deviceId = terminalId)
    }
}
```

Lifecycle is managed by `MainViewModel`:
- `start()` called in `onViewCreated()`
- `stop()` called in `onCleared()`

---

## API reference

Dokka is not configured in this module by default. Use the source KDoc directly,
or add the Dokka Gradle plugin before running any Dokka task.

---

## Dependencies

| Library | Version | Scope |
|---------|---------|-------|
| Ktor Server CIO | 3.0.3 | `implementation` |
| Ktor Client CIO | 3.0.3 | `implementation` |
| Kotlinx Coroutines | 1.9.0 | `implementation` |
| Timber | 5.0.1 | `implementation` |
