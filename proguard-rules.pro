# lan-coordinator library — internal ProGuard rules
# Keep public API so consumers can reference it without -keep rules of their own
-keep public class com.marn.go.coordinator.SequentialOrderManager { *; }
-keep public interface com.marn.go.coordinator.discovery.DeviceDiscoveryManager$DiscoveryListener { *; }
