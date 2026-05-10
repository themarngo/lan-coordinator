# consumer-rules.pro — applied to the consuming app's release build automatically.
# Keep Ktor CIO classes that are loaded via reflection at runtime.
-keep class io.ktor.server.cio.** { *; }
-keep class io.ktor.client.engine.cio.** { *; }
-dontwarn io.ktor.**
