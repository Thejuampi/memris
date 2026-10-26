# Packaged consumers

These are independent Java 21 applications. They resolve published coordinates,
without project substitution or the engine's test classpath. The Spring consumers
use the starter's automatic configuration, repository scanning, converters,
named arenas, and context shutdown. Boot 2 and Boot 3 run in separate JVMs and
check that the other persistence API is absent.

From the repository root, publish a candidate locally and run all three:

```sh
./gradlew publishAllPublicationsToBuildRepository -Pversion=0.3.0
./gradlew -p verification/consumers verifyConsumers -PmemrisVersion=0.3.0 -PmemrisRepository=../../build/repository --no-configuration-cache
```

Repeat the consumer command with `-PpomOnly` to resolve Maven POMs instead of
Gradle module metadata. On PowerShell quote property arguments containing dots,
for example `'-PmemrisVersion=0.3.0'`.

To verify Maven Central after publication, omit `-PmemrisRepository`. With
`-PpomOnly`, also use `-PmemrisRepository=https://repo.maven.apache.org/maven2`.
The exclusive candidate repository ensures that a missing Memris artifact fails
instead of falling back to an older remote artifact.
