# Installation

This guide covers installing Memris in your Java project.

## Requirements

Before installing Memris, ensure you have:

- **Java 21** or higher
- **Maven 3.6+** or **Gradle 7+**
- A compatible Spring Boot version (for Spring Boot integration)

!!! tip "Java Version"
    Memris requires Java 21+ due to its use of Java 21 features, including pattern matching switch support used in core hot paths.

## Maven

### Core Library Only

Add the core library to your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.thejuampi</groupId>
    <artifactId>memris</artifactId>
    <version>0.2.0</version>
</dependency>
```

### With Spring Boot 3

For Spring Boot 3.x applications, use the starter:

```xml
<dependency>
    <groupId>io.github.thejuampi</groupId>
    <artifactId>memris-spring-boot-starter-3</artifactId>
    <version>0.2.0</version>
</dependency>
```

### With Spring Boot 2

For Spring Boot 2.7.x applications:

```xml
<dependency>
    <groupId>io.github.thejuampi</groupId>
    <artifactId>memris-spring-boot-starter-2</artifactId>
    <version>0.2.0</version>
</dependency>
```

## Gradle

### Core Library Only

Add to your `build.gradle`:

```groovy
dependencies {
    implementation 'io.github.thejuampi:memris:0.2.0'
}
```

### With Spring Boot 3

```groovy
dependencies {
    implementation 'io.github.thejuampi:memris-spring-boot-starter-3:0.2.0'
}
```

### With Spring Boot 2

```groovy
dependencies {
    implementation 'io.github.thejuampi:memris-spring-boot-starter-2:0.2.0'
}
```

## Verify Installation

### Core Library

Create a simple test to verify the installation:

```java
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.core.MemrisArena;

public class InstallationTest {
    public static void main(String[] args) {
        MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
        MemrisArena arena = factory.createArena();
        System.out.println("Memris installed successfully!");
    }
}
```

### Spring Boot

For Spring Boot, verify the auto-configuration is working:

```java
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import io.memris.spring.data.repository.config.EnableMemrisRepositories;

@SpringBootApplication
@EnableMemrisRepositories
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        System.out.println("Spring Boot + Memris integration working!");
    }
}
```

## Next Steps

- [Quick Start](quick-start.md) - Build your first Memris application in 5 minutes
- [Configuration](configuration.md) - Learn about configuration options
- [Spring Boot Setup](spring-boot-setup.md) - Full Spring Boot integration guide
