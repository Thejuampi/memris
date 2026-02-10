# Configuration Properties

Memris can be configured via Spring Boot's `application.yml` or `application.properties`.

## Basic Configuration

### Minimal Configuration

```yaml
memris:
  arenas:
    default:
      page-size: 1024
```

This creates a default arena with a page size of 1024 rows.

### Full Configuration

```yaml
memris:
  default-arena: default
  arenas:
    default:
      page-size: 1024
      max-pages: 1024
      initial-pages: 1024
      enable-parallel-sorting: true
      parallel-sort-threshold: 1000
      codegen-enabled: true
      enable-prefix-index: true
      enable-suffix-index: true
```

## Property Reference

### Global Properties

| Property | Default | Description |
|----------|---------|-------------|
| `memris.default-arena` | `default` | Name of the default arena to use |

### Arena Properties

Each arena under `memris.arenas.<name>` supports:

| Property | Default | Description |
|----------|---------|-------------|
| `page-size` | 1024 | Number of rows per page |
| `max-pages` | 1024 | Maximum number of pages |
| `initial-pages` | 1024 | Initial number of pages to allocate |
| `enable-parallel-sorting` | true | Enable parallel sorting for large datasets |
| `parallel-sort-threshold` | 1000 | Minimum row count to trigger parallel sorting |
| `codegen-enabled` | true | Enable ByteBuddy code generation |
| `enable-prefix-index` | true | Enable prefix indexes for startsWith queries |
| `enable-suffix-index` | true | Enable suffix indexes for endsWith queries |

## Configuration Examples

### Read-Heavy Workload

Optimize for read performance:

```yaml
memris:
  arenas:
    default:
      page-size: 2048
      max-pages: 2048
      initial-pages: 2048
      enable-parallel-sorting: true
      parallel-sort-threshold: 500
```

### Write-Heavy Workload

Optimize for write performance:

```yaml
memris:
  arenas:
    default:
      page-size: 1024
      max-pages: 4096
      initial-pages: 4096
      enable-parallel-sorting: false
```

### Memory-Constrained

Minimize memory usage:

```yaml
memris:
  arenas:
    default:
      page-size: 512
      max-pages: 512
      initial-pages: 256
```

### Analytics Workload

Larger pages for analytical queries:

```yaml
memris:
  arenas:
    analytics:
      page-size: 4096
      max-pages: 8192
      enable-parallel-sorting: true
```

## Multiple Arenas

Configure multiple isolated data arenas:

```yaml
memris:
  default-arena: transactional
  arenas:
    transactional:
      page-size: 1024
      max-pages: 1024
      initial-pages: 1024
    cache:
      page-size: 512
      max-pages: 512
      initial-pages: 256
    analytics:
      page-size: 4096
      max-pages: 8192
      initial-pages: 4096
```

### Using Multiple Arenas

Inject specific arenas using `@Qualifier`:

```java
@Service
public class DataService {
    private final MemrisArena transactionalArena;
    private final MemrisArena cacheArena;
    
    public DataService(
            @Qualifier("transactional") MemrisArena transactionalArena,
            @Qualifier("cache") MemrisArena cacheArena) {
        this.transactionalArena = transactionalArena;
        this.cacheArena = cacheArena;
    }
}
```

## Environment-Based Configuration

### System Properties

Override via command line:

```bash
java -Dmemris.arenas.default.page-size=2048 -jar app.jar
```

### Environment Variables

```bash
export MEMRIS_ARENAS_DEFAULT_PAGE_SIZE=2048
export MEMRIS_ARENAS_DEFAULT_MAX_PAGES=4096
```

### Profile-Specific

Use Spring profiles for environment-specific config:

**application-dev.yml:**
```yaml
memris:
  arenas:
    default:
      page-size: 512
      max-pages: 512
```

**application-prod.yml:**
```yaml
memris:
  arenas:
    default:
      page-size: 4096
      max-pages: 8192
```

## Property Binding

Properties are bound to `MemrisArenaProperties`:

```java
@ConfigurationProperties("memris")
public class MemrisArenaProperties {
    private String defaultArena = "default";
    private Map<String, MemrisConfigurationProperties> arenas = new HashMap<>();
    // ...
}
```

And converted to `MemrisConfiguration`:

```java
@Data
public class MemrisConfigurationProperties {
    private int pageSize = 1024;
    private int maxPages = 1024;
    private int initialPages = 1024;
    private boolean enableParallelSorting = true;
    private int parallelSortThreshold = 1000;
    private boolean codegenEnabled = true;
    private boolean enablePrefixIndex = true;
    private boolean enableSuffixIndex = true;
    
    public MemrisConfiguration toConfiguration() {
        return MemrisConfiguration.builder()
            .pageSize(pageSize)
            .maxPages(maxPages)
            // ...
            .build();
    }
}
```

## Validation

Memris validates configuration at startup:

- `maxPages` must be >= `initialPages`
- `pageSize` must be positive
- `parallelSortThreshold` must be positive

Invalid configuration throws `IllegalArgumentException` on startup.

## Configuration Properties File

For reference, here's a complete `application.yml`:

```yaml
spring:
  application:
    name: my-memris-app

memris:
  default-arena: default
  arenas:
    default:
      page-size: 1024
      max-pages: 1024
      initial-pages: 1024
      enable-parallel-sorting: true
      parallel-sort-threshold: 1000
      codegen-enabled: true
      enable-prefix-index: true
      enable-suffix-index: true

logging:
  level:
    io.memris: INFO
```

## Configuration Metadata

Spring Boot's configuration processor generates metadata for IDE auto-completion. Ensure you have:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-configuration-processor</artifactId>
    <optional>true</optional>
</dependency>
```

This provides:
- Auto-completion in IDE
- Documentation on hover
- Validation of property names
