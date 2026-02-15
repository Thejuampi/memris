# Configuration

Memris provides flexible configuration options to tune performance and behavior.

## Programmatic Configuration

When using the core library, configure via `MemrisConfiguration`:

```java
import io.memris.core.MemrisConfiguration;
import io.memris.core.MemrisConfiguration.TableImplementation;
import io.memris.repository.MemrisRepositoryFactory;

MemrisConfiguration config = MemrisConfiguration.builder()
    .tableImplementation(TableImplementation.BYTECODE)
    .pageSize(1024)
    .maxPages(1024)
    .initialPages(1024)
    .enableParallelSorting(true)
    .parallelSortThreshold(1000)
    .codegenEnabled(true)
    .enablePrefixIndex(true)
    .enableSuffixIndex(true)
    .build();

MemrisRepositoryFactory factory = new MemrisRepositoryFactory(config);
```

## Configuration Properties

### Page Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `pageSize` | 1024 | Number of rows per page. Larger pages reduce memory overhead but increase allocation time. |
| `maxPages` | 1024 | Maximum number of pages. Limits total capacity to `pageSize × maxPages`. |
| `initialPages` | 1024 | Initial number of pages to allocate. |

!!! tip "Page Size Tuning"
    - **Small datasets (< 100K rows)**: Use smaller page sizes (512-1024)
    - **Large datasets (> 1M rows)**: Use larger page sizes (2048-4096)
    - **High-write workloads**: Pre-allocate with higher `initialPages`

### Sorting Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `enableParallelSorting` | true | Enable parallel sorting for large datasets |
| `parallelSortThreshold` | 1000 | Minimum row count to trigger parallel sorting |

### Code Generation

| Property | Default | Description |
|----------|---------|-------------|
| `codegenEnabled` | true | Enable ByteBuddy code generation for optimized runtime executors |
| `tableImplementation` | BYTECODE | Table generation strategy: `BYTECODE` (ByteBuddy) or `METHOD_HANDLE` (fallback) |

!!! warning "Disable Code Generation"
    Only disable code generation if you encounter compatibility issues. The fallback `METHOD_HANDLE` table strategy generally has higher overhead than `BYTECODE` on hot paths.

```java
MemrisConfiguration config = MemrisConfiguration.builder()
    .tableImplementation(TableImplementation.METHOD_HANDLE)
    .codegenEnabled(false)
    .build();
```

### Index Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `enablePrefixIndex` | true | Enable prefix indexes for `startsWith` queries |
| `enableSuffixIndex` | true | Enable suffix indexes for `endsWith` queries |

### Audit Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `auditProvider` | null | Provider for `@CreatedBy`/`@LastModifiedBy` audit fields |

```java
MemrisConfiguration config = MemrisConfiguration.builder()
    .auditProvider(() -> SecurityContextHolder.getCurrentPrincipal())
    .build();
```

### Advanced Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `entityMetadataProvider` | default | Custom provider for entity metadata extraction |

```java
MemrisConfiguration config = MemrisConfiguration.builder()
    .entityMetadataProvider(myCustomProvider)
    .build();
```

## Spring Boot Configuration

With Spring Boot, use `application.yml`:

```yaml
memris:
  default-arena: default
  arenas:
    default:
      page-size: 1024
      max-pages: 2048
      initial-pages: 1024
      enable-parallel-sorting: true
      parallel-sort-threshold: 1000
      codegen-enabled: true
      enable-prefix-index: true
      enable-suffix-index: true
```

### Multiple Arenas

Configure multiple isolated data arenas:

```yaml
memris:
  default-arena: primary
  arenas:
    primary:
      page-size: 1024
      max-pages: 1024
    analytics:
      page-size: 4096
      max-pages: 8192
      enable-parallel-sorting: true
```

### Audit Provider (Spring Boot)

In Spring Boot, provide an `AuditProvider` bean for `@CreatedBy`/`@LastModifiedBy`:

```java
import io.memris.core.AuditProvider;
import io.memris.core.MemrisConfiguration;

@Bean
public AuditProvider auditProvider() {
    return () -> SecurityContextHolder.getContext().getAuthentication().getName();
}

@Bean
public MemrisConfiguration memrisConfiguration(AuditProvider auditProvider) {
    return MemrisConfiguration.builder()
        .auditProvider(auditProvider)
        .build();
}
```

## All Builder Options Reference

Complete list of `MemrisConfiguration.builder()` options:

| Method | Type | Default | Description |
|--------|------|---------|-------------|
| `tableImplementation()` | `TableImplementation` | `BYTECODE` | Table generation strategy |
| `pageSize()` | `int` | 1024 | Rows per page |
| `maxPages()` | `int` | 1024 | Maximum pages per table |
| `initialPages()` | `int` | 1024 | Pages to pre-allocate |
| `enableParallelSorting()` | `boolean` | true | Enable parallel sort for large results |
| `parallelSortThreshold()` | `int` | 1000 | Row count to trigger parallel sort |
| `codegenEnabled()` | `boolean` | true | Enable runtime code generation |
| `enablePrefixIndex()` | `boolean` | true | Enable prefix index for `startsWith` |
| `enableSuffixIndex()` | `boolean` | true | Enable suffix index for `endsWith` |
| `auditProvider()` | `AuditProvider` | null | Provider for audit fields |
| `entityMetadataProvider()` | `EntityMetadataProvider` | default | Custom metadata extractor |

## Performance Tuning Guide

### For Read-Heavy Workloads

```java
MemrisConfiguration config = MemrisConfiguration.builder()
    .pageSize(2048)  // Larger pages for better cache locality
    .initialPages(2048)
    .enableParallelSorting(true)
    .parallelSortThreshold(500)  // Sort in parallel more aggressively
    .build();
```

### For Write-Heavy Workloads

```java
MemrisConfiguration config = MemrisConfiguration.builder()
    .pageSize(1024)
    .initialPages(4096)  // Pre-allocate more pages
    .maxPages(8192)
    .enableParallelSorting(false)  // Reduce overhead
    .build();
```

### For Memory-Constrained Environments

```java
MemrisConfiguration config = MemrisConfiguration.builder()
    .pageSize(512)  // Smaller pages
    .maxPages(512)
    .initialPages(256)
    .build();
```

## Configuration Validation

Memris validates configuration at startup:

```java
// Invalid: maxPages must be >= initialPages
MemrisConfiguration.builder()
    .initialPages(1000)
    .maxPages(100)  // Throws IllegalArgumentException
    .build();

// Invalid: pageSize must be positive
MemrisConfiguration.builder()
    .pageSize(0)  // Throws IllegalArgumentException
    .build();
```

## Environment-Based Configuration

### Using System Properties

```bash
java -Dmemris.pageSize=2048 -Dmemris.maxPages=4096 -jar app.jar
```

### Using Environment Variables

```bash
export MEMRIS_PAGESIZE=2048
export MEMRIS_MAXPAGES=4096
```

!!! note "Spring Boot"
    Environment variables are automatically mapped when using Spring Boot integration. See [Spring Boot Configuration Properties](../spring-boot/configuration-properties.md).
