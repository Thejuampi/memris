# SRP Layer Separation - Repository Architecture

## Current Architecture Analysis

### Component Responsibilities (Current State)

| Component | Current Responsibilities | SRP Violations |
|-----------|------------------------|----------------|
| **MemrisRepositoryFactory** | • Table creation/management<br>• Nested entity table building<br>• Join table creation<br>• Enum caching<br>• ID generation<br>• Index management<br>• Sorting configuration<br>• Repository creation delegation | ❌ Does too much - should only be a facade/entry point |
| **RepositoryScaffolder** | • Entity metadata extraction coordination<br>• Query method extraction<br>• Query planning/compilation orchestration<br>• RepositoryRuntime building<br>• Repository generation delegation | ✅ Good orchestration role |
| **RepositoryEmitter** | • ByteBuddy class generation<br>• Constructor generation<br>• Query method generation only | ⚠️ Only handles query methods, missing CRUD |
| **QueryMethodLexer** | • Method name tokenization<br>• Entity metadata caching<br>• Property validation | ✅ Clear single responsibility |
| **QueryPlanner** | • Token processing<br>• LogicalQuery creation | ✅ Clear single responsibility |
| **QueryCompiler** | • Property path to column index resolution<br>• CompiledQuery creation | ✅ Clear single responsibility |
| **RepositoryRuntime** | • Query execution<br>• Entity materialization<br>• Type conversion dispatch | ✅ Clear single responsibility |
| **EntityMetadata/MetadataExtractor** | • Entity structure extraction<br>• Field mappings<br>• Type codes<br>• Method handles | ✅ Clear single responsibility |

---

## Proposed Clean Architecture (SRP)

### Layer 1: Domain Layer (No dependencies on other layers)

```
io.memris.spring.domain
├── entity
│   ├── EntityStructure.java         // Record: pre-processed entity metadata
│   ├── FieldMapping.java             // Record: field → column mapping
│   └── RelationshipType.java         // Enum: ONE_TO_ONE, MANY_TO_ONE, etc.
└── query
    ├── ParsedQuery.java              // Record: parsed method result (future)
    ├── QueryToken.java               // Record: lexer token
    └── QueryTokenType.java           // Enum: token types (future)
```

**Responsibilities:**
- Define immutable data structures (records)
- No behavior, just data
- No dependencies on other layers
- Used by parser, compiler, and runtime layers

**Note:** The `plan` package will continue to use `LogicalQuery` and `CompiledQuery` records. The `domain.query` package is reserved for future enhancements (e.g., caching parsed queries, AST representation).

---

### Layer 2: Parser Layer (Domain → Tokens)

```
io.memris.spring.plan
├── QueryMethodLexer.java             // ONLY tokenizes method names
├── QueryMethodToken.java             // Token record (existing)
├── QueryMethodTokenType.java         // Token type enum (existing)
└── QueryPlanner.java                 // ONLY converts tokens → LogicalQuery
```

**Responsibilities:**
- `QueryMethodLexer`: String → List<QueryMethodToken>
  - Input: Method name string (e.g., "findByDepartmentNameIgnoreCase")
  - Output: List of tokens (PROPERTY_PATH, OPERATOR, etc.)
  - Does NOT validate against entity structure
  - Does NOT create LogicalQuery
  - Pure lexical analysis

- `QueryPlanner`: List<QueryMethodToken> → LogicalQuery
  - Input: List of tokens + entity class (future: EntityMetadata)
  - Output: LogicalQuery with conditions/operators
  - Validates tokens against entity structure
  - Creates semantic query representation
  - Handles combinators (AND/OR), IgnoreCase, OrderBy

**SRP Boundaries:**
- Lexer: "What words are in this string?"
- Planner: "What does this sequence of tokens mean?"

---

### Layer 3: Compiler Layer (LogicalQuery → CompiledQuery)

```
io.memris.spring.plan
├── QueryCompiler.java                 // ONLY compiles LogicalQuery → CompiledQuery
├── CompiledQuery.java                 // Record: executable query
└── LogicalQuery.java                  // Record: semantic query
```

**Responsibilities:**
- `QueryCompiler`: LogicalQuery → CompiledQuery
  - Input: LogicalQuery with property paths (e.g., "department.address.city")
  - Output: CompiledQuery with column indices (e.g., columnIndex=5)
  - Resolves property paths to column indices using EntityMetadata
  - No query execution, just compilation
  - Happens once per query method during repository creation

---

### Layer 4: Metadata Layer (Entity → Structure)

```
io.memris.spring.metadata
├── EntityMetadata.java               // Record: complete entity structure
├── FieldMapping.java                  // Record: field → column mapping
├── MetadataExtractor.java            // Extracts entity structure via reflection
└── TypeCode.java                      // Enum: type constants
```

**Responsibilities:**
- `MetadataExtractor`: Class<?> → EntityMetadata
  - Input: Entity class
  - Output: EntityMetadata with all field mappings
  - Uses reflection once per entity class (not in hot path)
  - Caches results for reuse
  - Extracts: field names, types, column names, type codes, MethodHandles

---

### Layer 5: Runtime Layer (Query Execution)

```
io.memris.spring.runtime
├── RepositoryRuntime.java            // Executes compiled queries
├── QueryExecutor.java                // Executes single query (extract from Runtime)
└── EntityMaterializer.java           // Materializes entities from rows (extract from Runtime)
```

**Responsibilities:**
- `RepositoryRuntime`: Query execution engine
  - Executes compiled queries against FfmTable
  - Returns results (List, Optional, long, boolean)
  - Materializes entities using MethodHandles
  - Zero reflection in hot path
  - Uses dense arrays for O(1) access: columnNames[], typeCodes[], converters[], setters[]

---

### Layer 6: Emitter Layer (ByteBuddy Code Generation)

```
io.memris.spring.scaffold
├── RepositoryEmitter.java            // Generates bytecode via ByteBuddy
├── RepositoryMethodImplementation.java // Strategy interface for method generation
├── QueryMethodImpl.java              // Query method bytecode generation
├── CrudMethodImpl.java               // CRUD method bytecode generation
└── GeneratedClassNamer.java          // Generates unique class names
```

**Responsibilities:**
- `RepositoryEmitter`: ByteBuddy bytecode generation
  - Input: Repository interface + compiled queries + factory reference
  - Output: Generated repository class instance
  - Generates ALL methods (query + CRUD)
  - Delegates query methods to RepositoryRuntime via typed entrypoints
  - Delegates CRUD methods to CrudOperationExecutor via MethodDelegation
  - Uses WRAPPER classloading strategy to avoid "already loaded" errors

- `RepositoryMethodImplementation`: Strategy interface
  - Defines contract for implementing repository methods
  - Context provides: factory, table, entityClass, compiledQueries

---

### Layer 7: Factory Layer (Orchestration & CRUD Operations)

```
io.memris.spring
├── MemrisRepositoryFactory.java      // Facade/entry point ONLY
├── TableManager.java                 // Manages table creation/retrieval
├── CrudOperationExecutor.java        // Executes CRUD operations
└── RepositoryScaffolder.java        // Orchestrates repository creation
```

**Responsibilities:**

#### MemrisRepositoryFactory (Facade)
- Entry point for users
- Delegates to specialized components
- Manages lifecycle (Arena, close)
- Does NOT implement business logic
- Provides configuration options (sorting, etc.)

#### TableManager
- Creates tables for entities (buildTable, buildNestedEntityTables)
- Creates join tables (buildJoinTables)
- Caches tables for reuse
- Manages table lookup

#### CrudOperationExecutor
- Implements: save(), saveAll(), delete(), deleteAll(), deleteAllById(), findAll()
- Called by generated repository methods via MethodDelegation
- Manipulates FfmTable directly
- Handles ID generation

#### RepositoryScaffolder
- Orchestrates repository creation
- Coordinates: metadata extraction → planning → compilation → runtime → emission
- Returns final repository instance
- Pure coordination, no business logic

---

## Current Issues vs. Proposed Solution

### Issue 1: Missing CRUD Methods

**Current:** `RepositoryEmitter` only generates query methods

**Solution:** Add `RepositoryMethodImplementation` strategy pattern:

```java
public interface RepositoryMethodImplementation {
    DynamicType.Builder<?> implement(
        DynamicType.Builder<?> builder,
        Method method,
        EmissionContext ctx);
}

// Context for method generation
public record EmissionContext(
    MemrisRepositoryFactory factory,
    CrudOperationExecutor crudExecutor,
    FfmTable table,
    Class<?> entityClass,
    EntityMetadata<?> metadata,
    CompiledQuery[] compiledQueries
) {}

// Query method: delegates to RepositoryRuntime
public class QueryMethodImpl implements RepositoryMethodImplementation {
    @Override
    public DynamicType.Builder<?> implement(
            DynamicType.Builder<?> builder,
            Method method,
            EmissionContext ctx) {

        // Find the compiled query for this method
        CompiledQuery cq = findCompiledQuery(ctx.compiledQueries(), method.getName());
        String runtimeMethod = determineRuntimeMethod(cq);
        int queryId = findQueryId(ctx.compiledQueries(), cq);

        // Build method call: rt.<runtimeMethod>(queryId, arg0, arg1, ...)
        MethodCall call = MethodCall.invoke(named(runtimeMethod))
                .onField("runtime")
                .withArgument(queryId);

        // Add method arguments
        for (int i = 0; i < method.getParameterCount(); i++) {
            call = call.withArgument(i);
        }

        return builder.defineMethod(method.getName(), method.getReturnType(), Visibility.PUBLIC)
                .withParameters(method.getParameterTypes())
                .intercept(MethodDelegation.to(new RuntimeMethodInterceptor(queryId, runtimeMethod)));
    }
}

// CRUD method: delegates to CrudOperationExecutor
public class SaveMethodImpl implements RepositoryMethodImplementation {
    @Override
    public DynamicType.Builder<?> implement(
            DynamicType.Builder<?> builder,
            Method method,
            EmissionContext ctx) {

        // Generate: return crudExecutor.save(entity, table, factory);
        return builder.defineMethod(method.getName(), method.getReturnType(), Visibility.PUBLIC)
                .withParameters(method.getParameterTypes())
                .intercept(MethodDelegation.to(ctx.crudExecutor()));
    }
}

public class DeleteAllMethodImpl implements RepositoryMethodImplementation {
    @Override
    public DynamicType.Builder<?> implement(
            DynamicType.Builder<?> builder,
            Method method,
            EmissionContext ctx) {

        // Generate: crudExecutor.deleteAll(table);
        return builder.defineMethod(method.getName(), method.getReturnType(), Visibility.PUBLIC)
                .intercept(MethodCall.invoke(CrudOperationExecutor.class)
                        .method("deleteAll")
                        .withArgument(table(ctx.table()))
                        .withArgument(factory(ctx.factory())));
    }
}
```

### Issue 2: MemrisRepositoryFactory Does Too Much

**Current:** Factory handles tables, nested entities, joins, enums, indexes, sorting

**Solution:** Extract to specialized components:
- `TableManager` - table lifecycle
- `CrudOperationExecutor` - CRUD operations
- `RepositoryScaffolder` - orchestrates creation (keep existing name)

### Issue 3: "Cannot Inject Already Loaded Type"

**Current:** INJECTION strategy fails on regeneration

**Solution:** Use WRAPPER strategy + unique class names:

```java
public class GeneratedClassNamer {
    private final AtomicLong classId = new AtomicLong(0);

    public String generateName(Class<?> repositoryInterface) {
        long id = classId.getAndIncrement();
        return repositoryInterface.getSimpleName() + "$MemrisImpl$" + id;
    }
}
```

### Issue 4: EntityMetadata vs Class<?> in QueryPlanner

**Current:** QueryPlanner receives entityClass and does reflection during planning

**Solution (Phase 5):** Replace with EntityMetadata record (future enhancement)
- Pre-process all entity structure once
- Pass EntityMetadata instead of Class<?>
- Zero reflection during query planning

---

## Implementation Plan

### Phase 1: Extract TableManager (Foundation)
**Goal:** Remove table management responsibility from MemrisRepositoryFactory

1. Create `io.memris.spring.TableManager` class
2. Move methods from factory:
   - `buildTable()` → TableManager.getOrCreateTable()
   - `buildNestedEntityTables()` → TableManager.ensureNestedTables()
   - `buildJoinTables()` → TableManager.ensureJoinTables()
   - `cacheEnumValues()` → TableManager.cacheEnumValues()
3. Update factory to delegate to TableManager
4. Tests: Verify table creation still works

### Phase 2: Extract CrudOperationExecutor
**Goal:** Centralize CRUD operations in dedicated class

1. Create `io.memris.spring.CrudOperationExecutor` class
2. Implement methods:
   - `save(T entity, FfmTable table, MemrisRepositoryFactory factory)`
   - `saveAll(List<T> entities, ...)`
   - `delete(T entity, ...)`
   - `deleteAll(...)`
   - `deleteAllById(List<ID> ids, ...)`
   - `findAll()`
3. Handle ID generation via factory
4. Tests: Verify CRUD operations work

### Phase 3: Fix RepositoryEmitter (Method Generation)
**Goal:** Generate ALL methods (query + CRUD) with proper delegation

1. Create `RepositoryMethodImplementation` interface
2. Create `EmissionContext` record
3. Implement `QueryMethodImpl` - delegates to RepositoryRuntime
4. Implement CRUD implementations:
   - `SaveMethodImpl` - delegates to CrudOperationExecutor
   - `DeleteMethodImpl` - delegates to CrudOperationExecutor
   - `DeleteAllMethodImpl` - delegates to CrudOperationExecutor
   - `FindAllMethodImpl` - delegates to CrudOperationExecutor
5. Fix classloading: Change INJECTION to WRAPPER
6. Add `GeneratedClassNamer` for unique class names
7. Update `RepositoryEmitter.emitAndInstantiate()` to:
   - Iterate over ALL methods in repository interface
   - Classify each method (query vs CRUD)
   - Apply appropriate implementation strategy
8. Tests: Verify all repository methods work

### Phase 4: Rename RepositoryScaffolder → RepositoryBuilder
**Goal:** Clarify that this component orchestrates creation

1. Rename class to `RepositoryBuilder`
2. Update all references
3. Clarify Javadoc: "Orchestrates repository creation"
4. Tests: Verify build process still works

### Phase 5: Clean Up Metadata Layer (Future Enhancement)
**Goal:** Replace Class<?> with EntityMetadata record

1. Create domain.entity.EntityStructure record with:
   - All field mappings
   - Relationship types
   - Column indices
   - Type codes
2. Update MetadataExtractor to return EntityStructure
3. Update QueryPlanner to accept EntityStructure
4. Remove reflection from QueryPlanner (zero reflection during planning)
5. Tests: Verify query planning works with pre-processed metadata

---

## File Structure After Refactoring

```
io.memris.spring/
├── MemrisRepositoryFactory.java      // Facade (entry point)
├── TableManager.java                 // Table lifecycle
├── CrudOperationExecutor.java        // CRUD operations
├── RepositoryScaffolder.java         // Creation orchestration (renamed from Builder)
├── domain/
│   ├── entity/
│   │   ├── EntityStructure.java      // NEW: Pre-processed entity metadata
│   │   ├── FieldMapping.java         // Moved from root
│   │   └── RelationshipType.java     // NEW: Relationship enum
│   └── query/
│       ├── ParsedQuery.java          // NEW: Future AST representation
│       ├── QueryToken.java           // NEW: Moved from plan
│       └── QueryTokenType.java       // NEW: Moved from plan
├── metadata/
│   ├── EntityMetadata.java           // Moved from root, enhanced
│   └── MetadataExtractor.java        // Moved from root
├── plan/
│   ├── QueryMethodLexer.java         // String → Tokens
│   ├── QueryPlanner.java             // Tokens → LogicalQuery
│   ├── QueryCompiler.java             // LogicalQuery → CompiledQuery
│   ├── LogicalQuery.java             // Semantic query record
│   └── CompiledQuery.java            // Executable query record
├── runtime/
│   ├── RepositoryRuntime.java        // Query execution
│   ├── QueryExecutor.java            // NEW: Extracted from Runtime
│   └── EntityMaterializer.java       // NEW: Extracted from Runtime
└── scaffold/
    ├── RepositoryEmitter.java        // ByteBuddy generation
    ├── RepositoryMethodImplementation.java  // Strategy interface
    ├── MethodInterceptor.java        // Runtime interceptor (renamed from MethodInterceptor)
    ├── QueryMethodImpl.java          // Query method strategy
    ├── SaveMethodImpl.java           // CRUD: save()
    ├── SaveAllMethodImpl.java        // CRUD: saveAll()
    ├── DeleteMethodImpl.java         // CRUD: delete()
    ├── DeleteAllMethodImpl.java      // CRUD: deleteAll()
    ├── DeleteAllByIdMethodImpl.java  // CRUD: deleteAllById()
    ├── FindAllMethodImpl.java        // CRUD: findAll()
    └── GeneratedClassNamer.java      // Unique naming
```

---

## Summary of Responsibilities

| Layer | Component | Single Responsibility |
|--------|-----------|----------------------|
| **Entry** | MemrisRepositoryFactory | Facade - delegates to specialists |
| **Storage** | TableManager | Create/cache/manage tables |
| **CRUD** | CrudOperationExecutor | Execute save/delete operations |
| **Build** | RepositoryScaffolder | Orchestrate repository creation |
| **Metadata** | MetadataExtractor | Extract entity structure via reflection |
| **Domain** | EntityStructure | Pre-processed entity metadata (future) |
| **Parse** | QueryMethodLexer | Tokenize method name strings |
| **Plan** | QueryPlanner | Convert tokens to semantic queries |
| **Compile** | QueryCompiler | Resolve property paths to indices |
| **Execute** | RepositoryRuntime | Execute compiled queries |
| **Generate** | RepositoryEmitter | Generate bytecode via ByteBuddy |

---

## Performance Validation

### O(1) Guarantees Maintained

| Layer | Operation | Complexity | Notes |
|-------|-----------|------------|-------|
| **Parser** | Method name tokenization | O(n) | Build-time only, n = method name length |
| **Planner** | Token → LogicalQuery | O(m) | Build-time only, m = token count |
| **Compiler** | Property path resolution | O(f) | Build-time only, f = field count |
| **Runtime** | Query execution lookup | O(1) | Array access by queryId |
| **Runtime** | Column access | O(1) | Direct array index |
| **Runtime** | Entity materialization | O(r * f) | r = row count, f = field count |

### Hot Path Analysis

**Query Execution Hot Path:**
```
repo.findByDepartmentAndStatus("Sales", "active")
        │
        ▼ (generated stub)
RepositoryRuntime.list2(queryId=0, "Sales", "active")
        │
        ├─→ compiledQueries[0] → O(1) array lookup
        ├─→ executeQuery() → SIMD scan with O(n/VECTOR_WIDTH)
        └─→ materialize() → O(result_size * field_count)
```

**No Reflection in Hot Path:**
- ✅ No `Method.invoke()` calls
- ✅ No `Field.get()` / `Field.set()` calls
- ✅ No string-based property lookups
- ✅ Pre-compiled MethodHandles for field access
- ✅ Direct array access for metadata

---

## Key Design Decisions

### 1. Keep RepositoryScaffolder Name
**Decision:** Do NOT rename to RepositoryBuilder

**Rationale:**
- "Scaffolder" better describes the action of building a temporary structure
- "Builder" pattern typically implies fluent API with setters
- Current name is more descriptive of what it does

### 2. Keep RepositoryRuntime Name
**Decision:** Do NOT rename to QueryExecutor

**Rationale:**
- "RepositoryRuntime" indicates it's the runtime for repository queries
- "QueryExecutor" sounds like it executes a single query
- Current name accurately reflects that it holds compiled queries and runtime state

### 3. Rename MethodImplementation
**Decision:** Rename to RepositoryMethodImplementation

**Rationale:**
- More specific and descriptive
- Avoids naming conflicts with other interfaces
- Makes it clear this is for repository method generation

### 4. Extract EntityMaterializer
**Decision:** Create separate class from RepositoryRuntime

**Rationale:**
- Separates concerns: execution vs materialization
- EntityMaterializer handles row → entity conversion
- Makes both classes smaller and more focused

### 5. Use Strategy Pattern for Method Generation
**Decision:** RepositoryMethodImplementation interface with concrete implementations

**Rationale:**
- Open/Closed Principle: easy to add new method types
- Each implementation is independently testable
- Clear delegation targets (RepositoryRuntime vs CrudOperationExecutor)

---

## Migration Strategy

### Step 1: Create New Components (Non-Breaking)
- Create `TableManager` (doesn't affect existing code yet)
- Create `CrudOperationExecutor` (doesn't affect existing code yet)
- Create domain package structure
- Tests: Verify new components work independently

### Step 2: Update RepositoryEmitter (Non-Breaking)
- Add `RepositoryMethodImplementation` interface
- Implement query methods (existing behavior)
- Implement CRUD methods (new capability)
- Tests: Verify both query and CRUD methods work

### Step 3: Refactor Factory (Breaking)
- Delegate table operations to TableManager
- Delegate CRUD operations to CrudOperationExecutor
- Update factory to use RepositoryScaffolder
- Tests: Verify all tests still pass

### Step 4: Clean Up (Polish)
- Extract EntityMaterializer from RepositoryRuntime
- Create domain.entity.EntityStructure
- Update QueryPlanner to use EntityStructure
- Tests: Verify zero-reflection goal maintained
