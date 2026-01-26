# Memris Architecture Documentation

## Overview & Design Principles

Memris is a blazingly fast, multi-threaded, in-memory storage engine for Java 21 with Spring Data JPA repository integration via dynamic bytecode generation.

**Key Design Principles:**
1. **O(1) First** - No O(n) operations allowed in hot paths
2. **Zero Reflection Hot Path** - No reflection in runtime query execution
3. **No Maps/String Lookups Hot Path** - Runtime MUST use index-based access only
4. **Primitive-Only APIs** - No boxed types in hot paths
5. **Compile Once, Reuse Forever** - All compilation happens at repository creation time
6. **Generic Pattern Matching** - No per-repository hardcoding

---

## High-Level Architecture

```plantuml
@startuml Memris Architecture
skinparam monochrome true

[User Code] as UC
[Repository Interface] as RI
[MemrisRepositoryFactory] as Factory

package "Build-Time Components" {
  [MetadataExtractor] as ME
  [EntityMetadata] as EM
  [QueryPlanner] as QP
  [QueryCompiler] as QC
  [RepositoryScaffolder] as RS
  [RepositoryEmitter] as RE

  ME -right-> EM
  QC -right-> QP
  RS -down-> RE
}

package "Runtime Engine" {
  [RepositoryRuntime] as RT
  [CompiledQuery[]] as CQ
  [Dense Arrays] as DA
  [FfmTable] as FT
  [Indexes] as IX
}

package "Generated Repository" {
  [UserRepositoryImpl] as Impl
}

UC -down-> RI
RI -down-> Factory
Factory -right-> ME
Factory -right-> RS

RS -right-> RT
RE -right-> Impl

Impl -right-> RT : queryId dispatch

RT -down-> CQ : Array lookup (O1)
RT -down-> DA : Pre-compiled metadata
RT -down-> FT : Column-indexed access
RT -down-> IX : Index queries

DA -right-> RT : String[] columnNames\nbyte[] typeCodes\nMethodHandle[] setters

FT -right-> [SIMD Vectors] : Vector scans

note right of Factory
  Factory Responsibilities:
  • Arena lifecycle management
  • FfmTable creation per entity
  • Index creation and management
  • Join table management
  • Repository instantiation
end note

note left of RT
  RepositoryRuntime:
  Zero reflection hot path
  • list0/1/2(queryId, args)
  • optional1(queryId, arg)
  • exists1(queryId, arg)
  • count0/1/2(queryId, args)
  • TypeCode switch dispatch
  • Dense array access
  • MethodHandle materialization
end note

note bottom of FT
  FfmTable Storage:
  • Column-indexed API
    getInt(columnId, row)
    getString(columnId, row)
    ... all primitives
  • SIMD vector scans
    (int/long columns)
  • Off-heap MemorySegments
end note

@enduml
```

---

## Layer Responsibilities (SRP)

Memris follows strict Single Responsibility Principle with 7 distinct layers:

### Layer 1: Domain Layer (No dependencies on other layers)

**Package:** `io.memris.spring.domain`

**Components:**
- `EntityStructure` - Record: pre-processed entity metadata
- `FieldMapping` - Record: field to column mapping
- `RelationshipType` - Enum: ONE_TO_ONE, MANY_TO_ONE, etc.
- `QueryToken` - Record: lexer token
- `QueryTokenType` - Enum: token types

**Responsibilities:**
- Define immutable data structures (records)
- No behavior, just data
- No dependencies on other layers
- Used by parser, compiler, and runtime layers

### Layer 2: Parser Layer (Domain → Tokens)

**Package:** `io.memris.spring.plan`

**Components:**
- `QueryMethodLexer` - ONLY tokenizes method names
- `QueryMethodToken` - Token record
- `QueryMethodTokenType` - Token type enum
- `QueryPlanner` - ONLY converts tokens to LogicalQuery

**Responsibilities:**
- **QueryMethodLexer**: String → List<QueryMethodToken>
  - Input: Method name string (e.g., "findByDepartmentNameIgnoreCase")
  - Output: List of tokens (PROPERTY_PATH, OPERATOR, etc.)
  - Does NOT validate against entity structure
  - Does NOT create LogicalQuery
  - Pure lexical analysis

- **QueryPlanner**: List<QueryMethodToken> → LogicalQuery
  - Input: List of tokens + entity class
  - Output: LogicalQuery with conditions/operators
  - Validates tokens against entity structure
  - Creates semantic query representation
  - Handles combinators (AND/OR), IgnoreCase, OrderBy

**SRP Boundaries:**
- Lexer: "What words are in this string?"
- Planner: "What does this sequence of tokens mean?"

### Layer 3: Compiler Layer (LogicalQuery → CompiledQuery)

**Package:** `io.memris.spring.plan`

**Components:**
- `QueryCompiler` - ONLY compiles LogicalQuery to CompiledQuery
- `CompiledQuery` - Record: executable query
- `LogicalQuery` - Record: semantic query

**Responsibilities:**
- **QueryCompiler**: LogicalQuery → CompiledQuery
  - Input: LogicalQuery with property paths (e.g., "department.address.city")
  - Output: CompiledQuery with column indices (e.g., columnIndex=5)
  - Resolves property paths to column indices using EntityMetadata
  - No query execution, just compilation
  - Happens once per query method during repository creation

### Layer 4: Metadata Layer (Entity → Structure)

**Package:** `io.memris.spring.metadata`

**Components:**
- `EntityMetadata` - Record: complete entity structure
- `FieldMapping` - Record: field to column mapping
- `MetadataExtractor` - Extracts entity structure via reflection
- `TypeCode` - Enum: type constants

**Responsibilities:**
- **MetadataExtractor**: Class<?> → EntityMetadata
  - Input: Entity class
  - Output: EntityMetadata with all field mappings
  - Uses reflection once per entity class (not in hot path)
  - Caches results for reuse
  - Extracts: field names, types, column names, type codes, MethodHandles

### Layer 5: Runtime Layer (Query Execution)

**Package:** `io.memris.spring.runtime`

**Components:**
- `RepositoryRuntime` - Executes compiled queries
- `QueryExecutor` - Executes single query (extract from Runtime)
- `EntityMaterializer` - Materializes entities from rows (extract from Runtime)

**Responsibilities:**
- **RepositoryRuntime**: Query execution engine
  - Executes compiled queries against FfmTable
  - Returns results (List, Optional, long, boolean)
  - Materializes entities using MethodHandles
  - Zero reflection in hot path
  - Uses dense arrays for O(1) access: columnNames[], typeCodes[], converters[], setters[]
  - Query method recognition: tokenized via QueryMethodLexer (prefixes: find, read, query, get, count, exists)
  - CRUD method recognition: by signature (save, deleteById, etc.)

### Layer 6: Emitter Layer (ByteBuddy Code Generation)

**Package:** `io.memris.spring.scaffold`

**Components:**
- `RepositoryEmitter` - Generates bytecode via ByteBuddy
- `RepositoryMethodImplementation` - Strategy interface for method generation
- `QueryMethodImpl` - Query method bytecode generation
- `CrudMethodImpl` - CRUD method bytecode generation
- `GeneratedClassNamer` - Generates unique class names

**Responsibilities:**
- **RepositoryEmitter**: ByteBuddy bytecode generation
  - Input: Repository interface + compiled queries + factory reference
  - Output: Generated repository class instance
  - Generates ALL methods (query + CRUD)
  - Delegates query methods to RepositoryRuntime via typed entrypoints
  - Delegates CRUD methods to CrudOperationExecutor via MethodDelegation
  - Uses WRAPPER classloading strategy to avoid "already loaded" errors

- **RepositoryMethodImplementation**: Strategy interface
  - Defines contract for implementing repository methods
  - Context provides: factory, table, entityClass, compiledQueries

### Layer 7: Factory Layer (Orchestration & CRUD Operations)

**Package:** `io.memris.spring`

**Components:**
- `MemrisRepositoryFactory` - Facade/entry point ONLY
- `TableManager` - Manages table creation/retrieval
- `CrudOperationExecutor` - Executes CRUD operations
- `RepositoryScaffolder` - Orchestrates repository creation

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

## Build-Time vs Runtime Pipelines

**CRITICAL:** Compilation happens ONCE at repository creation time. Runtime NEVER runs lexer/planner/compiler.

### Build-Time Pipeline (Once per repository)

```plantuml
@startuml
package "Build-Time (Repository Creation)" {
    rectangle "Methods" {
        method "findById(Long)" as m1
        method "findByName(String)" as m2
        method "save(T)" as m3
    }

    [QueryMethodLexer] as Lexer
    [QueryPlanner] as Planner
    [QueryCompiler] as Compiler
    [RepositoryEmitter] as Emitter

    package "Compiled Artifacts" {
        [CompiledQuery[]] as CQArray
        [RepositoryPlan<T>] as Plan
    }

    m1 --> Lexer : tokenize(methodName)
    m2 --> Lexer : tokenize(methodName)
    m3 --> Lexer : recognized by signature (not tokenized)

    Lexer --> Planner : List<Token>
    Planner --> Compiler : LogicalQuery

    Compiler --> CQArray : CompiledQuery per query method

    CQArray --> Plan : RepositoryPlan
    Plan --> Emitter : emit bytecode

    note right of Plan
      RepositoryPlan contains:
      - CompiledQuery[] queries
      - EntityMaterializer<T>
      - EntityExtractor<T>
      - RuntimeKernel with column arrays
    end note

    note right of m3
      CRUD methods recognized
      by signature, NOT
      tokenized through lexer
      (save, deleteById, etc.)
    end note
}
@enduml
```

### Runtime Pipeline (Per invocation)

```plantuml
@startuml
actor User

package "Generated Repository" {
    method "findById(Long id)" as findById
    method "save(T entity)" as save
}

package "RuntimeKernel" {
    [RepositoryRuntime] as Runtime
    [FfmTable] as Table
}

package "Storage" {
    [IntColumn] as IntCol
    [StringColumn] as StrCol
}

User -> findById : call
findById -> Runtime : optional1(Q_FIND_BY_ID, id)
activate Runtime

Runtime -> Runtime : cq = compiledQueries[queryId]
Runtime -> IntCol : scanEquals(columnIdx, value)
activate IntCol
IntCol --> Runtime : SelectionVector
deactivate IntCol

Runtime -> Runtime : materialize(entity, rowIndex)
Runtime --> findById : Optional<T>
deactivate Runtime

findById --> User : return

note right of findById
  Generated method is
  a thin wrapper:
  return runtime.optional1(
    Q_FIND_BY_ID, id);
end note

note right of Runtime
  NO lexer/planner/compiler
  NO string-based column lookups
  NO reflection
  Direct array access only
end note
@enduml
```

### Runtime Hard Requirements

**MUST NOT:**
- Call `FfmTable.getX(String columnName, ...)` - uses Map lookup
- Call `FfmTable.column(String columnName)` - uses Map lookup
- Use reflection for field access
- Parse method names at runtime

**MUST:**
- Use `columns[columnIndex]` - array access
- Use pre-resolved column indices from CompiledCondition
- Use MethodHandles pre-compiled at build time
- Dispatch by queryId (integer)

---

## Performance Characteristics

### O(1) Guarantees

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

### Hot Path (Execution) Characteristics

- No string allocations
- No reflection
- Direct array access
- SIMD vector scans (where applicable)

---

## Key Design Decisions

### 1. O(1) First, O(log n) Second, O(n) Forbidden

**Decision:** All hot path operations must be O(1)

**Rationale:**
- String lookups are O(n)
- Array access is O(1)
- MethodHandles faster than reflection
- Pre-compile everything at build time

### 2. Unified Compilation Strategy

**Decision:** All methods (query + CRUD) become CompiledQuery with assigned queryIds

**Rationale:**
- Query methods: Lexer → Planner → Compiler (pattern-based)
- CRUD methods: Direct Compiler invocation (signature-based)
- Unified runtime dispatch via queryId integer
- No per-repository hardcoding; patterns are generic across all repositories

### 3. ReturnKind Enum

**Decision:** ReturnKind describes both operation type and return type

**Rationale:**
- No separate OperationType enum needed
- Return type determines runtime method dispatch
- Simple and explicit

### 4. Zero Reflection Hot Path

**Decision:** Pre-compile everything at build time, use integer indices at runtime

**Rationale:**
- String lookups are O(n)
- Array access is O(1)
- MethodHandles faster than reflection
- Hot path has zero reflection overhead

### 5. Strategy Pattern for Method Generation

**Decision:** RepositoryMethodImplementation interface with concrete implementations

**Rationale:**
- Open/Closed Principle: easy to add new method types
- Each implementation is independently testable
- Clear delegation targets (RepositoryRuntime vs CrudOperationExecutor)

---

## References

- **Storage Deep Dive:** [design/storage.md](design/storage.md)
- **Selection Pipeline:** [design/selection.md](design/selection.md)
- **Query Parsing:** [design/query-parsing.md](design/query-parsing.md)
- **User Guidelines:** [../CLAUDE.md](../CLAUDE.md)
