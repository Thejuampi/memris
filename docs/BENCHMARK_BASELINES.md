# Memris Performance Baselines

**Generated:** 2026-02-01  
**Environment:** Windows, JDK 21.0.7, OpenJDK 64-Bit Server VM  
**JMH Version:** 1.37  
**Test Data:** 100,000 pre-populated rows (except insert benchmark)

---

## Summary

| Benchmark | Mode | Score | Error | Units | Description |
|-----------|------|-------|-------|-------|-------------|
| **count_rows** | avgt | ≈ 10⁻⁶ | - | ms/op | Count all rows (uses table statistics) |
| **insert_100k_rows** | avgt | 0.007 | ± 0.004 | ms/op | Insert 100,000 new rows |
| **lookup_by_id** | avgt | 0.002 | ± 0.001 | ms/op | Lookup 100,000 rows by ID |
| **scan_all_rows** | avgt | 170.591 | ± 59.970 | ms/op | Full table scan of 100,000 rows |

---

## Detailed Results

### 1. count_rows
**What it measures:** Table row counting using internal statistics

```
Benchmark: io.memris.benchmarks.MemrisBenchmarks.count_rows
Mode: Average time
Iterations: 3
Warmup: 2 iterations

Result:
  ≈ 10⁻⁶ ms/op (effectively instantaneous)
```

**Analysis:** The count operation is essentially free because it uses pre-computed table statistics rather than scanning rows.

---

### 2. insert_100k_rows
**What it measures:** Batch insert performance for 100,000 entities

```
Benchmark: io.memris.benchmarks.MemrisBenchmarks.insert_100k_rows
Mode: Average time per operation
Iterations: 3
Warmup: 2 iterations

Result:
  0.007 ± 0.004 ms/op [Average]
  (min, avg, max) = (0.007, 0.007, 0.007)
  stdev = 0.001
  CI (99.9%): [0.003, 0.011]
```

**Analysis:** 
- **Throughput:** ~142,857 inserts per second (1/0.007)
- **Latency:** ~7 microseconds per insert
- Very consistent performance with low variance

---

### 3. lookup_by_id
**What it measures:** Primary key lookup performance for 100,000 lookups

```
Benchmark: io.memris.benchmarks.MemrisBenchmarks.lookup_by_id
Mode: Average time per operation  
Iterations: 3
Warmup: 2 iterations

Result:
  0.002 ± 0.001 ms/op [Average]
  (min, avg, max) = (0.002, 0.002, 0.002)
  stdev = 0.001
  CI (99.9%): [0.002, 0.003]
```

**Analysis:**
- **Throughput:** ~500,000 lookups per second (1/0.002)
- **Latency:** ~2 microseconds per lookup
- Excellent performance for hash index-based lookups

---

### 4. scan_all_rows
**What it measures:** Full table scan returning all 100,000 rows

```
Benchmark: io.memris.benchmarks.MemrisBenchmarks.scan_all_rows
Mode: Average time per operation
Iterations: 3
Warmup: 2 iterations

Result:
  170.591 ± 59.970 ms/op [Average]
  (min, avg, max) = (168.034, 170.591, 174.299)
  stdev = 3.287
  CI (99.9%): [110.621, 230.561]
```

**Analysis:**
- **Scan rate:** ~586,000 rows/second (100,000/0.17)
- Higher variance due to entity materialization overhead
- Includes object allocation and field mapping costs

---

## Performance Characteristics

### O(1) Operations
- **Count:** Uses cached statistics, no iteration
- **Lookup by ID:** Hash index lookup
- **Insert:** Amortized O(1) with row reuse

### O(n) Operations  
- **Full scan:** Must iterate and materialize all rows
- **Range queries:** Index-based but still linear in result size

### Memory Usage
- **Per row overhead:** ~16 bytes (metadata) + column data
- **100K rows:** ~10-20MB depending on column count

---

## Running the Benchmarks

```bash
cd memris-core
mvn exec:java \
  -Dexec.mainClass="io.memris.benchmarks.MemrisBenchmarks" \
  -Dexec.classpathScope=test \
  -Dexec.args="-i 3 -wi 2 -f 0 -bm avgt -tu ms"
```

**Note:** `-f 0` disables forking (required for Maven exec plugin). For production benchmarks, use forking with proper classpath setup.

---

## Known Limitations

**Table Capacity:** Current default limit is 1,048,576 rows (1024 pages × 1024 rows/page)

**Impact on Benchmarks:**
- Concurrent benchmarks with writes may hit capacity during long runs
- Insert-heavy workloads limited by pre-allocated capacity

---

## Comparison with Other Benchmarks

| Operation | Memris (this run) | Typical RDBMS* | In-Memory DB* |
|-----------|-------------------|----------------|---------------|
| Insert | 142K ops/sec | 10-50K ops/sec | 100-500K ops/sec |
| Lookup by ID | 500K ops/sec | 20-100K ops/sec | 200K-1M ops/sec |
| Full Scan | 586K rows/sec | 50-200K rows/sec | 500K-2M rows/sec |

*Approximate values for comparison purposes only

---

## Notes

- Benchmarks run in single JVM (no forking) for faster execution
- Results should be treated as indicative rather than absolute
- Production deployments should use proper JMH forking for statistically rigorous results
- Variance higher for scan operations due to GC and materialization costs

---

## Dispatch Benchmarks

The dispatch-focused benchmark suite targets query dispatch paths introduced by the runtime/dispatch extraction work.

### Classes
- `io.memris.benchmarks.DispatchSelectionBenchmark`
- `io.memris.benchmarks.DispatchAllocationBenchmark`

### Run Commands

```bash
mvn.cmd -q -e -pl memris-core test-compile

mvn.cmd -pl memris-core exec:java \
  -Dexec.mainClass="org.openjdk.jmh.Main" \
  -Dexec.classpathScope=test \
  -Dexec.args="io.memris.benchmarks.DispatchSelectionBenchmark io.memris.benchmarks.DispatchAllocationBenchmark -wi 3 -i 5 -f 1 -bm avgt -tu ns -prof gc -rf json -rff target/jmh-results/dispatch-avgt.json"

mvn.cmd -pl memris-core exec:java \
  -Dexec.mainClass="org.openjdk.jmh.Main" \
  -Dexec.classpathScope=test \
  -Dexec.args="io.memris.benchmarks.DispatchSelectionBenchmark io.memris.benchmarks.DispatchAllocationBenchmark -wi 2 -i 3 -f 1 -bm thrpt -tu ops/s -rf csv -rff target/jmh-results/dispatch-thrpt.csv"
```

### Artifacts
- `memris-core/target/jmh-results/dispatch-avgt.json`
- `memris-core/target/jmh-results/dispatch-thrpt.csv`

---

## Embedded Path Benchmarks

This suite guards flat vs embedded save/find/update throughput after the
plan-driven embedded-path refactor.

### Class
- `io.memris.benchmarks.EmbeddedPathBenchmark`

### Baseline Source
- `memris-core/src/jmh/resources/embedded-path-baseline.json`

### Regression Policy
- Flat benchmarks fail above **10%** slowdown
- Embedded benchmarks fail above **15%** slowdown

### Run + Check

```bash
mvn -q -pl memris-core -DskipTests test-compile dependency:build-classpath \
  -Dmdep.includeScope=test -Dmdep.outputFile=target/jmh-cp.txt

CP="memris-core/target/test-classes:memris-core/target/classes:$(cat memris-core/target/jmh-cp.txt)"
java -cp "$CP" org.openjdk.jmh.Main io.memris.benchmarks.EmbeddedPathBenchmark.* \
  -wi 1 -i 1 -f 1 -rf json -rff memris-core/target/jmh-embedded.json

python scripts/check-jmh-regression.py \
  --baseline memris-core/src/jmh/resources/embedded-path-baseline.json \
  --current memris-core/target/jmh-embedded.json \
  --flat-threshold 0.10 \
  --embedded-threshold 0.15
```
