# PR #24 Before/After Benchmark Plan

## Goal
Run stable before/after benchmarks comparing `main` vs PR #24 for `write8_read8`, then present a detailed report with throughput, latency, GC metrics, and estimated CPU cycles saved.

## Execution Steps

### 1) Environment Setup
- Ensure clean working tree (stash local changes if needed)
- Create results directory: `memris-core/target/benchmarks/comparison/`
- Note CPU frequency for cycle estimation (ask user or use 3.5 GHz default)

### 2) Before Run (main branch)
```bash
git checkout main
mvn -pl memris-core exec:java ^
  -Dexec.mainClass="io.memris.benchmarks.MemrisBenchmarks" ^
  -Dexec.classpathScope=test ^
  -Dexec.args="-wi 1 -i 3 -f 1 -bm avgt,thrpt -tu ms -rf json -rff target/benchmarks/comparison/jmh-before.json" ^
  -Dexec.jvmArgs="--enable-native-access=ALL-UNNAMED -Xlog:gc*:file=target/benchmarks/comparison/gc-before.log:time,level,tags -Xms512m -Xmx512m -XX:+UseG1GC"
```
Capture: `jmh-before.json`, `gc-before.log`

### 3) After Run (PR #24 branch)
```bash
git checkout perf/materializer-table-direct
mvn -pl memris-core exec:java ^
  -Dexec.mainClass="io.memris.benchmarks.MemrisBenchmarks" ^
  -Dexec.classpathScope=test ^
  -Dexec.args="-wi 1 -i 3 -f 1 -bm avgt,thrpt -tu ms -rf json -rff target/benchmarks/comparison/jmh-after.json" ^
  -Dexec.jvmArgs="--enable-native-access=ALL-UNNAMED -Xlog:gc*:file=target/benchmarks/comparison/gc-after.log:time,level,tags -Xms512m -Xmx512m -XX:+UseG1GC"
```
Capture: `jmh-after.json`, `gc-after.log`

### 4) Data Extraction & Report Generation
Parse both JSON outputs and GC logs, compute:
- Throughput (ops/s) before vs after
- Average operation time (ns/op)
- GC count and total pause time
- Allocation rate (if available)
- Estimated CPU cycles per operation (using CPU frequency)
- Cycles saved per operation and per second

Generate Markdown table report with all metrics and % changes.

## Success Criteria
- Both runs complete without errors
- JSON files created and parseable
- Report shows clear before/after comparison with quantified improvements

## Notes
- Use short runs (wi=1, i=3) to keep total time ~6 minutes
- Use 512m heap to avoid paging file issues seen earlier
- CPU frequency needed for cycle estimation (will ask user)