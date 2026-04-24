# TPC-DS Benchmark Analysis: Comet vs Native Spark

## Environment

### Versions
Spark: 3.5.8
Comet: 0.15.0
Java: 17.0.17 (Eclipse Adoptium)
Scala: 2.12.18
Source tag verification: Spark v3.5.8 matches. Comet has no exact 0.15.0 tag; used 0.15.0-rc1 (closest available).

### Configuration Differences
All 13 differing Spark properties between the two apps are infrastructure-only (pod names, app IDs, timestamps, submission IDs). No execution-affecting properties differ.

Comet-only properties (9):
| Property | Value |
|----------|-------|
| spark.comet.cast.allowIncompatible | true |
| spark.comet.dppFallback.enabled | true |
| spark.comet.exec.enabled | true |
| spark.comet.exec.shuffle.enabled | true |
| spark.comet.exec.shuffle.mode | auto |
| spark.comet.explainFallback.enabled | true |
| spark.plugins | org.apache.spark.CometPlugin |
| spark.shuffle.manager | org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager |
| spark.sql.extensions | org.apache.comet.CometSparkSessionExtensions |

### App-Level Summary
| Metric          | Default       | Comet         | Delta         |
|-----------------|---------------|---------------|---------------|
| Executors       | 23            | 23            | +0            |
| Total Cores     | 115           | 115           | +0            |
| Max Memory      | 996.5 GB      | 996.5 GB      | +0            |
| Jobs            | 6,136         | 6,175         | +39           |
| Stages          | 9,238         | 9,099         | -139          |
| Tasks           | 4,764,377     | 4,835,226     | +70,849       |
| Input           | 15.4 TB       | 14.5 TB       | -971.4 GB     |
| Shuffle Read    | 11.3 TB       | 6.3 TB        | -5.0 TB       |
| Shuffle Write   | 9.4 TB        | 7.9 TB        | -1.5 TB       |
| Spill (Disk)    | 0 B           | 0 B           | +0            |
| GC Time         | 2h47m33s      | 1h49m8s       | -58m25s       |
| Failed Tasks    | 0             | 2             | +2            |

---

## Query Summary

| Query | Change | Default | Comet | Shuffle Read D | GC Time D | Root Cause Category |
|-------|--------|---------|-------|----------------|-----------|---------------------|
| q5-v4.0 | +50.9% | 40.3s | 18.7s | -25.7 GB (-92%) | -6.4s | Native columnar execution + shuffle |
| q56-v4.0 | +49.0% | 5.3s | 2.8s | -6.6 MB (~0%) | -2.6s | AQE join strategy change |
| q41-v4.0 | +46.5% | 0.7s | 0.4s | +70 KB | +0s | Native columnar execution |
| q45-v4.0 | +42.6% | 11.9s | 6.7s | -2.9 GB (-78%) | -1.4s | AQE join strategy change + columnar shuffle |
| q9-v4.0 | +41.5% | 69.3s | 47.8s | +7.1 MB | -27.2s | Native columnar execution |
| q32-v4.0 | -90.1% | 2.9s | 5.6s | +1.9 KB | +31ms | Infrastructure straggler noise |
| q50-v4.0 | -34.8% | 81.7s | 108.8s | -33.9 GB (-22%) | -35.3s | CometSort performance gap |
| q39a-v4.0 | -23.1% | 5.0s | 6.2s | -2.4 MB | -323ms | Partition inflation + scan overhead |
| q68-v4.0 | -21.5% | 5.6s | 6.0s | -1.1 GB (-85%) | +1.1s | Partition inflation + scheduling gaps |
| q20-v4.0 | -13.7% | 5.0s | 5.6s | +14.0 MB (+3%) | -162ms | Arrow shuffle write amplification |

---

## Improved Queries

### q5-v4.0 (+50.9%) -> [detailed analysis](./findings/q5-v4.0.md)
- Shuffle read on the SortMergeJoin stage dropped 92% (27.9 GB to 2.2 GB) due to Comet's native columnar shuffle for web_sales (2.16B rows)
- GC time in the scan stage dropped from 7,236ms to 77ms; CometSort was 56% faster than Spark Sort (13.9 min vs 31.9 min total)
- **Root cause:** Comet's native scan-filter-exchange pipeline for web_sales produced compact Arrow columnar shuffle data, massively reducing downstream shuffle read and sort time in the join stage, with cascading contention reduction benefiting non-Comet stages

### q56-v4.0 (+49.0%) -> [detailed analysis](./findings/q56-v4.0.md)
- AQE converted all three SortMergeJoins to BroadcastHashJoins because CometExchange reported customer_address shuffle size as 2.7 MiB (vs 10.7 MiB in Default), falling below the 10 MB broadcast threshold
- Combined join stage time dropped from 8.6s to 1.4s; GC dropped from 2.8s to 206ms
- **Root cause:** Comet's Arrow columnar shuffle encoding produced a more compact representation that caused AQE to select broadcast joins, eliminating sort buffers, shuffle overhead, and GC pressure from sort buffer allocation

### q41-v4.0 (+46.5%) -> [detailed analysis](./findings/q41-v4.0.md)
- Max task in the dominant scan+aggregate stage dropped from 398ms to 174ms processing the same 1.3 MB item table partition
- Entire plan replaced with native Comet operators; all WholeStageCodegen nodes eliminated
- **Root cause:** On this small CPU-bound self-join query (360K rows), Comet's native columnar execution avoids ColumnarToRow conversion and JVM codegen overhead, providing lower per-task latency at all quantiles

### q45-v4.0 (+42.6%) -> [detailed analysis](./findings/q45-v4.0.md)
- One SortMergeJoin converted to BroadcastHashJoin, eliminating 1.5 GB intermediate shuffle write and 1.5 GB shuffle read; total shuffle read dropped 78% (3.7 GB to 822 MB)
- Task count dropped 78% (3,272 to 725); stages dropped from 29 to 10
- **Root cause:** Comet's native operators changed the cost model visible to AQE, enabling a broadcast join that removed the largest intermediate shuffle from the critical path, compounded by 27-30% more compact columnar shuffle encoding

### q9-v4.0 (+41.5%) -> [detailed analysis](./findings/q9-v4.0.md)
- Median task duration dropped 33-48% across all five parallel store_sales scan stages (p50: 420-539ms to 276-361ms)
- GC time dropped from 27.3s to 136ms (200x reduction) across 33,156 identical tasks
- **Root cause:** Comet's fused native scan-filter-project-aggregate pipeline processes 8.6B rows per scan in vectorized columnar batches without JVM heap allocation, eliminating both per-row overhead and GC pauses on this purely CPU-bound aggregation query

---

## Regressed Queries

### q32-v4.0 (-90.1%) -> [detailed analysis](./findings/q32-v4.0.md)
- The regressing stages run identical Spark WholeStageCodegen in both apps -- no Comet operators are involved in the slow stages
- Comet's best iteration (2,132ms) is faster than Default's best (2,338ms); the median regression is driven by single-task stragglers reaching 7.1s (10.5x p50) in Comet vs 1.8s max in Default
- **Root cause:** Measurement artifact from infrastructure-level straggler interference on a single task out of 118 in Comet's non-Comet scan stages, amplified by 6.6x wider iteration variance (range 8,856ms vs 1,351ms)

### q50-v4.0 (-34.8%) -> [detailed analysis](./findings/q50-v4.0.md)
- CometSort took 1.94 hours total (median 34.9s/task) vs Spark Sort at 27.5 minutes (median 8.4s/task) -- 4.2x slower on 7.96B store_sales rows
- Scan stage was 39% faster in Comet (21.4s vs 35.2s) with GC dropping from 39.5s to 455ms, but the join stage was 1.88x slower (87.0s vs 46.3s)
- **Root cause:** CometSort's native DataFusion sort is substantially slower than Spark's Tungsten-optimized pointer-based sort for this large-scale row sort, creating back-pressure that inflates shuffle fetch wait times (0ms to 7.2 min) and doubles downstream HashAggregate build time

### q39a-v4.0 (-23.1%) -> [detailed analysis](./findings/q39a-v4.0.md)
- Inventory scan stage median task time 35-77% higher in Comet (234ms vs 173ms) due to columnar shuffle serialization overhead
- CometColumnarExchange retained 200 partitions where Default used 7, inflating broadcast stage from 32ms to 358ms and producing 3.2x more shuffle data (23.3 MB vs 7.2 MB)
- **Root cause:** Two compounding factors: slower scan-stage serialization to Comet's columnar format (+330-538ms), and CometColumnarExchange overriding the natural partition count with 200 partitions for a broadcast exchange that only needed 7

### q68-v4.0 (-21.5%) -> [detailed analysis](./findings/q68-v4.0.md)
- Broadcast collection stage expanded from 9 tasks / 33ms (AQE-coalesced) to 200 tasks / 568ms (CometColumnarExchange); inter-job gap after broadcast was 667ms vs 76ms
- Store_sales scan stage GC was 1,553ms in Comet vs 388ms in Default despite identical 4.7 GB input
- **Root cause:** Critical-path scheduling regression from three compounding delays: scan-phase GC overhead (+327ms), broadcast partition inflation (+541ms), and inter-job scheduling gap (+591ms), partially offset by faster join/aggregate stages (-993ms)

### q20-v4.0 (-13.7%) -> [detailed analysis](./findings/q20-v4.0.md)
- CometColumnarExchange wrote 23.9 MiB vs Default's 10.2 MiB (2.3x) for 53,785 post-aggregation records across 200 partitions
- AQE created 23 coalesced partitions instead of 10; reshuffle stage took 462ms vs 94ms (+368ms)
- **Root cause:** Arrow IPC columnar encoding has disproportionate per-partition overhead for small record counts (~269 records/partition), causing shuffle write amplification that cascades through AQE partition coalescing and Arrow decode overhead

---

## Root Cause Categories

### Native Columnar Execution Speedup
Queries: q5-v4.0, q9-v4.0, q41-v4.0
Comet's fused native pipeline (CometNativeScan + CometFilter + CometProject + CometHashAggregate) processes columnar Arrow batches without JVM heap allocation or row-level materialization. This produces 33-50% per-task speedups and near-elimination of GC time (200x reduction in q9). The benefit scales with table size and CPU-boundedness of the scan-filter-aggregate pattern.

### AQE Join Strategy Change (SortMergeJoin to BroadcastHashJoin)
Queries: q56-v4.0, q45-v4.0
Comet's Arrow columnar shuffle encoding produces more compact shuffle output than Spark's UnsafeRow serialization. When the resulting shuffle size falls below AQE's 10 MB autoBroadcastJoinThreshold, AQE converts SortMergeJoins to BroadcastHashJoins. This eliminates sort operators, reduces task count, and removes GC pressure from sort buffer allocation. In q56, the threshold crossing was narrow (2.7 MiB vs 10.7 MiB for the same 703K-row table).

### CometSort Performance Gap on Large Sorts
Queries: q50-v4.0
CometSort (backed by DataFusion's native sort) is 4.2x slower than Spark's Tungsten sort when sorting billions of rows for a SortMergeJoin. Spark's sort operates on UnsafeRow pointers with an optimized in-place sort, while CometSort operates on Arrow columnar batches requiring different memory access patterns. The format transition cost (CometNativeColumnarToRow on 7.96B rows) adds further overhead. This is a targeted performance gap that affects queries with very large pre-join sorts.

### CometColumnarExchange Partition Inflation
Queries: q39a-v4.0, q68-v4.0
CometColumnarExchange retains the default 200 shuffle partitions where Spark's AQEShuffleRead would coalesce to far fewer (7-9). This creates disproportionate overhead for small-to-medium datasets: 22-29x more tasks in the broadcast collection stage, 3.2x more shuffle data from per-partition Arrow encoding overhead, and longer inter-job scheduling gaps as the driver materializes 200-partition broadcast results. The per-task work is comparable, but the task scheduling and materialization overhead dominates.

### Arrow IPC Shuffle Write Amplification on Small Payloads
Queries: q20-v4.0
When per-partition record counts are small (~269 records), Arrow IPC columnar encoding produces 2.3x more bytes than Spark's row-based serialization for the same logical data. This amplification cascades: AQE creates more coalesced partitions, each task reads more shuffle blocks, and Arrow decoding overhead becomes a measurable fraction of task time. The effect is specific to post-aggregation reshuffles where the record count is small but distributed across many partitions.

### Infrastructure Straggler Noise (Not a Comet Regression)
Queries: q32-v4.0
The reported -90.1% regression is a measurement artifact. The slow stages execute identical Spark WholeStageCodegen in both apps. Comet's best run (2,132ms) is actually faster than Default's best (2,338ms). The median-based comparison is skewed by single-task outliers reaching 7-15x the p50 in 3 of 5 Comet iterations, consistent with infrastructure-level interference rather than any Comet code path.

---

## Cross-Query Patterns

1. **GC elimination is Comet's most consistent benefit.** Every query where Comet replaces scan operators shows dramatic GC reduction (39.5s to 455ms in q50, 27.3s to 136ms in q9, 7.2s to 77ms in q5). This is because Comet processes data off-heap in native memory, avoiding JVM object allocation for intermediate rows. At the app level, total GC dropped by 58 minutes (-35%).

2. **Compact Arrow shuffle enables indirect AQE improvements.** In q56 and q45, the performance gain comes not from faster execution of the same plan, but from Comet's shuffle producing smaller statistics that cause AQE to select a fundamentally different (and better) join strategy. This is an emergent interaction between Comet's columnar encoding and Spark's adaptive optimizer.

3. **The same Arrow encoding that helps large tables hurts small payloads.** Arrow IPC columnar format is more compact than UnsafeRow for large record sets (25% savings in q50, 20% in q5) but has higher per-partition fixed overhead that causes 2.3-3.2x amplification when records-per-partition is small (q20, q39a). The crossover point appears to be in the low hundreds of records per partition.

4. **CometColumnarExchange does not participate in AQE partition coalescing.** In q39a, q68, and q20, CometColumnarExchange retains 200 partitions where AQEShuffleRead would coalesce to 7-23. This is the most common regression pattern, appearing in 3 of the 4 genuine regressions. The partition count itself is not the issue -- per-task work is comparable -- but the scheduling overhead and Arrow encoding overhead per partition accumulate.

5. **CometSort has a targeted performance gap vs Tungsten Sort.** The q50 regression (4.2x slower sort on 7.96B rows) is the only query where CometSort is on the critical path with billions of rows. In q5, CometSort was 56% faster than Spark Sort on 2.16B rows in the same pre-join position, suggesting the performance crossover depends on data volume, key distribution, or memory pressure characteristics that differ between these queries.
