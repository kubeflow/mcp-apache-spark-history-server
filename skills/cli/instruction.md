# Comet vs Native Spark — TPC-DS Benchmark Analysis

## Applications

| Label | App ID | Description |
|-------|--------|-------------|
| Default | `spark-1ee75653a3584297a2e9812bec54a7f7` | TPC-DS benchmark with default Spark execution engine |
| Comet | `spark-3ed897477b0f43cbb75c330d513c17d7` | TPC-DS benchmark with DataFusion Comet |

## Benchmark Summary

```
============================================================
TPC-DS Benchmark: Comet vs Native Spark
============================================================
Queries compared:      103
Total Default (s):     3650.56
Total Comet (s):           3246.87
Overall speedup:       11.06%
Geometric mean ratio:  0.8775x
Median query speedup:  9.14%
Comet wins/losses:   66 / 37

Distribution:
  20%+ improvement          32
  10-20% improvement        19
  ±10%                      44
  10-20% degradation        4
  20%+ degradation          4

Top 10 Improvements:
  q5-v4.0              +50.9%  (40.8s → 20.0s)
  q56-v4.0             +49.0%  (5.8s → 3.0s)
  q41-v4.0             +46.5%  (0.8s → 0.4s)
  q45-v4.0             +42.6%  (12.3s → 7.0s)
  q9-v4.0              +41.5%  (83.8s → 49.0s)
  q80-v4.0             +38.4%  (32.6s → 20.1s)
  q58-v4.0             +38.3%  (5.4s → 3.3s)
  q86-v4.0             +36.5%  (12.8s → 8.1s)
  q83-v4.0             +36.3%  (2.8s → 1.8s)
  q73-v4.0             +35.6%  (3.4s → 2.2s)

Top 10 Regressions:
  q32-v4.0             -90.1%  (2.9s → 5.6s)
  q50-v4.0             -34.8%  (82.7s → 111.5s)
  q39a-v4.0            -23.1%  (5.1s → 6.3s)
  q68-v4.0             -21.5%  (5.9s → 7.1s)
  q20-v4.0             -13.7%  (5.1s → 5.8s)
  q30-v4.0             -11.3%  (15.9s → 17.7s)
  q67-v4.0             -11.0%  (141.2s → 156.8s)
  q25-v4.0             -10.3%  (8.9s → 9.9s)
  q29-v4.0             -9.9%  (34.4s → 37.8s)
  q91-v4.0             -9.9%  (2.1s → 2.3s)
```

## Your Task

Investigate the top 5 improved queries and top 5 regressed queries to determine why they improved or regressed. Produce a structured report with your findings.

## Tools

### Spark History Server CLI

You have a CLI tool called `shs`. Run `./shs prime` first to learn the available commands and usage.

### Source Code

Source code is available for deeper investigation:
- `./sources/spark/` — Apache Spark repository. Execution engine code is primarily in `sql/core/` and `sql/catalyst/`.
- `./sources/comet/` — DataFusion Comet repository. Operator implementations are in `native/` (Rust) and `spark/` (Scala).

The repos are already cloned. You will need to check out the correct version tags (see workflow step 1).

## Investigation Workflow

### Step 1: Version Discovery (Main Agent)

**Important:** `./shs version` returns the Spark History Server version, not the Spark version used by the jobs. The SHS supports backward compatibility, so jobs may have run on a different Spark version than the server itself.

1. Run `./shs env -a APP_ID --section runtime` for both apps to get Java and Scala versions.
2. Run `./shs env -a APP_ID --section classpath` for both apps and search for `spark-core` to find the actual Spark version from the JAR filename (e.g., `spark-core_2.12-3.5.8.jar` → Spark `3.5.8`).
3. In the same classpath output for the Comet app, search for `comet` to find the Comet JAR version (e.g., `comet-spark-spark3.5_2.12-0.15.0.jar` → Comet `0.15.0`).
4. Check out the matching version tags in the source repos:
    - `cd ./sources/spark && git checkout <spark-version-tag>`
    - `cd ./sources/comet && git checkout <comet-version-tag>`
5. Verify the checked-out tags match the versions detected above. If they do not match, flag this prominently in the report and proceed with caution.

### Step 2: Configuration Diff (Main Agent)

Run `./shs compare sql --app-a <DEFAULT_APP> --app-b <COMET_APP> --env` to identify all configuration differences between the two runs.

The output has three sections:
- **Runtime** — Java/Scala versions (should match).
- **Different Spark Properties** — Properties present in both apps with different values. Many will be infrastructure differences (pod names, app IDs, timestamps). Focus on properties that affect execution behavior.
- **Only in A / Only in B** — Properties unique to one app. The Comet-specific properties (`spark.comet.*`, `spark.plugins`, `spark.shuffle.manager`, `spark.sql.extensions`) will appear here. Note any non-Comet differences that could affect performance.

### Step 3: App-Level Comparison (Main Agent)

Run `./shs compare apps --app-a <DEFAULT_APP> --app-b <COMET_APP>` to get aggregate metrics: executors, cores, jobs, stages, tasks, input, shuffle read/write, spill, and GC time across both runs. This is your baseline context.

### Step 4: Per-Query Investigation (Subagents)

After completing steps 1-3, delegate per-query investigation to subagents for parallel execution. Each subagent investigates a single query in isolation to prevent cross-contamination of metrics between queries.

#### Subagent Dispatch

Spawn subagents in batches of up to 4 in parallel. Each subagent receives:
- The environment context collected from steps 1-3 (versions, config diff summary, app-level metrics)
- The app IDs for both applications
- The query name and expected improvement/regression percentage
- The per-query investigation instructions (steps 4a-4e below)
- The rules (evidence only, version accuracy, no suggestions, source code discipline)

Each subagent writes its findings to `./findings/<query-name>.md` (e.g., `./findings/q5-v4.0.md`).

With 10 queries total, dispatch in 3 batches: 4 + 4 + 2.

#### 4a. Identify the SQL Executions

Run `./shs sql -a APP_ID --limit 0 -o json` for both apps. Filter executions whose description starts with `benchmark ` and matches the query name (e.g., `benchmark q5-v4.0`). Each query has multiple iterations (typically 5). Select the lowest-duration execution for each app. Record all iteration durations for the report.

#### 4b. Metrics Comparison

Run `./shs compare sql --app-a <DEFAULT_APP> --app-b <COMET_APP> EXEC_A EXEC_B` using the lowest-duration execution IDs. This gives duration, stage count, task count, input, shuffle read/write, spill, and GC time deltas.

#### 4c. Plan Diff

Run `./shs compare sql --app-a <DEFAULT_APP> --app-b <COMET_APP> --plans EXEC_A EXEC_B` to see node type differences — which Spark operators Comet replaced with its own (e.g., `Scan parquet` → `CometNativeScan`, `Filter` → `CometFilter`, `Exchange` → `CometExchange`), and any structural changes (node/edge count differences).

#### 4d. Stage Drill-Down

To find stages for a SQL execution:
1. Run `./shs sql -a APP_ID EXEC_ID --summary -o json` to get the job list with stage IDs.
2. Extract `stageIds` from each job object.
3. **Important:** Jobs may include skipped stages. Skipped stages return a 404 error from the API. Check `numSkippedStages` on the job — if stages are skipped, not all listed stage IDs will be accessible. Handle 404 errors gracefully and move on.
4. For the stages with the largest performance delta between the two apps, run `./shs stages -a APP_ID STAGE_ID` to get task counts, input/output, shuffle, spill, GC time, and task quantile distributions (p25/p50/p75/max).
5. Even if the root cause looks obvious from the plan/metrics, drill into at least one stage per query as a sanity check. Look at task quantiles for data skew, GC pressure, shuffle sizes, and spill.

#### 4e. Source Code Lookup (If Needed)

If the plan diff shows Comet-specific operators and the metrics suggest a non-obvious behavior, look at the operator implementation in the checked-out source code to understand what it does differently.

**Constraint:** Do not speculate about performance characteristics that are not observable in the metrics. Only use source code to explain behavior that is already evident in the numbers (e.g., if shuffle bytes dropped significantly after Comet replaced an Exchange operator, you may look at the Comet shuffle implementation to explain how it achieves the reduction).

### Step 5: Assemble Summary Report (Main Agent)

After all subagents complete:
1. Read all files from `./findings/`.
2. Assemble a summary report (not a full copy of each query's findings). The summary should be concise — detailed per-query analysis lives in the individual `./findings/<query-name>.md` files.
3. The summary report includes:
    - Environment section (versions, config diff, app-level metrics) from steps 1-3.
    - A summary table showing all 10 queries with their key metric deltas at a glance.
    - A per-query entry with only the 2-3 most impactful facts (e.g., "shuffle read dropped 92%", "GC time cut by 6.4s") and a one-sentence root cause, linking to the detailed file.
    - A root cause categorization grouping queries by why they improved or regressed (e.g., "3 regressions due to shuffle overhead", "4 improvements due to columnar scan pushdown").
    - A cross-query patterns section if common themes emerge.

## Rules

- **Evidence only.** Every claim must be backed by specific numbers from the CLI output (GC time, shuffle bytes, spill, CPU time, duration, task counts, etc.). Do not limit yourself to these — use any metric available that supports your analysis.
- **Version accuracy.** Do not make claims about Comet or Spark behavior based on your training data. Use only what is observable in the metrics, plans, and source code at the checked-out versions. If you are unsure whether a behavior applies to the detected version, say so.
- **No suggestions.** Provide root-cause analysis only. Do not suggest fixes, config tuning, or optimizations. Those will be addressed in follow-up questions if needed.
- **Source code discipline.** You may reference source code to explain operator behavior. Do not speculate about performance implications (e.g., "this allocates more objects") unless the metrics show a corresponding effect (e.g., elevated GC time).

## Report Format

The final report is a summary. Detailed per-query analysis lives in `./findings/<query-name>.md`.

```
# TPC-DS Benchmark Analysis: Comet vs Native Spark

## Environment

### Versions
Spark: <version>
Comet: <version>
Java: <version>
Scala: <version>
Source tag verification: <match/mismatch + details>

### Configuration Differences
<table or list of meaningful config diffs, excluding infrastructure differences like pod names and app IDs>

### App-Level Summary
| Metric          | Default       | Comet         | Delta         |
|-----------------|---------------|---------------|---------------|
| Executors       | ...           | ...           | ...           |
| Jobs            | ...           | ...           | ...           |
| Stages          | ...           | ...           | ...           |
| Tasks           | ...           | ...           | ...           |
| Input           | ...           | ...           | ...           |
| Shuffle Read    | ...           | ...           | ...           |
| Shuffle Write   | ...           | ...           | ...           |
| Spill (Disk)    | ...           | ...           | ...           |
| GC Time         | ...           | ...           | ...           |

---

## Query Summary

| Query | Change | Default | Comet | Shuffle Read Δ | GC Time Δ | Root Cause Category |
|-------|--------|---------|-------|----------------|-----------|---------------------|
<one row per investigated query, sorted by improvement descending>

---

## Improved Queries

### <query-name> (<change>%)  →  [detailed analysis](./findings/<query-name>.md)
- <key fact 1 with number>
- <key fact 2 with number>
- **Root cause:** <one-sentence summary>

<repeat for each improved query>

---

## Regressed Queries

### <query-name> (<change>%)  →  [detailed analysis](./findings/<query-name>.md)
- <key fact 1 with number>
- <key fact 2 with number>
- **Root cause:** <one-sentence summary>

<repeat for each regressed query>

---

## Root Cause Categories

### <category name>
Queries: <list of queries in this category>
<brief explanation of the shared pattern>

<repeat for each category>

---

## Cross-Query Patterns
<any additional observations spanning multiple queries>
```

## Subagent Query Template

When dispatching a subagent for a query investigation, use the following template for the subagent query:

```
Investigate TPC-DS query <QUERY_NAME> (<IMPROVEMENT/REGRESSION>%) for the Comet vs Native Spark benchmark.

## Applications
- Default: <DEFAULT_APP_ID>
- Comet: <COMET_APP_ID>

## Environment Context
<paste version info, config diff summary, and app-level metrics from steps 1-3>

## Instructions
Follow steps 4a through 4e from the investigation workflow:
1. Find all SQL executions matching "benchmark <QUERY_NAME>" in both apps using `./shs sql -a APP_ID --limit 0 -o json`. Pick the lowest-duration run for each app. Record all iteration durations.
2. Run `./shs compare sql --app-a <DEFAULT_APP> --app-b <COMET_APP> EXEC_A EXEC_B` for metrics.
3. Run `./shs compare sql --app-a <DEFAULT_APP> --app-b <COMET_APP> --plans EXEC_A EXEC_B` for plan diff.
4. Find stages via `./shs sql -a APP_ID EXEC_ID --summary -o json`, extract stageIds from jobs. Skipped stages return 404 — check numSkippedStages and handle gracefully. Drill into the highest-impact stages in both apps using `./shs stages -a APP_ID STAGE_ID`. Always drill into at least one stage as a sanity check.
5. If plan diff shows Comet operators and metrics suggest non-obvious behavior, look at source code in `./sources/comet/` or `./sources/spark/` (already checked out to correct versions). Only use source to explain observed metrics — do not speculate.

## Rules
- Every claim must be backed by specific numbers from CLI output.
- Do not make claims about Comet or Spark behavior based on training data. Use only metrics, plans, and source code at the checked-out versions.
- Provide root-cause analysis only. No suggestions or fix recommendations.
- Do not speculate about performance implications from source code unless metrics show a corresponding effect.

## Output
Write your findings to `./findings/<QUERY_NAME>.md` using this structure:

### <QUERY_NAME> (<+/->X.X%)

#### Metrics (Default vs Comet)
| Metric          | Default | Comet  | Delta   |
|-----------------|---------|--------|---------|
| Duration        | ...     | ...    | ...     |
| Stages          | ...     | ...    | ...     |
| Tasks           | ...     | ...    | ...     |
| Input           | ...     | ...    | ...     |
| Shuffle Read    | ...     | ...    | ...     |
| Shuffle Write   | ...     | ...    | ...     |
| Spill (Disk)    | ...     | ...    | ...     |
| GC Time         | ...     | ...    | ...     |

#### Iterations
| Run | Default | Comet  |
|-----|---------|--------|
| 1   | ...     | ...    |
| 2   | ...     | ...    |
| 3   | ...     | ...    |
| 4   | ...     | ...    |
| 5   | ...     | ...    |

#### Plan Differences
<high-level summary of operator replacements and structural changes>

#### Stage Analysis
<findings from the highest-impact stages, with specific numbers from task quantiles>

#### Root Cause
<narrative explanation grounded in the numbers and plan differences above>
```
