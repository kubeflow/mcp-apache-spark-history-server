# Compare Applications

This example demonstrates Claude Code using the `shs` CLI to independently investigate performance differences between two Spark applications that ran the TPC-DS 3TB benchmark — one with the default Spark execution engine and one with [DataFusion Comet](https://github.com/apache/datafusion-comet).

Given the two application IDs and a structured prompt, Claude Code compared configurations, drilled into SQL executions, analyzed stage-level task quantiles, and produced a per-query root cause analysis across 10 queries — dispatching subagents in parallel for each query investigation.

| Label | App ID | Description |
|-------|--------|-------------|
| Default | `spark-1ee75653a3584297a2e9812bec54a7f7` | TPC-DS benchmark with default Spark execution engine |
| Comet | `spark-3ed897477b0f43cbb75c330d513c17d7` | TPC-DS benchmark with DataFusion Comet |

## Files

- [instructions.md](./instructions.md) — The prompt given to Claude Code, including the investigation workflow and subagent dispatch template.
- [report.md](./report.md) — The final summary report with environment context, per-query findings, and root cause categories.
- [findings/](./findings/) — Detailed per-query analysis produced by subagents (one file per investigated query).

The event data is too large to store in Git, so this example includes only the prompt and output.
