"""Curated, LLM-friendly output models for MCP tools.

These intentionally expose a small, summarized subset of the raw Spark History
Server payloads.
"""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class SqlExecutionSummary(BaseModel):
    """Header-level summary of a single SQL execution."""

    id: Optional[int] = None
    status: Optional[str] = None
    description: Optional[str] = None
    submission_time: Optional[str] = Field(None, alias="submissionTime")
    duration: Optional[int] = None  # milliseconds, as reported by SHS
    success_job_ids: List[int] = Field(default_factory=list, alias="successJobIds")
    failed_job_ids: List[int] = Field(default_factory=list, alias="failedJobIds")
    running_job_ids: List[int] = Field(default_factory=list, alias="runningJobIds")

    model_config = ConfigDict(populate_by_name=True)


class SqlNodeMetrics(BaseModel):
    """Per-node plan metrics, keyed name -> value."""

    node_id: Optional[int] = Field(None, alias="nodeId")
    node_name: Optional[str] = Field(None, alias="nodeName")
    metrics: dict[str, str] = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True)


class SqlJobSummary(BaseModel):
    """Curated job row associated with a SQL execution."""

    job_id: Optional[int] = Field(None, alias="jobId")
    status: Optional[str] = None
    description: Optional[str] = None
    duration: Optional[int] = None  # milliseconds
    num_tasks: Optional[int] = Field(None, alias="numTasks")
    num_failed_tasks: Optional[int] = Field(None, alias="numFailedTasks")
    stage_ids: List[int] = Field(default_factory=list, alias="stageIds")

    model_config = ConfigDict(populate_by_name=True)


class SqlStageSummary(BaseModel):
    """Curated stage row associated with a SQL execution."""

    stage_id: Optional[int] = Field(None, alias="stageId")
    attempt_id: Optional[int] = Field(None, alias="attemptId")
    status: Optional[str] = None
    description: Optional[str] = None
    num_tasks: Optional[int] = Field(None, alias="numTasks")
    num_failed_tasks: Optional[int] = Field(None, alias="numFailedTasks")
    duration: Optional[int] = None  # milliseconds
    input_bytes: Optional[int] = Field(None, alias="inputBytes")
    shuffle_read_bytes: Optional[int] = Field(None, alias="shuffleReadBytes")
    shuffle_write_bytes: Optional[int] = Field(None, alias="shuffleWriteBytes")

    model_config = ConfigDict(populate_by_name=True)


class StageMetricsAggregation(BaseModel):
    """Aggregated stage metrics scoped to a SQL execution."""

    stage_count: int = Field(0, alias="stageCount")
    tasks: int = 0
    duration: int = 0  # milliseconds (sum of per-stage wall durations)
    input_bytes: int = Field(0, alias="inputBytes")
    shuffle_read_bytes: int = Field(0, alias="shuffleReadBytes")
    shuffle_write_bytes: int = Field(0, alias="shuffleWriteBytes")
    disk_bytes_spilled: int = Field(0, alias="diskBytesSpilled")
    jvm_gc_time: int = Field(0, alias="jvmGcTime")

    model_config = ConfigDict(populate_by_name=True)


class SqlExecutionDetail(BaseModel):
    """Detailed view of a SQL execution.

    By default, only ``execution`` (the header) is populated. The optional
    sections are filled in when the corresponding ``include_*`` flag is set on
    ``get_sql_execution``.
    """

    execution: SqlExecutionSummary
    plan_description: Optional[str] = Field(None, alias="planDescription")
    node_metrics: Optional[List[SqlNodeMetrics]] = Field(None, alias="nodeMetrics")
    jobs: Optional[List[SqlJobSummary]] = None
    stage_metrics: Optional[StageMetricsAggregation] = Field(None, alias="stageMetrics")
    stages: Optional[List[SqlStageSummary]] = None

    model_config = ConfigDict(populate_by_name=True)


class SqlCompareSide(BaseModel):
    """One side of a SQL execution metrics comparison."""

    app: str
    sql_id: Optional[int] = Field(None, alias="sqlId")
    description: Optional[str] = None
    status: Optional[str] = None
    duration: Optional[int] = None  # milliseconds
    jobs: int = 0
    stages: int = 0
    tasks: int = 0
    stage_time: int = Field(0, alias="stageTime")  # milliseconds
    input_bytes: int = Field(0, alias="inputBytes")
    shuffle_read_bytes: int = Field(0, alias="shuffleReadBytes")
    shuffle_write_bytes: int = Field(0, alias="shuffleWriteBytes")
    disk_bytes_spilled: int = Field(0, alias="diskBytesSpilled")
    jvm_gc_time: int = Field(0, alias="jvmGcTime")

    model_config = ConfigDict(populate_by_name=True)


class SqlNodeTypeDiff(BaseModel):
    """Count of a single plan node type on each side of a plan comparison."""

    node_type: str = Field(..., alias="nodeType")
    a: int = 0
    b: int = 0

    model_config = ConfigDict(populate_by_name=True)


class SqlPlanComparison(BaseModel):
    """Plan-structure diff between two SQL executions."""

    app_a: str = Field(..., alias="appA")
    app_b: str = Field(..., alias="appB")
    exec_id_a: Optional[int] = Field(None, alias="execIdA")
    exec_id_b: Optional[int] = Field(None, alias="execIdB")
    node_count_a: int = Field(0, alias="nodeCountA")
    node_count_b: int = Field(0, alias="nodeCountB")
    edge_count_a: int = Field(0, alias="edgeCountA")
    edge_count_b: int = Field(0, alias="edgeCountB")
    node_type_diffs: List[SqlNodeTypeDiff] = Field(
        default_factory=list, alias="nodeTypeDiffs"
    )

    model_config = ConfigDict(populate_by_name=True)


class SqlExecutionComparison(BaseModel):
    """Metrics diff between two SQL executions, with an optional plan-structure diff."""

    a: SqlCompareSide
    b: SqlCompareSide
    plan_comparison: Optional[SqlPlanComparison] = Field(None, alias="planComparison")

    model_config = ConfigDict(populate_by_name=True)
