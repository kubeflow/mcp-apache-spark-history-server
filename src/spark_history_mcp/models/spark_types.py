from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Sequence, Set

from pydantic import BaseModel, ConfigDict, Field, field_validator


class JobExecutionStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_string(cls, s: str) -> "JobExecutionStatus":
        try:
            return cls(s.upper())
        except ValueError:
            return cls[s.upper()]


class StageStatus(str, Enum):
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    COMPLETE = "COMPLETE"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"

    @classmethod
    def from_string(cls, s: str) -> "StageStatus":
        try:
            return cls(s.upper())
        except ValueError:
            return cls[s.upper()]


class TaskStatus(str, Enum):
    RUNNING = "RUNNING"
    KILLED = "KILLED"
    FAILED = "FAILED"
    SUCCESS = "SUCCESS"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_string(cls, s: str) -> "TaskStatus":
        try:
            return cls(s.upper())
        except ValueError:
            return cls[s.upper()]


class TaskSorting(str, Enum):
    ID = "ID"
    INCREASING_RUNTIME = "INCREASING_RUNTIME"
    DECREASING_RUNTIME = "DECREASING_RUNTIME"

    _alternate_names = {"runtime": INCREASING_RUNTIME, "-runtime": DECREASING_RUNTIME}

    @classmethod
    def from_string(cls, s: str) -> "TaskSorting":
        lower = s.lower()
        if lower in cls._alternate_names:
            return cls._alternate_names[lower]
        try:
            return cls(s.upper())
        except ValueError:
            return cls[s.upper()]


class ApplicationStatus(str, Enum):
    COMPLETED = "COMPLETED"
    RUNNING = "RUNNING"

    @classmethod
    def from_string(cls, s: str) -> "ApplicationStatus":
        try:
            return cls(s.upper())
        except ValueError:
            return cls[s.upper()]


class ThreadState(str, Enum):
    NEW = "NEW"
    RUNNABLE = "RUNNABLE"
    BLOCKED = "BLOCKED"
    WAITING = "WAITING"
    TIMED_WAITING = "TIMED_WAITING"
    TERMINATED = "TERMINATED"


class ExecutorMetrics(BaseModel):
    metrics: Optional[Dict[str, int]] = Field(None, alias="metrics")

    model_config = ConfigDict(populate_by_name=True)


class ApplicationInfo(BaseModel):
    id: str
    name: str
    cores_granted: Optional[int] = Field(None, alias="coresGranted")
    max_cores: Optional[int] = Field(None, alias="maxCores")
    cores_per_executor: Optional[int] = Field(None, alias="coresPerExecutor")
    memory_per_executor_mb: Optional[int] = Field(None, alias="memoryPerExecutorMB")
    attempts: Sequence["ApplicationAttemptInfo"]

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)


class ApplicationAttemptInfo(BaseModel):
    attempt_id: Optional[str] = Field(None, alias="attemptId")
    start_time: Optional[datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime] = Field(None, alias="endTime")
    last_updated: Optional[datetime] = Field(None, alias="lastUpdated")
    duration: int
    spark_user: Optional[str] = Field(None, alias="sparkUser")
    app_spark_version: Optional[str] = Field(None, alias="appSparkVersion")
    completed: bool = False

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    @field_validator("start_time", "end_time", "last_updated", mode="before")
    @classmethod
    def parse_datetime(cls, value):
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000)
        if isinstance(value, str) and value.endswith("GMT"):
            # Handle Spark's ISO date format that ends with GMT
            try:
                # Remove GMT and parse
                dt_str = value.replace("GMT", "+0000")
                return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f%z")
            except ValueError:
                pass
        return value


class ResourceProfileInfo(BaseModel):
    id: int
    executor_resources: Optional[Dict[str, Any]] = Field(
        None, alias="executorResources"
    )  # Will be typed properly once those classes are defined
    task_resources: Optional[Dict[str, Any]] = Field(None, alias="taskResources")

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)


class ExecutorStageSummary(BaseModel):
    task_time: Optional[int] = Field(None, alias="taskTime")
    failed_tasks: Optional[int] = Field(None, alias="failedTasks")
    succeeded_tasks: Optional[int] = Field(None, alias="succeededTasks")
    killed_tasks: Optional[int] = Field(None, alias="killedTasks")
    input_bytes: Optional[int] = Field(None, alias="inputBytes")
    input_records: Optional[int] = Field(None, alias="inputRecords")
    output_bytes: Optional[int] = Field(None, alias="outputBytes")
    output_records: Optional[int] = Field(None, alias="outputRecords")
    shuffle_read: Optional[int] = Field(None, alias="shuffleRead")
    shuffle_read_records: Optional[int] = Field(None, alias="shuffleReadRecords")
    shuffle_write: Optional[int] = Field(None, alias="shuffleWrite")
    shuffle_write_records: Optional[int] = Field(None, alias="shuffleWriteRecords")
    memory_bytes_spilled: Optional[int] = Field(None, alias="memoryBytesSpilled")
    disk_bytes_spilled: Optional[int] = Field(None, alias="diskBytesSpilled")
    is_blacklisted_for_stage: Optional[bool] = Field(
        None, alias="isBlacklistedForStage"
    )  # deprecated
    peak_memory_metrics: Optional[ExecutorMetrics] = Field(
        None, alias="peakMemoryMetrics"
    )
    is_excluded_for_stage: Optional[bool] = Field(None, alias="isExcludedForStage")

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)


class SpeculationStageSummary(BaseModel):
    num_tasks: Optional[int] = Field(None, alias="numTasks")
    num_active_tasks: Optional[int] = Field(None, alias="numActiveTasks")
    num_completed_tasks: Optional[int] = Field(None, alias="numCompletedTasks")
    num_failed_tasks: Optional[int] = Field(None, alias="numFailedTasks")
    num_killed_tasks: Optional[int] = Field(None, alias="numKilledTasks")

    model_config = ConfigDict(populate_by_name=True)


class ExecutorSummary(BaseModel):
    id: str
    host_port: Optional[str] = Field(None, alias="hostPort")
    is_active: Optional[bool] = Field(None, alias="isActive")
    rdd_blocks: Optional[int] = Field(None, alias="rddBlocks")
    memory_used: Optional[int] = Field(None, alias="memoryUsed")
    disk_used: Optional[int] = Field(None, alias="diskUsed")
    total_cores: Optional[int] = Field(None, alias="totalCores")
    max_tasks: Optional[int] = Field(None, alias="maxTasks")
    active_tasks: Optional[int] = Field(None, alias="activeTasks")
    failed_tasks: Optional[int] = Field(None, alias="failedTasks")
    completed_tasks: Optional[int] = Field(None, alias="completedTasks")
    total_tasks: Optional[int] = Field(None, alias="totalTasks")
    total_duration: Optional[int] = Field(None, alias="totalDuration")
    total_gc_time: Optional[int] = Field(None, alias="totalGCTime")
    total_input_bytes: Optional[int] = Field(None, alias="totalInputBytes")
    total_shuffle_read: Optional[int] = Field(None, alias="totalShuffleRead")
    total_shuffle_write: Optional[int] = Field(None, alias="totalShuffleWrite")
    is_blacklisted: Optional[bool] = Field(None, alias="isBlacklisted")  # deprecated
    max_memory: Optional[int] = Field(None, alias="maxMemory")
    add_time: Optional[datetime] = Field(None, alias="addTime")
    remove_time: Optional[datetime] = Field(None, alias="removeTime")
    remove_reason: Optional[str] = Field(None, alias="removeReason")
    executor_logs: Optional[Dict[str, str]] = Field(None, alias="executorLogs")
    memory_metrics: Optional["MemoryMetrics"] = Field(None, alias="memoryMetrics")
    blacklisted_in_stages: Set[int] = Field(
        set(), alias="blacklistedInStages"
    )  # deprecated
    peak_memory_metrics: Optional[ExecutorMetrics] = Field(
        None, alias="peakMemoryMetrics"
    )
    attributes: Dict[str, str]
    resources: Dict[
        str, Any
    ]  # Will be typed properly once ResourceInformation is defined
    resource_profile_id: Optional[int] = Field(None, alias="resourceProfileId")
    is_excluded: Optional[bool] = Field(None, alias="isExcluded")
    excluded_in_stages: Set[int] = Field(set(), alias="excludedInStages")

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    @field_validator("add_time", "remove_time", mode="before")
    @classmethod
    def parse_datetime(cls, value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000)
        if isinstance(value, str) and value.endswith("GMT"):
            # Handle Spark's ISO date format that ends with GMT
            try:
                # Remove GMT and parse
                dt_str = value.replace("GMT", "+0000")
                return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f%z")
            except ValueError:
                pass
        return value


class MemoryMetrics(BaseModel):
    used_on_heap_storage_memory: Optional[int] = Field(
        None, alias="usedOnHeapStorageMemory"
    )
    used_off_heap_storage_memory: Optional[int] = Field(
        None, alias="usedOffHeapStorageMemory"
    )
    total_on_heap_storage_memory: Optional[int] = Field(
        None, alias="totalOnHeapStorageMemory"
    )
    total_off_heap_storage_memory: Optional[int] = Field(
        None, alias="totalOffHeapStorageMemory"
    )

    model_config = ConfigDict(populate_by_name=True)


class JobData(BaseModel):
    job_id: Optional[int] = Field(None, alias="jobId")
    name: str
    description: Optional[str] = None
    submission_time: Optional[datetime] = Field(None, alias="submissionTime")
    completion_time: Optional[datetime] = Field(None, alias="completionTime")
    stage_ids: Optional[Sequence[int]] = Field(None, alias="stageIds")
    job_group: Optional[str] = Field(None, alias="jobGroup")
    job_tags: Sequence[str] = Field([], alias="jobTags")
    status: str  # JobExecutionStatus as string
    num_tasks: Optional[int] = Field(None, alias="numTasks")
    num_active_tasks: Optional[int] = Field(None, alias="numActiveTasks")
    num_completed_tasks: Optional[int] = Field(None, alias="numCompletedTasks")
    num_skipped_tasks: Optional[int] = Field(None, alias="numSkippedTasks")
    num_failed_tasks: Optional[int] = Field(None, alias="numFailedTasks")
    num_killed_tasks: Optional[int] = Field(None, alias="numKilledTasks")
    num_completed_indices: Optional[int] = Field(None, alias="numCompletedIndices")
    num_active_stages: Optional[int] = Field(None, alias="numActiveStages")
    num_completed_stages: Optional[int] = Field(None, alias="numCompletedStages")
    num_skipped_stages: Optional[int] = Field(None, alias="numSkippedStages")
    num_failed_stages: Optional[int] = Field(None, alias="numFailedStages")
    killed_tasks_summary: Dict[str, int] = Field({}, alias="killedTasksSummary")

    model_config = ConfigDict(
        populate_by_name=True, arbitrary_types_allowed=True, extra="ignore"
    )

    @field_validator("submission_time", "completion_time", mode="before")
    @classmethod
    def parse_datetime(cls, value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000)
        if isinstance(value, str) and value.endswith("GMT"):
            # Handle Spark's ISO date format that ends with GMT
            try:
                # Remove GMT and parse
                dt_str = value.replace("GMT", "+0000")
                return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f%z")
            except ValueError:
                pass
        return value


class RDDStorageInfo(BaseModel):
    id: int
    name: str
    num_partitions: Optional[int] = Field(None, alias="numPartitions")
    num_cached_partitions: Optional[int] = Field(None, alias="numCachedPartitions")
    storage_level: Optional[str] = Field(None, alias="storageLevel")
    memory_used: Optional[int] = Field(None, alias="memoryUsed")
    disk_used: Optional[int] = Field(None, alias="diskUsed")
    data_distribution: Optional[Sequence["RDDDataDistribution"]] = Field(
        None, alias="dataDistribution"
    )
    partitions: Optional[Sequence["RDDPartitionInfo"]] = None

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={datetime: lambda v: v.isoformat() if v else None},
    )


class RDDDataDistribution(BaseModel):
    address: str
    memory_used: Optional[int] = Field(None, alias="memoryUsed")
    memory_remaining: Optional[int] = Field(None, alias="memoryRemaining")
    disk_used: Optional[int] = Field(None, alias="diskUsed")
    on_heap_memory_used: Optional[int] = Field(None, alias="onHeapMemoryUsed")
    off_heap_memory_used: Optional[int] = Field(None, alias="offHeapMemoryUsed")
    on_heap_memory_remaining: Optional[int] = Field(None, alias="onHeapMemoryRemaining")
    off_heap_memory_remaining: Optional[int] = Field(
        None, alias="offHeapMemoryRemaining"
    )

    model_config = ConfigDict(populate_by_name=True)


class RDDPartitionInfo(BaseModel):
    block_name: Optional[str] = Field(None, alias="blockName")
    storage_level: Optional[str] = Field(None, alias="storageLevel")
    memory_used: Optional[int] = Field(None, alias="memoryUsed")
    disk_used: Optional[int] = Field(None, alias="diskUsed")
    executors: Sequence[str]

    model_config = ConfigDict(populate_by_name=True)


class StageData(BaseModel):
    status: str  # StageStatus as string
    stage_id: Optional[int] = Field(None, alias="stageId")
    attempt_id: Optional[int] = Field(None, alias="attemptId")
    num_tasks: Optional[int] = Field(None, alias="numTasks")
    num_active_tasks: Optional[int] = Field(None, alias="numActiveTasks")
    num_complete_tasks: Optional[int] = Field(None, alias="numCompleteTasks")
    num_failed_tasks: Optional[int] = Field(None, alias="numFailedTasks")
    num_killed_tasks: Optional[int] = Field(None, alias="numKilledTasks")
    num_completed_indices: Optional[int] = Field(None, alias="numCompletedIndices")

    submission_time: Optional[datetime] = Field(None, alias="submissionTime")
    first_task_launched_time: Optional[datetime] = Field(
        None, alias="firstTaskLaunchedTime"
    )
    completion_time: Optional[datetime] = Field(None, alias="completionTime")
    failure_reason: Optional[str] = Field(None, alias="failureReason")

    executor_deserialize_time: Optional[int] = Field(
        None, alias="executorDeserializeTime"
    )
    executor_deserialize_cpu_time: Optional[int] = Field(
        None, alias="executorDeserializeCpuTime"
    )
    executor_run_time: Optional[int] = Field(None, alias="executorRunTime")
    executor_cpu_time: Optional[int] = Field(None, alias="executorCpuTime")
    result_size: Optional[int] = Field(None, alias="resultSize")
    jvm_gc_time: Optional[int] = Field(None, alias="jvmGcTime")
    result_serialization_time: Optional[int] = Field(
        None, alias="resultSerializationTime"
    )
    memory_bytes_spilled: Optional[int] = Field(None, alias="memoryBytesSpilled")
    disk_bytes_spilled: Optional[int] = Field(None, alias="diskBytesSpilled")
    peak_execution_memory: Optional[int] = Field(None, alias="peakExecutionMemory")
    input_bytes: Optional[int] = Field(None, alias="inputBytes")
    input_records: Optional[int] = Field(None, alias="inputRecords")
    output_bytes: Optional[int] = Field(None, alias="outputBytes")
    output_records: Optional[int] = Field(None, alias="outputRecords")
    shuffle_remote_blocks_fetched: Optional[int] = Field(
        None, alias="shuffleRemoteBlocksFetched"
    )
    shuffle_local_blocks_fetched: Optional[int] = Field(
        None, alias="shuffleLocalBlocksFetched"
    )
    shuffle_fetch_wait_time: Optional[int] = Field(None, alias="shuffleFetchWaitTime")
    shuffle_remote_bytes_read: Optional[int] = Field(
        None, alias="shuffleRemoteBytesRead"
    )
    shuffle_remote_bytes_read_to_disk: Optional[int] = Field(
        None, alias="shuffleRemoteBytesReadToDisk"
    )
    shuffle_local_bytes_read: Optional[int] = Field(None, alias="shuffleLocalBytesRead")
    shuffle_read_bytes: Optional[int] = Field(None, alias="shuffleReadBytes")
    shuffle_read_records: Optional[int] = Field(None, alias="shuffleReadRecords")
    shuffle_corrupt_merged_block_chunks: Optional[int] = Field(
        0, alias="shuffleCorruptMergedBlockChunks"
    )
    shuffle_merged_fetch_fallback_count: Optional[int] = Field(
        0, alias="shuffleMergedFetchFallbackCount"
    )
    shuffle_merged_remote_blocks_fetched: Optional[int] = Field(
        0, alias="shuffleMergedRemoteBlocksFetched"
    )
    shuffle_merged_local_blocks_fetched: Optional[int] = Field(
        0, alias="shuffleMergedLocalBlocksFetched"
    )
    shuffle_merged_remote_chunks_fetched: Optional[int] = Field(
        0, alias="shuffleMergedRemoteChunksFetched"
    )
    shuffle_merged_local_chunks_fetched: Optional[int] = Field(
        0, alias="shuffleMergedLocalChunksFetched"
    )
    shuffle_merged_remote_bytes_read: Optional[int] = Field(
        0, alias="shuffleMergedRemoteBytesRead"
    )
    shuffle_merged_local_bytes_read: Optional[int] = Field(
        0, alias="shuffleMergedLocalBytesRead"
    )
    shuffle_remote_reqs_duration: Optional[int] = Field(
        0, alias="shuffleRemoteReqsDuration"
    )
    shuffle_merged_remote_reqs_duration: Optional[int] = Field(
        0, alias="shuffleMergedRemoteReqsDuration"
    )
    shuffle_write_bytes: Optional[int] = Field(None, alias="shuffleWriteBytes")
    shuffle_write_time: Optional[int] = Field(None, alias="shuffleWriteTime")
    shuffle_write_records: Optional[int] = Field(None, alias="shuffleWriteRecords")

    name: str
    description: Optional[str] = None
    details: str
    scheduling_pool: Optional[str] = Field(None, alias="schedulingPool")

    # rdd_ids: Optional[Sequence[int]] = Field(None, alias="rddIds")
    accumulator_updates: Optional[Sequence["AccumulableInfo"]] = Field(
        None, alias="accumulatorUpdates"
    )
    tasks: Optional[Dict[str, "TaskData"]] = None
    executor_summary: Optional[Dict[str, ExecutorStageSummary]] = Field(
        None, alias="executorSummary"
    )
    speculation_summary: Optional[SpeculationStageSummary] = Field(
        None, alias="speculationSummary"
    )
    killed_tasks_summary: Dict[str, int] = Field({}, alias="killedTasksSummary")
    resource_profile_id: Optional[int] = Field(None, alias="resourceProfileId")
    peak_executor_metrics: Optional[ExecutorMetrics] = Field(
        None, alias="peakExecutorMetrics"
    )
    task_metrics_distributions: Optional["TaskMetricDistributions"] = Field(
        None, alias="taskMetricsDistributions"
    )
    executor_metrics_distributions: Optional["ExecutorMetricsDistributions"] = Field(
        None, alias="executorMetricsDistributions"
    )
    is_shuffle_push_enabled: bool = Field(False, alias="isShufflePushEnabled")
    shuffle_mergers_count: Optional[int] = Field(0, alias="shuffleMergersCount")

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    @field_validator(
        "submission_time", "first_task_launched_time", "completion_time", mode="before"
    )
    @classmethod
    def parse_datetime(cls, value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000)
        if isinstance(value, str) and value.endswith("GMT"):
            # Handle Spark's ISO date format that ends with GMT
            try:
                # Remove GMT and parse
                dt_str = value.replace("GMT", "+0000")
                return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f%z")
            except ValueError:
                pass
        return value


class TaskData(BaseModel):
    task_id: Optional[int] = Field(None, alias="taskId")
    index: int
    attempt: int
    partition_id: Optional[int] = Field(None, alias="partitionId")
    launch_time: Optional[datetime] = Field(None, alias="launchTime")
    result_fetch_start: Optional[datetime] = Field(None, alias="resultFetchStart")
    duration: Optional[int] = None
    executor_id: Optional[str] = Field(None, alias="executorId")
    host: str
    status: str
    task_locality: Optional[str] = Field(None, alias="taskLocality")
    speculative: bool
    accumulator_updates: Optional[Sequence["AccumulableInfo"]] = Field(
        None, alias="accumulatorUpdates"
    )
    error_message: Optional[str] = Field(None, alias="errorMessage")
    task_metrics: Optional["TaskMetrics"] = Field(None, alias="taskMetrics")
    executor_logs: Dict[str, str] = Field({}, alias="executorLogs")
    scheduler_delay: Optional[int] = Field(0, alias="schedulerDelay")
    getting_result_time: Optional[int] = Field(0, alias="gettingResultTime")

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    @field_validator("launch_time", "result_fetch_start", mode="before")
    @classmethod
    def parse_datetime(cls, value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000)
        if isinstance(value, str) and value.endswith("GMT"):
            # Handle Spark's ISO date format that ends with GMT
            try:
                # Remove GMT and parse
                dt_str = value.replace("GMT", "+0000")
                return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f%z")
            except ValueError:
                pass
        return value


class TaskMetrics(BaseModel):
    executor_deserialize_time: Optional[int] = Field(
        None, alias="executorDeserializeTime"
    )
    executor_deserialize_cpu_time: Optional[int] = Field(
        None, alias="executorDeserializeCpuTime"
    )
    executor_run_time: Optional[int] = Field(None, alias="executorRunTime")
    executor_cpu_time: Optional[int] = Field(None, alias="executorCpuTime")
    result_size: Optional[int] = Field(None, alias="resultSize")
    jvm_gc_time: Optional[int] = Field(None, alias="jvmGcTime")
    result_serialization_time: Optional[int] = Field(
        None, alias="resultSerializationTime"
    )
    memory_bytes_spilled: Optional[int] = Field(None, alias="memoryBytesSpilled")
    disk_bytes_spilled: Optional[int] = Field(None, alias="diskBytesSpilled")
    peak_execution_memory: Optional[int] = Field(None, alias="peakExecutionMemory")
    input_metrics: Optional["InputMetrics"] = Field(None, alias="inputMetrics")
    output_metrics: Optional["OutputMetrics"] = Field(None, alias="outputMetrics")
    shuffle_read_metrics: Optional["ShuffleReadMetrics"] = Field(
        None, alias="shuffleReadMetrics"
    )
    shuffle_write_metrics: Optional["ShuffleWriteMetrics"] = Field(
        None, alias="shuffleWriteMetrics"
    )

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)


class InputMetrics(BaseModel):
    bytes_read: Optional[int] = Field(None, alias="bytesRead")
    records_read: Optional[int] = Field(None, alias="recordsRead")

    model_config = ConfigDict(populate_by_name=True)


class OutputMetrics(BaseModel):
    bytes_written: Optional[int] = Field(None, alias="bytesWritten")
    records_written: Optional[int] = Field(None, alias="recordsWritten")

    model_config = ConfigDict(populate_by_name=True)


class ShufflePushReadMetrics(BaseModel):
    corrupt_merged_block_chunks: Optional[int] = Field(
        None, alias="corruptMergedBlockChunks"
    )
    merged_fetch_fallback_count: Optional[int] = Field(
        None, alias="mergedFetchFallbackCount"
    )
    remote_merged_blocks_fetched: Optional[int] = Field(
        None, alias="remoteMergedBlocksFetched"
    )
    local_merged_blocks_fetched: Optional[int] = Field(
        None, alias="localMergedBlocksFetched"
    )
    remote_merged_chunks_fetched: Optional[int] = Field(
        None, alias="remoteMergedChunksFetched"
    )
    local_merged_chunks_fetched: Optional[int] = Field(
        None, alias="localMergedChunksFetched"
    )
    remote_merged_bytes_read: Optional[int] = Field(None, alias="remoteMergedBytesRead")
    local_merged_bytes_read: Optional[int] = Field(None, alias="localMergedBytesRead")
    remote_merged_reqs_duration: Optional[int] = Field(
        None, alias="remoteMergedReqsDuration"
    )

    model_config = ConfigDict(populate_by_name=True)


class ShuffleReadMetrics(BaseModel):
    remote_blocks_fetched: Optional[int] = Field(None, alias="remoteBlocksFetched")
    local_blocks_fetched: Optional[int] = Field(None, alias="localBlocksFetched")
    fetch_wait_time: Optional[int] = Field(None, alias="fetchWaitTime")
    remote_bytes_read: Optional[int] = Field(None, alias="remoteBytesRead")
    remote_bytes_read_to_disk: Optional[int] = Field(
        None, alias="remoteBytesReadToDisk"
    )
    local_bytes_read: Optional[int] = Field(None, alias="localBytesRead")
    records_read: Optional[int] = Field(None, alias="recordsRead")
    remote_reqs_duration: Optional[int] = Field(None, alias="remoteReqsDuration")
    shuffle_push_read_metrics: Optional[ShufflePushReadMetrics] = Field(
        None, alias="shufflePushReadMetrics"
    )

    model_config = ConfigDict(populate_by_name=True)


class ShuffleWriteMetrics(BaseModel):
    bytes_written: Optional[int] = Field(None, alias="bytesWritten")
    write_time: Optional[int] = Field(None, alias="writeTime")
    records_written: Optional[int] = Field(None, alias="recordsWritten")

    model_config = ConfigDict(populate_by_name=True)


class TaskMetricDistributions(BaseModel):
    quantiles: Optional[Sequence[float]] = Field(None, alias="quantiles")

    duration: Optional[Sequence[float]] = Field(None, alias="duration")
    executor_deserialize_time: Optional[Sequence[float]] = Field(
        None, alias="executorDeserializeTime"
    )
    executor_deserialize_cpu_time: Optional[Sequence[float]] = Field(
        None, alias="executorDeserializeCpuTime"
    )
    executor_run_time: Optional[Sequence[float]] = Field(None, alias="executorRunTime")
    executor_cpu_time: Optional[Sequence[float]] = Field(None, alias="executorCpuTime")
    result_size: Optional[Sequence[float]] = Field(None, alias="resultSize")
    jvm_gc_time: Optional[Sequence[float]] = Field(None, alias="jvmGcTime")
    result_serialization_time: Optional[Sequence[float]] = Field(
        None, alias="resultSerializationTime"
    )
    getting_result_time: Optional[Sequence[float]] = Field(
        None, alias="gettingResultTime"
    )
    scheduler_delay: Optional[Sequence[float]] = Field(None, alias="schedulerDelay")
    peak_execution_memory: Optional[Sequence[float]] = Field(
        None, alias="peakExecutionMemory"
    )
    memory_bytes_spilled: Optional[Sequence[float]] = Field(
        None, alias="memoryBytesSpilled"
    )
    disk_bytes_spilled: Optional[Sequence[float]] = Field(
        None, alias="diskBytesSpilled"
    )

    input_metrics: Optional["InputMetricDistributions"] = Field(
        None, alias="inputMetrics"
    )
    output_metrics: Optional["OutputMetricDistributions"] = Field(
        None, alias="outputMetrics"
    )
    shuffle_read_metrics: Optional["ShuffleReadMetricDistributions"] = Field(
        None, alias="shuffleReadMetrics"
    )
    shuffle_write_metrics: Optional["ShuffleWriteMetricDistributions"] = Field(
        None, alias="shuffleWriteMetrics"
    )

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)


class InputMetricDistributions(BaseModel):
    bytes_read: Optional[Sequence[float]] = Field(None, alias="bytesRead")
    records_read: Optional[Sequence[float]] = Field(None, alias="recordsRead")

    model_config = ConfigDict(populate_by_name=True)


class OutputMetricDistributions(BaseModel):
    bytes_written: Optional[Sequence[float]] = Field(None, alias="bytesWritten")
    records_written: Optional[Sequence[float]] = Field(None, alias="recordsWritten")

    model_config = ConfigDict(populate_by_name=True)


class ShufflePushReadMetricDistributions(BaseModel):
    corrupt_merged_block_chunks: Optional[Sequence[float]] = Field(
        None, alias="corruptMergedBlockChunks"
    )
    merged_fetch_fallback_count: Optional[Sequence[float]] = Field(
        None, alias="mergedFetchFallbackCount"
    )
    remote_merged_blocks_fetched: Optional[Sequence[float]] = Field(
        None, alias="remoteMergedBlocksFetched"
    )
    local_merged_blocks_fetched: Optional[Sequence[float]] = Field(
        None, alias="localMergedBlocksFetched"
    )
    remote_merged_chunks_fetched: Optional[Sequence[float]] = Field(
        None, alias="remoteMergedChunksFetched"
    )
    local_merged_chunks_fetched: Optional[Sequence[float]] = Field(
        None, alias="localMergedChunksFetched"
    )
    remote_merged_bytes_read: Optional[Sequence[float]] = Field(
        None, alias="remoteMergedBytesRead"
    )
    local_merged_bytes_read: Optional[Sequence[float]] = Field(
        None, alias="localMergedBytesRead"
    )
    remote_merged_reqs_duration: Optional[Sequence[float]] = Field(
        None, alias="remoteMergedReqsDuration"
    )

    model_config = ConfigDict(populate_by_name=True)


class ExecutorMetricsDistributions(BaseModel):
    quantiles: Sequence[float]

    task_time: Optional[Sequence[float]] = Field(None, alias="taskTime")
    failed_tasks: Optional[Sequence[float]] = Field(None, alias="failedTasks")
    succeeded_tasks: Optional[Sequence[float]] = Field(None, alias="succeededTasks")
    killed_tasks: Optional[Sequence[float]] = Field(None, alias="killedTasks")
    input_bytes: Optional[Sequence[float]] = Field(None, alias="inputBytes")
    input_records: Optional[Sequence[float]] = Field(None, alias="inputRecords")
    output_bytes: Optional[Sequence[float]] = Field(None, alias="outputBytes")
    output_records: Optional[Sequence[float]] = Field(None, alias="outputRecords")
    shuffle_read: Optional[Sequence[float]] = Field(None, alias="shuffleRead")
    shuffle_read_records: Optional[Sequence[float]] = Field(
        None, alias="shuffleReadRecords"
    )
    shuffle_write: Optional[Sequence[float]] = Field(None, alias="shuffleWrite")
    shuffle_write_records: Optional[Sequence[float]] = Field(
        None, alias="shuffleWriteRecords"
    )
    memory_bytes_spilled: Optional[Sequence[float]] = Field(
        None, alias="memoryBytesSpilled"
    )
    disk_bytes_spilled: Optional[Sequence[float]] = Field(
        None, alias="diskBytesSpilled"
    )
    peak_memory_metrics: Optional["ExecutorPeakMetricsDistributions"] = Field(
        None, alias="peakMemoryMetrics"
    )

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)


class ExecutorPeakMetricsDistributions(BaseModel):
    quantiles: Sequence[float]
    executor_metrics: Optional[Sequence[ExecutorMetrics]] = Field(
        None, alias="executorMetrics"
    )

    model_config = ConfigDict(populate_by_name=True)


class ShuffleReadMetricDistributions(BaseModel):
    read_bytes: Optional[Sequence[float]] = Field(None, alias="readBytes")
    read_records: Optional[Sequence[float]] = Field(None, alias="readRecords")
    remote_blocks_fetched: Optional[Sequence[float]] = Field(
        None, alias="remoteBlocksFetched"
    )
    local_blocks_fetched: Optional[Sequence[float]] = Field(
        None, alias="localBlocksFetched"
    )
    fetch_wait_time: Optional[Sequence[float]] = Field(None, alias="fetchWaitTime")
    remote_bytes_read: Optional[Sequence[float]] = Field(None, alias="remoteBytesRead")
    remote_bytes_read_to_disk: Optional[Sequence[float]] = Field(
        None, alias="remoteBytesReadToDisk"
    )
    total_blocks_fetched: Optional[Sequence[float]] = Field(
        None, alias="totalBlocksFetched"
    )
    remote_reqs_duration: Optional[Sequence[float]] = Field(
        None, alias="remoteReqsDuration"
    )
    shuffle_push_read_metrics_dist: Optional[ShufflePushReadMetricDistributions] = (
        Field(None, alias="shufflePushReadMetricsDist")
    )

    model_config = ConfigDict(populate_by_name=True)


class ShuffleWriteMetricDistributions(BaseModel):
    write_bytes: Optional[Sequence[float]] = Field(None, alias="writeBytes")
    write_records: Optional[Sequence[float]] = Field(None, alias="writeRecords")
    write_time: Optional[Sequence[float]] = Field(None, alias="writeTime")

    model_config = ConfigDict(populate_by_name=True)


class AccumulableInfo(BaseModel):
    id: int
    name: str
    update: str = None
    value: str

    model_config = ConfigDict(populate_by_name=True)


class VersionInfo(BaseModel):
    spark: str

    model_config = ConfigDict(populate_by_name=True)


class ApplicationEnvironmentInfo(BaseModel):
    runtime: "RuntimeInfo"
    spark_properties: Optional[Sequence[tuple[str, str]]] = Field(
        None, alias="sparkProperties"
    )
    hadoop_properties: Optional[Sequence[tuple[str, str]]] = Field(
        None, alias="hadoopProperties"
    )
    system_properties: Optional[Sequence[tuple[str, str]]] = Field(
        None, alias="systemProperties"
    )
    metrics_properties: Optional[Sequence[tuple[str, str]]] = Field(
        None, alias="metricsProperties"
    )
    classpath_entries: Optional[Sequence[tuple[str, str]]] = Field(
        None, alias="classpathEntries"
    )
    resource_profiles: Optional[Sequence[ResourceProfileInfo]] = Field(
        None, alias="resourceProfiles"
    )

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)


class RuntimeInfo(BaseModel):
    java_version: Optional[str] = Field(None, alias="javaVersion")
    java_home: Optional[str] = Field(None, alias="javaHome")
    scala_version: Optional[str] = Field(None, alias="scalaVersion")

    model_config = ConfigDict(populate_by_name=True)


class StackTrace(BaseModel):
    elems: Sequence[str]

    def __str__(self) -> str:
        return "".join(self.elems)

    def html(self) -> str:
        return "<br />".join(elem.rstrip() for elem in self.elems)

    def mkstring(self, start: str, sep: str, end: str) -> str:
        return start + sep.join(self.elems) + end

    model_config = ConfigDict(populate_by_name=True)


class ThreadStackTrace(BaseModel):
    thread_id: Optional[int] = Field(None, alias="threadId")
    thread_name: Optional[str] = Field(None, alias="threadName")
    thread_state: Optional[str] = Field(
        None, alias="threadState"
    )  # ThreadState as string
    stack_trace: Optional[StackTrace] = Field(None, alias="stackTrace")
    blocked_by_thread_id: Optional[int] = Field(None, alias="blockedByThreadId")
    blocked_by_lock: Optional[str] = Field(None, alias="blockedByLock")
    holding_locks: Sequence[str] = Field([], alias="holdingLocks")  # deprecated
    synchronizers: Sequence[str]
    monitors: Sequence[str]
    lock_name: Optional[str] = Field(None, alias="lockName")
    lock_owner_name: Optional[str] = Field(None, alias="lockOwnerName")
    suspended: bool
    in_native: Optional[bool] = Field(None, alias="inNative")
    is_daemon: Optional[bool] = Field(None, alias="isDaemon")
    priority: int

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)


class ProcessSummary(BaseModel):
    id: str
    host_port: Optional[str] = Field(None, alias="hostPort")
    is_active: Optional[bool] = Field(None, alias="isActive")
    total_cores: Optional[int] = Field(None, alias="totalCores")
    add_time: Optional[datetime] = Field(None, alias="addTime")
    remove_time: Optional[datetime] = Field(None, alias="removeTime")
    process_logs: Optional[Dict[str, str]] = Field(None, alias="processLogs")

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    @field_validator("add_time", "remove_time", mode="before")
    @classmethod
    def parse_datetime(cls, value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000)
        if isinstance(value, str) and value.endswith("GMT"):
            # Handle Spark's ISO date format that ends with GMT
            try:
                # Remove GMT and parse
                dt_str = value.replace("GMT", "+0000")
                return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f%z")
            except ValueError:
                pass
        return value


class SQLExecutionStatus(str, Enum):
    """Represents the status of a SQL execution."""

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Metric(BaseModel):
    """Represents a metric in a SQL execution plan node."""

    name: str
    value: str

    model_config = ConfigDict(populate_by_name=True)


class Node(BaseModel):
    """Represents a node in a SQL execution plan."""

    node_id: int = Field(..., alias="nodeId")
    node_name: str = Field(..., alias="nodeName")
    whole_stage_codegen_id: Optional[int] = Field(None, alias="wholeStageCodegenId")
    metrics: Sequence[Metric]

    model_config = ConfigDict(populate_by_name=True)


class SparkPlanGraphEdge(BaseModel):
    """Represents an edge in a SQL execution plan graph."""

    from_id: int = Field(..., alias="fromId")
    to_id: int = Field(..., alias="toId")

    model_config = ConfigDict(populate_by_name=True)


class ExecutionData(BaseModel):
    """Represents data about a SQL execution."""

    id: int
    status: str  # SQLExecutionStatus as string
    description: Optional[str] = Field(None, alias="planDescription")
    plan_description: str = Field(..., alias="planDescription")
    submission_time: datetime = Field(..., alias="submissionTime")
    duration: Optional[int] = Field(None, alias="durationMilliSeconds")
    running_job_ids: Sequence[int] = Field([], alias="runningJobIds")
    success_job_ids: Sequence[int] = Field([], alias="successJobIds")
    failed_job_ids: Sequence[int] = Field([], alias="failedJobIds")
    nodes: Sequence[Node]
    edges: Sequence[SparkPlanGraphEdge]

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    @field_validator("submission_time", mode="before")
    @classmethod
    def parse_datetime(cls, value):
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000)
        if isinstance(value, str) and value.endswith("GMT"):
            # Handle Spark's ISO date format that ends with GMT
            try:
                # Remove GMT and parse
                dt_str = value.replace("GMT", "+0000")
                return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f%z")
            except ValueError:
                pass
        return value

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutionData":
        """Create an ExecutionData instance from a dictionary."""
        return cls.model_validate(data)


class SparkPlanGraph(BaseModel):
    """Represents a Spark plan graph."""

    nodes: Sequence[Node]
    edges: Sequence[SparkPlanGraphEdge]
    all_nodes: Sequence[Node] = Field([], alias="allNodes")


class SparkPlanGraphNode(BaseModel):
    """Base class for nodes in a Spark plan graph."""

    id: int
    name: str
    metrics: Sequence[Any] = []


class SparkPlanGraphCluster(SparkPlanGraphNode):
    """Represents a cluster of nodes in a Spark plan graph."""

    nodes: Sequence[SparkPlanGraphNode]


# Forward references for type hints
class ExecutorResourceRequest(BaseModel):
    pass


class TaskResourceRequest(BaseModel):
    pass


class ResourceInformation(BaseModel):
    pass


class SparkUI(BaseModel):
    pass


# Update forward references
ShufflePushReadMetrics.model_rebuild()
ShufflePushReadMetricDistributions.model_rebuild()
ExecutorMetricsDistributions.model_rebuild()
ApplicationInfo.model_rebuild()
ApplicationAttemptInfo.model_rebuild()
ResourceProfileInfo.model_rebuild()
ExecutorStageSummary.model_rebuild()
ExecutorSummary.model_rebuild()
JobData.model_rebuild()
RDDStorageInfo.model_rebuild()
RDDDataDistribution.model_rebuild()
StageData.model_rebuild()
TaskData.model_rebuild()
TaskMetrics.model_rebuild()
TaskMetricDistributions.model_rebuild()
ShuffleReadMetricDistributions.model_rebuild()
ApplicationEnvironmentInfo.model_rebuild()
