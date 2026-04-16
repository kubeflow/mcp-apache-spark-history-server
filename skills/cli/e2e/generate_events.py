"""Generate Spark event logs covering jobs, stages, failures, SQL, and executor metrics."""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "file:///tmp/spark-events")
    .config("spark.eventLog.rolling.enabled", "false")
    .getOrCreate()
)

sc = spark.sparkContext

# ---------------------------------------------------------------------------
# Job 1: simple successful write
# ---------------------------------------------------------------------------
spark.range(1000).selectExpr("id", "id * 2 as doubled").write.mode("overwrite").parquet(
    "/tmp/e2e/job1"
)

# ---------------------------------------------------------------------------
# Job 2: multi-stage shuffle
# ---------------------------------------------------------------------------
spark.range(1000).selectExpr("id % 10 as key", "id as value").groupBy(
    "key"
).count().write.mode("overwrite").parquet("/tmp/e2e/job2")

# ---------------------------------------------------------------------------
# Job 3: flaky tasks — fail once per partition, succeed on retry
# ---------------------------------------------------------------------------
import os


def flaky_partition(index, iterator):
    marker = f"/tmp/e2e-marker-{index}"
    if not os.path.exists(marker):
        open(marker, "w").close()
        raise Exception(f"transient failure for partition {index}")
    return (x * 2 for x in iterator)


sc.parallelize(range(20), 4).mapPartitionsWithIndex(flaky_partition).collect()

# ---------------------------------------------------------------------------
# Job 4: failing job — tasks throw, job fails, script continues
# ---------------------------------------------------------------------------
try:
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType

    @udf(IntegerType())
    def fail_udf(x):
        raise Exception("deliberate permanent failure")

    spark.range(10).select(fail_udf("id")).collect()
except Exception:
    pass

# ---------------------------------------------------------------------------
# Job 4: cache a dataset to populate executor memory/storage metrics
# ---------------------------------------------------------------------------
big_df = spark.range(500000).selectExpr(
    "id", "id % 50 as dept", "id % 7 as region", "rand() * 1000 as salary"
)
big_df.cache()
big_df.count()

# ---------------------------------------------------------------------------
# SQL queries — complex plans with joins, windows, aggregations
# ---------------------------------------------------------------------------
big_df.createOrReplaceTempView("employees")
spark.range(50).selectExpr(
    "id as dept", "concat('dept_', id) as dept_name"
).createOrReplaceTempView("departments")

# SQL 1: join + window + aggregation + sort
spark.sql("""
    SELECT
        d.dept_name,
        e.region,
        count(*) as headcount,
        avg(e.salary) as avg_salary,
        sum(e.salary) as total_salary,
        rank() OVER (PARTITION BY e.region ORDER BY avg(e.salary) DESC) as rnk
    FROM employees e
    JOIN departments d ON e.dept = d.dept
    GROUP BY d.dept_name, e.region
    ORDER BY region, rnk
""").collect()

# SQL 2: self-join with subqueries
spark.sql("""
    SELECT a.dept, a.avg_sal, b.max_sal
    FROM (SELECT dept, avg(salary) as avg_sal FROM employees GROUP BY dept) a
    JOIN (SELECT dept, max(salary) as max_sal FROM employees GROUP BY dept) b
    ON a.dept = b.dept
    WHERE a.avg_sal > 400
""").collect()

# SQL 3: large sort to trigger spill under constrained memory
spark.range(2000000).selectExpr(
    "id", "rand() as r1", "rand() as r2", "rand() as r3"
).sort("r1", "r2", "r3").write.mode("overwrite").parquet("/tmp/e2e/spill_output")

# SQL 4: write output
spark.sql("SELECT dept, sum(salary) as total FROM employees GROUP BY dept").write.mode(
    "overwrite"
).parquet("/tmp/e2e/sql_output")

spark.stop()
