"""
Minimal DAG: dbt -> Nessie -> Iceberg on Dell ECS.

Fixes the "missing hive packages" error without rebuilding your image and
without patching dbt-spark's session.py.

Why it works: dbt-spark hard-codes SparkSession.builder.enableHiveSupport(),
which is just config("spark.sql.catalogImplementation", "hive"). That key is a
STATIC Spark config, so if a SparkSession already exists in the process,
getOrCreate() returns the existing one and silently drops it. So we create the
session first, then call dbt in the same process via dbtRunner.

You will see this WARN in the logs. It is expected - it is the fix working:
    WARN SparkSession: Using an existing Spark session; only runtime SQL
    configurations will take effect.

Requires: profiles.yml with `method: session` (already the case if you are
hitting the Hive error), and the Iceberg + Nessie jars already on the
classpath in your image.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.models import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

# ---------------------------------------------------------------------------
# EDIT THESE
# ---------------------------------------------------------------------------
NAMESPACE = "data-platform"
DBT_IMAGE = "registry.internal/dbt-spark-nessie:latest"
DBT_PROJECT_DIR = "/dbt"

ENV_VARS = {
    "NESSIE_URI": "http://nessie.data-platform.svc.cluster.local:19120/api/v2",
    "NESSIE_REF": "main",
    "NESSIE_CATALOG": "nessie",
    "ECS_ENDPOINT": "https://ecs.internal:9021",
    "ECS_WAREHOUSE": "s3://lakehouse-warehouse/iceberg",
    "ECS_REGION": "us-east-1",
    "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    "DBT_TARGET_PATH": "/tmp/dbt-target",
    "DBT_LOG_PATH": "/tmp/dbt-logs",
}

SECRETS = [
    Secret("env", "ECS_ACCESS_KEY_ID", "dell-ecs-credentials", "access_key_id"),
    Secret("env", "ECS_SECRET_ACCESS_KEY", "dell-ecs-credentials", "secret_access_key"),
    Secret("env", "NESSIE_TOKEN", "nessie-auth", "token"),
]

# ---------------------------------------------------------------------------
# Runs inside the pod. Creates the session, then hands over to dbt.
# ---------------------------------------------------------------------------
BOOTSTRAP = r'''
import os, sys, logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("bootstrap")

from pyspark.sql import SparkSession

cat = os.environ.get("NESSIE_CATALOG", "nessie")
conf = {
    "spark.sql.catalogImplementation": "in-memory",          # <-- kills Hive
    "spark.sql.extensions":
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    f"spark.sql.catalog.{cat}": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{cat}.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
    f"spark.sql.catalog.{cat}.uri": os.environ["NESSIE_URI"],
    f"spark.sql.catalog.{cat}.ref": os.environ.get("NESSIE_REF", "main"),
    f"spark.sql.catalog.{cat}.warehouse": os.environ["ECS_WAREHOUSE"],
    f"spark.sql.catalog.{cat}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    f"spark.sql.catalog.{cat}.s3.endpoint": os.environ["ECS_ENDPOINT"],
    f"spark.sql.catalog.{cat}.s3.path-style-access": "true",
    f"spark.sql.catalog.{cat}.s3.access-key-id": os.environ["ECS_ACCESS_KEY_ID"],
    f"spark.sql.catalog.{cat}.s3.secret-access-key": os.environ["ECS_SECRET_ACCESS_KEY"],
    f"spark.sql.catalog.{cat}.client.region": os.environ.get("ECS_REGION", "us-east-1"),
    "spark.sql.defaultCatalog": cat,
    "spark.sql.warehouse.dir": "/tmp/spark-warehouse",
}
if os.environ.get("NESSIE_TOKEN"):
    conf[f"spark.sql.catalog.{cat}.authentication.type"] = "BEARER"
    conf[f"spark.sql.catalog.{cat}.authentication.token"] = os.environ["NESSIE_TOKEN"]

builder = SparkSession.builder.appName("dbt").master(os.environ.get("SPARK_MASTER", "local[*]"))
for k, v in conf.items():
    builder = builder.config(k, v)

# NOTE: no .enableHiveSupport() here. That is the whole trick.
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

impl = spark.conf.get("spark.sql.catalogImplementation")
log.info("Spark %s ready | catalogImplementation=%s", spark.version, impl)
if impl != "in-memory":
    log.error("Hive leaked in - aborting before dbt starts")
    sys.exit(2)

try:
    log.info("Nessie namespaces: %s",
             [r[0] for r in spark.sql("SHOW NAMESPACES IN " + cat).collect()])
except Exception:
    log.exception("Cannot reach Nessie or ECS")
    sys.exit(2)

from dbt.cli.main import dbtRunner
args = list(sys.argv[1:]) + ["--no-use-colors"]
log.info("dbt %s", " ".join(args))
result = dbtRunner().invoke(args)

for node in getattr(result.result, "results", []) or []:
    uid = getattr(getattr(node, "node", None), "unique_id", "?")
    log.info("  %-50s %s", uid, node.status)
    if str(node.status).lower() in ("error", "fail"):
        log.error("FAILED %s: %s", uid, node.message)

if result.exception:
    log.error("dbt raised", exc_info=result.exception)

spark.stop()
sys.exit(0 if result.success else 1)
'''

with DAG(
    dag_id="dbt_iceberg_nessie",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/London"),
    schedule=None,
    catchup=False,
    tags=["dbt", "iceberg", "nessie"],
) as dag:

    dbt_run = KubernetesPodOperator(
        task_id="dbt_run",
        name="dbt-run",
        namespace=NAMESPACE,
        image=DBT_IMAGE,
        cmds=["python3", "-c"],
        arguments=[BOOTSTRAP, "run", "--project-dir", DBT_PROJECT_DIR],
        env_vars=ENV_VARS,
        secrets=SECRETS,
        get_logs=True,
        log_events_on_failure=True,
        on_finish_action="delete_succeeded_pod",
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )
