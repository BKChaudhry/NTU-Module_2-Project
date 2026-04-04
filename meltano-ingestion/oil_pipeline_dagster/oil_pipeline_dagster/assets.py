import subprocess
from dagster import asset, OpExecutionContext

MELTANO_DIR = "/home/rakhi/meltano-ingestion"
DBT_DIR = "/home/rakhi/meltano-ingestion/oil_pipeline_dbt"

@asset
def meltano_extract_load(context: OpExecutionContext):
    context.log.info("Running Meltano: tap-csv -> target-bigquery")
    result = subprocess.run(
        ["meltano", "run", "tap-csv", "target-bigquery"],
        cwd=MELTANO_DIR, capture_output=True, text=True
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Meltano failed: {result.stderr}")
    return "Meltano EL complete"

@asset(deps=[meltano_extract_load])
def dbt_transformations(context: OpExecutionContext):
    context.log.info("Running dbt models")
    result = subprocess.run(
        ["dbt", "run"], cwd=DBT_DIR, capture_output=True, text=True
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run failed: {result.stderr}")
    return "dbt transformations complete"

@asset(deps=[dbt_transformations])
def dbt_tests(context: OpExecutionContext):
    context.log.info("Running dbt tests")
    result = subprocess.run(
        ["dbt", "test"], cwd=DBT_DIR, capture_output=True, text=True
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt tests failed: {result.stderr}")
    return "dbt tests passed"
