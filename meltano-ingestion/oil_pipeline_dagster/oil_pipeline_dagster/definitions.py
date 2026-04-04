from dagster import Definitions
from oil_pipeline_dagster.assets import meltano_extract_load, dbt_transformations, dbt_tests

defs = Definitions(
    assets=[meltano_extract_load, dbt_transformations, dbt_tests],
)
