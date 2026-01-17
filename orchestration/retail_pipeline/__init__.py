from dagster import Definitions

from .assets_infra import start_infrastructure
from .assets import bronze_ingestion, silver_processing, gold_kpis
from .jobs import retail_job
from .schedules import daily_gold_schedule

defs = Definitions(
    assets=[
        start_infrastructure,
        bronze_ingestion,
        silver_processing,
        gold_kpis,
    ],
    jobs=[retail_job],
    schedules=[daily_gold_schedule],
)
