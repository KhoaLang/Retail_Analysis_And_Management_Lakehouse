from dagster import ScheduleDefinition
from .jobs import retail_job

daily_gold_schedule = ScheduleDefinition(
    job=retail_job,
    cron_schedule="0 1 * * *"
)
