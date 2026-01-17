from dagster import define_asset_job

retail_job = define_asset_job(
    name="retail_lakehouse_job"
)
