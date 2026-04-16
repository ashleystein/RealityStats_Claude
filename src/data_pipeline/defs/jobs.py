from dagster import define_asset_job

analytics_page_refresh_job = define_asset_job(
    name="analytics_page_refresh_job", selection="currently_airing_episodes*"
)