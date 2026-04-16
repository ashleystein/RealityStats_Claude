from dagster import Definitions, load_assets_from_modules

from src.data_pipeline.defs import assets
from src.data_pipeline.defs.assets import analytics_page_refresh_job, cast_info_job

# Load all assets from the imported modules
all_assets = load_assets_from_modules([assets])
print('####################')
print([asset.key for asset in all_assets])  # Should include 'currently_airing_episodes'

defs = Definitions(
    assets=all_assets,
    jobs=[analytics_page_refresh_job, cast_info_job]

)

