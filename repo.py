from dagster import Definitions, load_assets_from_modules
from assets import migrate_min_pg_asset

all_assets = load_assets_from_modules([migrate_min_pg_asset])

defs = Definitions(
    assets=all_assets,
)