from dagster import Definitions, load_assets_from_modules
# from dagster_duckdb_polars import DuckDBPolarsIOManager

from TrackFast import assets  # noqa: TID252
from TrackFast.jobs.job01_create_from_raw_to_transactions import job01_create_files_from_raw_end2end

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[
        job01_create_files_from_raw_end2end
    ]
)
