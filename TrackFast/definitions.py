from dagster import Definitions, load_assets_from_modules

from TrackFast import assets  # noqa: TID252
from TrackFast.jobs.prepare_raw import prepare_input_by_password_removal

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[prepare_input_by_password_removal]
)
