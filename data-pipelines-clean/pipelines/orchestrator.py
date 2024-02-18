##Base Imports
from datetime import date, timedelta
from CLIENTNAME_pipelines.config import Config
from prefect.storage import S3
from slugify import slugify
import pendulum
from CLIENTNAME_pipelines.utils.run_configs import standard_run_config
from prefect import Client
from prefect.schedules import Schedule
from CLIENTNAME_pipelines.utils.standard_clocks import daily_clock, weekly_clock, hourly_clock
import os

ENVIRONMENT = os.environ.get("CI_COMMIT_BRANCH", None)
if not ENVIRONMENT or ENVIRONMENT not in ["dev", "test", "prod"]:
    ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")

SET_SCHEDULE_ACTIVE = ENVIRONMENT == "dev"  # Will be set to 'prod' when ready
RUN_ENV = {"ENVIRONMENT": ENVIRONMENT, "HOST": "aws"}
default_conf = Config(**RUN_ENV)
run_conf = standard_run_config(default_conf, cpu=256, memory=512, storage=None)
project_name = default_conf.project_name


def clock_shifter(shift):
    r = 0
    while True:
        yield r
        r += shift


shifter = clock_shifter(2).__next__


def register_flow(flow_func, clocks, **kwargs):
    if type(clocks) is not list:
        clocks = [clocks]
    flow = flow_func(run_config=run_conf, schedule=Schedule(clocks=clocks), **kwargs)
    flow.storage = S3(
        bucket=default_conf.infra_bucket,
        key=f"prefect_flows/{slugify(flow.name)}/{slugify(pendulum.now('utc').isoformat())}",
    )
    flow.register(project_name, labels=[], set_schedule_active=SET_SCHEDULE_ACTIVE)


################################################################
## Disable All Flows

# Flows in this script will automatically be set to active when registered.
# Disabling all flows first will turn off any flows that aren't meant to be orchestrated
#     without deleting those flows or their logs
client = Client()
query = {
    "mutation": {
        "update_flow(where: {is_schedule_active: {_eq: true}} _set: {is_schedule_active:false})": {
            "returning": ["name", "is_schedule_active", "version"]
        }
    }
}
client.graphql(query)
################################################################
## Vendor1 Reports

from flows.vendor1.vendor1_reports import Vendor1Flow

vendor1_channels = [
    "CLIENTNAME_items",
    "CLIENTNAME_documentaries",
    "CLIENTNAME_television",
    "CLIENTNAME_features",
    "CLIENTNAME_main_channel",
]
yt_clocks = [
    daily_clock(shift=shifter(), channel_name=yt, days_back=7)
    for yt in vendor1_channels
]
register_flow(Vendor1Flow, clocks=yt_clocks, **RUN_ENV)

################################################################
## Outlook Attachment Flows

## Vendor3
from flows.outlook.vendor3.vendor3_flows import VENDOR3_JOBS, Vendor3OutlookFlow

amz_clocks = [
    weekly_clock(day_of_week=0, minute=shifter(), amz_flow_name=fn, days_back=7)
    for fn in VENDOR3_JOBS.keys()
]
register_flow(Vendor3OutlookFlow, clocks=amz_clocks, **RUN_ENV)

## Apple Vendor4
from flows.outlook.vendor4.vendor4_flow import Vendor4OutlookFlow

register_flow(
    Vendor4OutlookFlow,
    clocks=weekly_clock(day_of_week=1, minute=shifter(), days_back=7),
    **RUN_ENV,
)

## Vendor5TV
from flows.outlook.vendor5.vendor5_flow import Vendor5OutlookFlow

register_flow(
    Vendor5OutlookFlow,
    clocks=weekly_clock(day_of_week=2, minute=shifter(), days_back=7),
    **RUN_ENV,
)

## Vendor6
from flows.outlook.vendor6.vendor6_flow import Vendor6OutlookFlow

register_flow(
    Vendor6OutlookFlow,
    clocks=weekly_clock(day_of_week=3, minute=shifter(), days_back=7),
    **RUN_ENV,
)

## Vendor7TV
from flows.outlook.vendor7.vendor7_flow import Vendor7OutlookFlow

register_flow(
    Vendor7OutlookFlow,
    clocks=weekly_clock(day_of_week=4, minute=shifter(), days_back=7),
    **RUN_ENV,
)


################################################################
## DataSource1 CDN Logs
from flows.data_source1.CLIENTNAME_dar import FtDarFlow

register_flow(FtDarFlow, clocks=daily_clock(shift=shifter()), **RUN_ENV)


################################################################
## DataSource1 Detailed Asset Report
from flows.data_source1.cdn_logs import FtCdnLogFlow

register_flow(FtCdnLogFlow, clocks=daily_clock(shift=shifter()), **RUN_ENV)


################################################################
## DBT Flow
from flows.dbt_flow import DbtFlow

register_flow(DbtFlow, clocks=daily_clock(shift=shifter() + 150), **RUN_ENV)

################################################################
## DATASOURCE2 Titles Flow
from flows.DataSource2.DataSource2_title_match import Datasource2TitleMatchFlow

register_flow(Datasource2TitleMatchFlow, clocks=hourly_clock(limit=500), **RUN_ENV)
