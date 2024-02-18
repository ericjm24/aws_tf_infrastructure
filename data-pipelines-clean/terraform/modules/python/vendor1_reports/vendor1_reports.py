import requests
import boto3
import os
import json
import urllib.parse
from base64 import b64decode
from datetime import datetime as dt
import pandas as pd


S3_BUCKET = os.environ.get("S3_LANDING_BUCKET", "CLIENTNAME-ds-data-landing")
ENV = os.environ.get("ENVIRONMENT", "dev")
METRICS = [
    "views",
    "redViews",
    "comments",
    "likes",
    "dislikes",
    "itemsAddedToPlaylists",
    "itemsRemovedFromPlaylists",
    "shares",
    "estimatedMinutesWatched",
    "estimatedRedMinutesWatched",
    "averageViewDuration",
    "averageViewPercentage",
    "annotationClickThroughRate",
    "annotationCloseRate",
    "annotationImpressions",
    "annotationClickableImpressions",
    "annotationClosableImpressions",
    "annotationClicks",
    "annotationCloses",
    "cardClickRate",
    "cardTeaserClickRate",
    "cardImpressions",
    "cardTeaserImpressions",
    "cardClicks",
    "cardTeaserClicks",
    "subscribersGained",
    "subscribersLost",
    "estimatedRevenue",
    "estimatedAdRevenue",
    "grossRevenue",
    "estimatedRedPartnerRevenue",
    "monetizedPlaybacks",
    "playbackBasedCpm",
    "adImpressions",
    "cpm",
]
# Helper function to write outlook attachments to s3
s3 = boto3.client("s3")


def build_url(base_url, **kwargs):
    if not kwargs:
        return base_url
    base_url_parts = urllib.parse.urlparse(base_url)._asdict()
    if base_url_parts["query"]:
        query = {
            k: v
            for k, v in [x.split("=", 1) for x in base_url_parts["query"].split("&")]
        }
    else:
        query = {}
    for k, v in kwargs.items():
        if type(v) is list:
            query.update({k: ",".join(str(x) for x in v)})
        else:
            query.update({k: str(v)})
    base_url_parts["query"] = urllib.parse.urlencode(
        query, quote_via=urllib.parse.quote
    )
    return urllib.parse.ParseResult(**base_url_parts).geturl()


def save_attachment_to_s3(att, name):
    key = f"{ENV}/vendor1_api/{name}"
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=att)
    except:
        return None
    else:
        return f"{S3_BUCKET}/{key}"


def get_item_ids(input_files, column="item_id"):
    items = []
    for file in input_files:
        bucket, key = file.split("/", 1)
        response = s3.get_object(Bucket=bucket, Key=key)
        try:
            data = pd.read_csv(response.get("Body"))
        except Exception:
            print(Exception)
            continue
        items += list(data[column].dropna().unique())
    items = list(set(items))
    print(f"Found {len(items)} unique item ids.")
    return items


class AccessTokenExpiredException(Exception):
    pass


class AutomatedAccess:
    def __init__(self, access_credentials, channel_name):
        if type(access_credentials) is not dict:
            raise TypeError(
                f"Expected access_credentials as a type <dict>, instead received as type {type(access_credentials)}."
            )
        for k in ["client_id", "client_secret", "refresh_token"]:
            if k not in access_credentials.keys():
                raise TypeError(f"Access Credentials missing required key {k}")
        self.__dict__.update(access_credentials)
        self.channel_name = channel_name

    def refresh_access(self):

        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
        }
        response = requests.post(self.OAUTH_REFRESH_URL, data=data)
        self.access_token = json.loads(response.content)["access_token"]

    def reset_credentials(self, access_credentials):
        self.__dict__.update(access_credentials)

    def _vendor1_get(self, base_url, headers={"Accept": "application/json"}, **kwargs):
        req_headers = {"Authorization": f"Bearer {self.access_token}"}
        if headers:
            req_headers.update(headers)
        url = build_url(base_url, **kwargs)
        response = requests.get(url, headers=req_headers)
        if response.status_code == 401:
            print("Refreshing Access Code")
            self.refresh_access()
            return self._vendor1_get(url, headers)
        else:
            return response

    def _vendor1_post(self, url, data, headers={"Accept": "application/json"}):
        req_headers = {"Authorization": f"Bearer {self.access_token}"}
        if headers:
            req_headers.update(headers)
        response = requests.post(url, headers=req_headers, data=data)
        if response.status_code == 401:
            print("Refreshing Access Code")
            self.refresh_access()
            return self._vendor1_get(url, data, headers)
        else:
            return response

    OAUTH_REFRESH_URL = "https://oauth2.googleapis.com/token"
    REPORTTYPES_LIST = "https://vendor1reporting.googleapis.com/v1/reportTypes"
    JOBS_LIST = "https://vendor1reporting.googleapis.com/v1/jobs"
    JOBS_GET = "https://vendor1reporting.googleapis.com/v1/jobs/{jobId}"
    REPORTS_LIST = "https://vendor1reporting.googleapis.com/v1/jobs/{jobId}/reports"
    REPORTS_GET = (
        "https://vendor1reporting.googleapis.com/v1/jobs/{jobId}/reports/{reportId}"
    )
    REPORTS_QUERY = "https://vendor1analytics.googleapis.com/v2/reports"
    ITEM_SEARCH = "https://www.googleapis.com/vendor1/v3/search"

    def get_list_of_reports(self):
        response = self._vendor1_get(self.JOBS_LIST)
        jobs = json.loads(response.content).get("jobs", [])
        return jobs

    def save_report_to_s3(self, report, job_name):
        start_time = report["startTime"].split("T", 1)[0]
        end_time = report["endTime"].split("T", 1)[0]
        out_name = f"{self.channel_name}/{job_name}/{start_time}_{end_time}.csv"
        link = report["downloadUrl"]
        response = self._vendor1_get(link, headers=None)
        return save_attachment_to_s3(response.content, out_name)

    def get_reports(self, job_id, job_name, start_time, end_time):
        response = self._vendor1_get(
            self.REPORTS_LIST.format(jobId=job_id),
            startTimeAtOrAfter=start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            startTimeBefore=end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        reports = json.loads(response.content).get("reports", [])
        out = []
        for report in reports:
            response = self._vendor1_get(
                f"https://vendor1reporting.googleapis.com/v1/jobs/{job_id}/reports/{report['id']}"
            )
            r = json.loads(response.content)
            out.append(self.save_report_to_s3(r, job_name))
        return out

    def get_all_reports(self, start_time, end_time):
        jobs = self.get_list_of_reports()
        out = []
        for job in jobs:
            out += self.get_reports(
                job_id=job["id"],
                job_name=job["name"],
                start_time=start_time,
                end_time=end_time,
            )
        return out

    def get_targeted_metrics(self, item_ids, start_time, end_time):
        result = None
        start_date = start_time.strftime("%Y-%m-%d")
        end_date = end_time.strftime("%Y-%m-%d")
        max_results = 500
        for items in [item_ids[i : i + 50] for i in range(0, len(item_ids), 50)]:
            result_len = max_results
            start_index = 1
            while result_len >= max_results:
                response = self._vendor1_get(
                    self.REPORTS_QUERY,
                    startDate=start_date,
                    endDate=end_date,
                    ids=f"channel=={self.channel_id}",
                    filters=f"item=={','.join(items)}",
                    dimensions="country,item",
                    metrics=METRICS,
                    maxResults=max_results,
                    startIndex=start_index,
                )
                if response.status_code != 200:
                    print(
                        f"Received error code {response.status_code} for item_id {items}. Printing response object."
                    )
                    print(response.__dict__)
                    result_len = -1
                    continue
                try:
                    r = response.json()
                    result_len = len(r["rows"])
                    start_index += result_len
                    data = pd.DataFrame(
                        r["rows"], columns=[x["name"] for x in r["columnHeaders"]]
                    )
                except Exception:
                    print(
                        f"Failed to retrieve metrics for {len(items)} item_ids: {items}"
                    )
                    print(Exception)
                    result_len = -1
                    data = None
                if data is None:
                    continue
                result = pd.concat([result, data])
        if result is None:
            return None
        result = result.rename(columns={"item": "item_id"})
        result["start_date"] = start_date
        result["end_date"] = end_date
        result["channel_id"] = self.channel_id

        path = f"{S3_BUCKET}/{ENV}/vendor1_api/{self.channel_name}/finance_data/{start_date}_{end_date}.csv"
        result.to_csv(f"s3://{path}")
        return path


def lambda_handler(event, context):
    access_credentials = json.loads(b64decode(event["credentials"].encode("utf-8")))
    vendor1_access = AutomatedAccess(
        access_credentials, channel_name=event["channel_name"]
    )
    start_time = dt.strptime(event["start_time"], "%Y-%m-%d").date()
    end_time = dt.strptime(event["end_time"], "%Y-%m-%d").date()
    generated_reports = vendor1_access.get_all_reports(start_time, end_time)
    item_ids = get_item_ids(generated_reports, column="item_id")
    target_met = vendor1_access.get_targeted_metrics(item_ids, start_time, end_time)
    generated_reports.append(target_met)
    return list(set(x for x in generated_reports if x))
