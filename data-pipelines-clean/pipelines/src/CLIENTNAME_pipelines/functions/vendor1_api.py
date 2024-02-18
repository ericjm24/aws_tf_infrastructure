import requests
import json
import urllib.parse


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


class AccessTokenExpiredException(Exception):
    pass


class Vendor1ApiAccess(object):
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
