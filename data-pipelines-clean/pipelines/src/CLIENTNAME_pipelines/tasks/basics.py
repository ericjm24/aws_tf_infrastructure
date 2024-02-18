import prefect
from prefect import task
from datetime import timedelta
import re

##################################################
## Date Tasks
@task(nout=2)
def date_range(days_back=7, date_range=1):
    start_date = prefect.context.date - timedelta(days=days_back)
    end_date = start_date + timedelta(date_range)
    return start_date, end_date


@task
def date_prior(days_back=7):
    return prefect.context.date - timedelta(days=days_back)


##################################################
## S3 Tasks
@task
def key_from_s3_path(path):
    print(f"Path: {path}")
    if path.startswith(("s3://", "s3a://", "s3n://")):
        return path.split("/", 3)[-1]
    elif path.startswith("CLIENTNAME-ds-"):
        return path.split("/", 1)[-1]
    else:
        return path


##################################################
## S3 Tasks
@task
def printf(word):
    prefect.context.logger.info(word)


##################################################
## Regex Tasks
@task
def filter_files(filters=None, files=[]):
    if not filters:
        return list(set(files))
    if type(filters) is not list:
        filters = list(filters)
    if type(files) is not list:
        files = list(files)
    return list(
        set(
            [
                x
                for x in files
                if any(re.fullmatch(y, x.rsplit("/", 1)[-1]) for y in filters)
            ]
        )
    )
