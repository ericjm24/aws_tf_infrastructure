from prefect import Task
import boto3
from datetime import datetime
from time import sleep

glue_client = boto3.client("glue")
s3_client = boto3.client("s3")

DEFAULT_JOB_DEF = {
    "ExecutionProperty": {"MaxConcurrentRuns": 1},
    "Command": {"Name": "glueetl", "ScriptLocation": "string", "PythonVersion": "3"},
    "DefaultArguments": {},
    "NonOverridableArguments": {},
    "MaxRetries": 0,
    "Timeout": 120,
    "WorkerType": "Standard",
    "NumberOfWorkers": 10,
    "GlueVersion": "3.0",
}


def write_file_to_s3(body, bucket, key):
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    return f"s3://{bucket}/{key}"


class GlueJob:
    def __init__(self, job_name, environment, script_location):
        self.name = job_name
        self.environment = environment
        self.script_location = script_location

    def exists(self):
        jobs = glue_client.list_jobs(Tags=self.tags).get("JobNames", [])
        return bool(self.job_name in jobs)

    @property
    def job_name(self):
        return f"{self.name}_{self.environment}"

    @property
    def tags(self):
        return {"name": self.name, "environment": self.environment}

    def create_or_update_job(self, **kwargs):
        job_def = DEFAULT_JOB_DEF
        job_def.update(kwargs)
        job_def["Command"].update(ScriptLocation=self.script_location)
        job_def = {k: v for k, v in job_def.items() if v}
        if not self.exists():
            glue_client.create_job(Name=self.job_name, Tags=self.tags, **job_def)
        else:
            glue_client.update_job(JobName=self.job_name, JobUpdate=job_def)

    def run_job(self, **kwargs):
        r = glue_client.start_job_run(
            JobName=self.job_name, Arguments={f"--{k}": v for k, v in kwargs.items()}
        )
        self.job_run_id = r.get("JobRunId", None)
        return self.job_run_id

    def get_job_status(self):
        run_status = glue_client.get_job_run(
            JobName=self.job_name, RunId=self.job_run_id
        )
        return run_status.get("JobRun", {})


class GlueTask(Task):
    def __init__(self, glue_job_name=None, script_location="", **kwargs):
        self.glue_job_name = glue_job_name
        if script_location.startswith(("s3://", "s3a://", "s3n://")):
            self.s3_script_location = script_location
        else:
            self.s3_script_location = None
            with open(script_location, "rb") as f:
                self.script = f.read()
        super().__init__(**kwargs)

    def run(self, glue_job_name=None, conf=None, job_def_kwargs={}, **kwargs):
        glue_job_name = glue_job_name or self.glue_job_name
        env = conf.ENVIRONMENT
        if not self.s3_script_location:
            self.s3_script_location = write_file_to_s3(
                body=self.script,
                bucket=conf.infra_bucket,
                key=f"glue/{env}/{glue_job_name}.py",
            )
            self.logger.info(
                f"Successfully loaded python script to {self.s3_script_location}."
            )
        job = GlueJob(glue_job_name, env, self.s3_script_location)
        job.create_or_update_job(Role=conf.glue_service_role, **job_def_kwargs)
        self.logger.info(f"Created glue job {job.job_name}.")
        run_id = job.run_job(**kwargs)
        self.logger.info(f"Job created successfully with run id {run_id}")
        status = "STARTING"
        while status not in ["STOPPED", "SUCCEEDED", "FAILED", "TIMEOUT"]:
            sleep(5)
            response = job.get_job_status()
            status = response.get("JobRunState", None)
            self.logger.info(f"Job status is {status}.")
        self.logger.info(f"Glue job {job.job_name} completed with status {status}.")
        if status in ["STOPPED", "FAILED", "TIMEOUT"]:
            self.logger.info(response.get("ErrorMessage", None))
        assert status == "SUCCEEDED"
