from prefect.tasks.dbt.dbt import DbtShellTask
from zipfile import ZipFile
import requests
from io import BytesIO
import os
import shutil
import urllib


def make_base_url(ip):
    return f"http://{ip}/api/v4"


def get_project_id(base_url, project_name):
    response = requests.get(f"{base_url}/projects?search={project_name}")
    result = response.json()[0].get("id", None)
    return result


def get_branch_sha(base_url, project_id, branch_name):
    response = requests.get(
        f"{base_url}/projects/{project_id}/repository/branches/{urllib.parse.quote(branch_name,safe='')}"
    )
    result = response.json().get("commit", None).get("short_id", None)
    return result


def get_archive_from_name(base_url, project_name, branch_name):
    id = get_project_id(base_url, project_name)
    sha = get_branch_sha(base_url, id, branch_name)
    response = requests.get(
        f"{base_url}/projects/{id}/repository/archive.zip?sha={sha}"
    )
    return BytesIO(response.content)


class CLIENTNAMEDbtShellTaskSet(DbtShellTask):
    def __init__(
        self,
        dbt_target="CLIENTNAME_dbt",
        return_all=True,
        log_stderr=True,
        overwrite_profiles=True,
        profiles_dir="/tmp/.dbt",
        **kwargs,
    ):
        super().__init__(
            return_all=return_all,
            log_stderr=log_stderr,
            environment=dbt_target,
            profiles_dir=profiles_dir,
            overwrite_profiles=overwrite_profiles,
            **kwargs,
        )

    def run(
        self,
        command=None,
        conf=None,
        env=None,
        helper_script=None,
        dbt_kwargs={},
        dbt_branch_name=None,
    ):
        self.profile_name = conf.dbt_profile_name

        DbtKwargs = {"type": "snowflake", "threads": 1}
        DbtKwargs.update(conf.secrets.snowflake_creds.copy())
        DbtKwargs.update(dbt_kwargs)

        base_url = make_base_url(conf.s3.core_tf_state[conf.gitlab_ip_type])
        project_name = conf.dbt_repo_name
        branch_name = dbt_branch_name or conf.dbt_branch_name
        dirname = f"/tmp/{project_name}"

        shutil.rmtree(dirname, ignore_errors=True)
        os.makedirs(dirname, exist_ok=True)
        if not os.path.exists(self.profiles_dir):
            os.mkdir("/tmp/.dbt")
        cwd = os.getcwd()

        self.logger.info("Preparing to download DBT Repository")
        try:
            with ZipFile(
                get_archive_from_name(base_url, project_name, branch_name)
            ) as z:
                self.logger.info(
                    f"Repository {project_name} successfully retrieved (branch {branch_name})"
                )
                z.extractall(dirname)
                self.logger.info(f"DBT Repository extracted to {dirname}")
            if len(os.listdir(dirname)) == 1:
                arch_name = os.listdir(dirname)[0]
            else:
                arch_name = ""
            if type(command) is not list:
                command = [command]
            for c in command:
                super().run(
                    command=c,
                    env=env if env else {},
                    helper_script=f"cd {dirname}/{arch_name}\n{helper_script if helper_script else ''}",
                    dbt_kwargs=DbtKwargs,
                )
        except:
            raise
        finally:
            os.chdir(cwd)
