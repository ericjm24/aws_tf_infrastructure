from prefect.run_configs import ECSRun


def standard_run_config(conf, cpu=256, memory=1024, image=None, storage=None):
    task_definition = {
        "family": "prefect-flow",
        "taskRoleArn": conf.s3.core_tf_state["prefect_task_role_arn"],
        "executionRoleArn": conf.s3.core_tf_state["prefect_execution_role_arn"],
        "cpu": cpu,
        "memory": memory,
        "networkMode": "awsvpc",
        "containerDefinitions": [
            {
                "name": "flow",
                "image": image
                or f"{conf.s3.core_tf_state['prefect_image_repo_url']}/{conf.ENVIRONMENT}:latest",
                "cpu": cpu,
                "memory": memory,
            }
        ],
        "requiresCompatibilities": ["FARGATE"],
    }
    if storage:
        task_definition.update({"ephemeralStorage": {"sizeInGiB": storage}})

    standard_run_config = ECSRun(
        task_definition=task_definition,
        labels=[],
        run_task_kwargs={
            "networkConfiguration": {
                "awsvpcConfiguration": {
                    "subnets": [conf.s3.core_tf_state["prefect_subnet_id"]],
                    "securityGroups": [
                        conf.s3.core_tf_state["prefect_security_group_id"]
                    ],
                    "assignPublicIp": "ENABLED",
                }
            }
        },
    )
    return standard_run_config
