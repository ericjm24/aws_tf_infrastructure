#!/bin/bash
aws_acct="AWSACCOUNTID.dkr.ecr.us-east-1.vendor3aws.com"
repo_name="CLIENTNAME_pipelines"

docker build -t ${repo_name}/prefect:latest ../
img_id=$(docker images -q ${repo_name}/prefect:latest)

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ${aws_acct}

docker tag ${img_id} ${aws_acct}/${repo_name}/dev:latest
docker push ${aws_acct}/${repo_name}/dev:latest

docker image rm -f ${img_id}
