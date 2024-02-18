terraform-apply:
	terraform apply \
		-var-file=standard_inputs.tfvars \
		-var="aws_access_key=${AWS_ACCESS_KEY_ID}" \
		-var="aws_secret_key=${AWS_SECRET_ACCESS_KEY}" \
		-var="public_key_file=devkey-CLIENTNAME-ds.pem.pub"

terraform-destroy:
	terraform destroy \
		-var-file=standard_inputs.tfvars \
		-var="aws_access_key=${AWS_ACCESS_KEY_ID}" \
		-var="aws_secret_key=${AWS_SECRET_ACCESS_KEY}" \
		-var="public_key_file=devkey-CLIENTNAME-ds.pem.pub"