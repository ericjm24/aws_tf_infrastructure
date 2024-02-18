CURRENT_BRANCH := $(shell git symbolic-ref --short -q HEAD)
ENVIRONMENT ?= $(CURRENT_BRANCH)
terraform-plan:
	terraform plan -detailed-exitcode \
		-var-file=standard_inputs.tfvars

terraform-apply:
	case $(ENVIRONMENT) in \
		dev | test | prod) \
			terraform workspace select $(ENVIRONMENT) && \
				terraform apply \
				-var-file=standard_inputs.tfvars \
				;; \
		*) \
			echo "$(ENVIRONMENT) is not a valid environment. Valid environments are (dev, test, prod)" \
				;; \
		esac

terraform-autoapply:
	case $(ENVIRONMENT) in \
		dev | test | prod) \
			terraform workspace select $(ENVIRONMENT) && \
				terraform apply \
				-var-file=standard_inputs.tfvars \
				-auto-approve \
				;; \
		*) \
			echo "$(ENVIRONMENT) is not a valid environment. Valid environments are (dev, test, prod)" \
				;; \
		esac

terraform-destroy:
	terraform workspace select $(ENVIRONMENT) && terraform destroy \
		-var-file=standard_inputs.tfvars 
