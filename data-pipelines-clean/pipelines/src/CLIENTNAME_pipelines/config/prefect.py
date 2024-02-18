from ._base_config import SwitchedData

project_name = "CLIENTNAME_pipelines_${ENVIRONMENT}"
gitlab_ip_type = SwitchedData(
    switch="HOST",
    default="gitlab_public_ip",
    local="gitlab_public_ip",
    aws="gitlab_private_ip",
)
