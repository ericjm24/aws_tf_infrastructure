variable function_path {
    type = string
}

variable function_name {
    type = string
}

variable execution_role_arn {
    type = string
}

variable handler {
    type = string
}

variable s3_bucket {
    type = string
}

variable extra_excludes {
    type = list(string)
    default = []
}

variable environment {
    type = string
    default = "dev"
}

variable environment_variables {
    type = map(string)
    default = {}
}