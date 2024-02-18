data "aws_iam_role" "default-lambda-role" {
  name = "CLIENTNAME-ds-DefaultLambdaExecutionRole"
}

module "outlook_connector" {
    source = "./modules/lambda"
    function_path = "${path.cwd}/modules/python/outlook_connector"
    function_name = "outlook-connector"
    environment = local.current_env
    execution_role_arn = data.aws_iam_role.default-lambda-role.arn
    handler = "outlook_connector.lambda_handler"
    s3_bucket = data.terraform_remote_state.infra_core.outputs.data_infrastructure_bucket
    extra_excludes = ["o365_token.txt"]
    environment_variables = {
      S3_LANDING_BUCKET = data.terraform_remote_state.infra_core.outputs.data_landing_bucket
      OUTLOOK_CREDENTIALS_SECRET = "outlook_creds"
    }
}

module "vendor1_connector" {
    source = "./modules/lambda"
    function_path = "${path.cwd}/modules/python/vendor1_reports"
    function_name = "vendor1-reports"
    environment = local.current_env
    execution_role_arn = data.aws_iam_role.default-lambda-role.arn
    handler = "vendor1_reports.lambda_handler"
    s3_bucket = data.terraform_remote_state.infra_core.outputs.data_infrastructure_bucket
    extra_excludes = ["o365_token.txt"]
    environment_variables = {
      S3_LANDING_BUCKET = data.terraform_remote_state.infra_core.outputs.data_landing_bucket
    }
}

module "csv_to_parquet" {
    source = "./modules/lambda"
    function_path = "${path.cwd}/modules/python/csv_to_parquet"
    function_name = "csv-parquet-converter"
    environment = local.current_env
    execution_role_arn = data.aws_iam_role.default-lambda-role.arn
    handler = "csv_to_parquet.lambda_handler"
    s3_bucket = data.terraform_remote_state.infra_core.outputs.data_infrastructure_bucket
    extra_excludes = ["o365_token.txt"]
}

module "xls_to_parquet" {
    source = "./modules/lambda"
    function_path = "${path.cwd}/modules/python/xls_to_parquet"
    function_name = "xls-parquet-converter"
    environment = local.current_env
    execution_role_arn = data.aws_iam_role.default-lambda-role.arn
    handler = "xls_to_parquet.lambda_handler"
    s3_bucket = data.terraform_remote_state.infra_core.outputs.data_infrastructure_bucket
    extra_excludes = ["o365_token.txt"]
}

module "file_mover" {
    source = "./modules/lambda"
    function_path = "${path.cwd}/modules/python/file_mover"
    function_name = "file_mover"
    environment = local.current_env
    execution_role_arn = data.aws_iam_role.default-lambda-role.arn
    handler = "file_mover.lambda_handler"
    s3_bucket = data.terraform_remote_state.infra_core.outputs.data_infrastructure_bucket
    extra_excludes = ["o365_token.txt"]
}

module "title_to_DataSource2_id" {
    source = "./modules/lambda"
    function_path = "${path.cwd}/modules/python/title_to_DataSource2_id"
    function_name = "title_to_DataSource2_id"
    environment = local.current_env
    execution_role_arn = data.aws_iam_role.default-lambda-role.arn
    handler = "title_to_DataSource2_id.lambda_handler"
    s3_bucket = data.terraform_remote_state.infra_core.outputs.data_infrastructure_bucket
    extra_excludes = ["o365_token.txt"]
}

/*
This block is being added in case we want it later.
It will automatically find lambda functions and add them without needing to specify their blocks manually.
We would need to set some standards for naming conventions in order for this to work.
Remove the spaces in "* / **" below.

module "lambda_function" {
    source = "./modules/lambda"
    for_each = toset(compact([for s in fileset("modules/python/", "* / **"): split("/", s)[0]]))
    function_path = "${path.cwd}/modules/python/${each.value}"
    function_name = each.value
    execution_role_arn = data.aws_iam_role.default-lambda-role.arn
    handler = "${each.value}.lambda_handler"
    s3_bucket = "CLIENTNAME-ds-infra-core"
    extra_excludes = ["o365_token.txt"]
}

*/