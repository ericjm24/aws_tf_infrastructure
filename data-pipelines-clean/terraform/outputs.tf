output "outlook_lambda_name" {
  value = module.outlook_connector.qualified_function_name
}

output "vendor1_lambda_name" {
  value = module.vendor1_connector.qualified_function_name
}

output "csv_to_parquet_lambda_name" {
  value = module.csv_to_parquet.qualified_function_name
}

output "xls_to_parquet_lambda_name" {
  value = module.xls_to_parquet.qualified_function_name
}

output "file_mover_lambda_name" {
  value = module.file_mover.qualified_function_name
}

output "title_to_DataSource2_id_lambda_name" {
  value = module.title_to_DataSource2_id.qualified_function_name
}