/*
Many thanks to Aneesh Karve from Quilt Data for the basic structure of this module.

https://blog.quiltdata.com/an-easier-way-to-build-lambda-deployment-packages-with-docker-instead-of-ec2-9050cd486ba8
*/


locals {
    layer_archive_name = "${var.function_name}_${var.environment}_layer.zip"
    lib_archive_name = "${var.function_name}_${var.environment}_lib.zip"
    home_path = abspath(path.module)
    func_path = trimsuffix(var.function_path, "/")
    b_has_reqs = fileexists("${local.func_path}/requirements.txt")
    req_suffix = local.b_has_reqs ? substr(filemd5("${local.func_path}/requirements.txt"), 0,8) : ""
}

data "archive_file" "lambda-archive" {
  type        = "zip"
  source_dir = local.func_path
  output_path = "${local.func_path}/${local.lib_archive_name}"
  excludes = concat(
      [local.lib_archive_name, "package.sh", "Dockerfile", local.layer_archive_name],
      var.extra_excludes
  )
}

resource "null_resource" "docker-provisioner" {
    triggers = {
        run_on = local.req_suffix
    }

    provisioner "local-exec" {
        working_dir = local.func_path

        command = <<EOF
        if [ -f "requirements.txt" ]; then
            cp ${local.home_path}/Dockerfile ./
            cp ${local.home_path}/package.sh ./
            chmod u+x package.sh
            docker build -t lambda/${var.function_name} .

            docker run --rm -v ${local.func_path}:/io -t \
                -e ARCHIVE_NAME=${local.layer_archive_name} \
                lambda/${var.function_name} \
                bash /io/package.sh
        fi
        EOF
    }
}

resource "aws_s3_bucket_object" "lib_archive_s3" {
    bucket = var.s3_bucket
    key = "code/${var.function_name}/${var.environment}/lib_${substr(data.archive_file.lambda-archive.output_md5, 0, 8)}.zip"
    source = data.archive_file.lambda-archive.output_path
    lifecycle {
        ignore_changes = [source]
    }
}

resource "aws_s3_bucket_object" "layer_archive_s3" {
    count = local.b_has_reqs ? 1 : 0
    bucket = var.s3_bucket
    key = "code/${var.function_name}/${var.environment}/layer_${local.req_suffix}.zip"
    source = "${local.func_path}/${local.layer_archive_name}"
    depends_on = [resource.null_resource.docker-provisioner]
    lifecycle {
        ignore_changes = [source]
    }
}


resource "aws_lambda_layer_version" "python_dependencies" {
  count = local.b_has_reqs ? 1 : 0
  s3_bucket = aws_s3_bucket_object.layer_archive_s3[0].bucket
  s3_key = aws_s3_bucket_object.layer_archive_s3[0].key
  layer_name = "${var.environment}-${var.function_name}-python-dependencies"

  compatible_runtimes = ["python3.8"]
}

resource "aws_lambda_function" "lambda-function" {
    function_name = "${var.environment}-${var.function_name}"
    role = var.execution_role_arn
    handler = var.handler
    runtime = "python3.8"
    s3_bucket = aws_s3_bucket_object.lib_archive_s3.bucket
    s3_key = aws_s3_bucket_object.lib_archive_s3.key
    layers = aws_lambda_layer_version.python_dependencies[*].arn
    timeout = 900
    memory_size = 256
    environment {
        variables = merge(var.environment_variables, {"ENVIRONMENT"=var.environment})
    }
}

resource "null_resource" "cleanup" {
    triggers = {
        run_on = local.req_suffix
    }
    provisioner "local-exec" {
        working_dir = local.func_path

        command = "rm -f ${local.layer_archive_name} Dockerfile package.sh ${local.lib_archive_name}"
    }
    depends_on = [resource.aws_lambda_function.lambda-function]
}