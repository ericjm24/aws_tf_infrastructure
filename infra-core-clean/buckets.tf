resource "aws_s3_bucket" "data_landing" {
    bucket = "${var.bucket_prefix}-data-landing"
    versioning {
    enabled = false
   }
   server_side_encryption_configuration {
     rule {
         apply_server_side_encryption_by_default {
             sse_algorithm = "AES256"
         }
     }
   }
  tags = local.default_tags
}

resource "aws_s3_bucket" "data_staging" {
    bucket = "${var.bucket_prefix}-data-staging"
    versioning {
    enabled = true
   }
   lifecycle_rule {
     id = "change_storage_class"
     enabled = true
     transition {
         days = 30
         storage_class = "STANDARD_IA"
     }
     transition {
         days = 90
         storage_class = "GLACIER_IR"
     }
   }
   server_side_encryption_configuration {
     rule {
         apply_server_side_encryption_by_default {
             sse_algorithm = "AES256"
         }
     }
   }
  tags = local.default_tags
}

resource "aws_s3_bucket" "data_archive" {
    bucket = "${var.bucket_prefix}-data-archive"
    versioning {
    enabled = true
   }
   lifecycle_rule {
     id = "change_storage_class"
     enabled = true
     transition {
         days = 0
         storage_class = "GLACIER_IR"
     }
     transition {
         days = 90
         storage_class = "DEEP_ARCHIVE"
     }
   }
   server_side_encryption_configuration {
     rule {
         apply_server_side_encryption_by_default {
             sse_algorithm = "AES256"
         }
     }
   }
  tags = local.default_tags
}

resource "aws_s3_bucket" "data_logging" {
    bucket = "${var.bucket_prefix}-data-logging"
    versioning {
    enabled = false
   }
   lifecycle_rule {
     id = "change_storage_class"
     enabled = true
     transition {
         days = 30
         storage_class = "STANDARD_IA"
     }
     transition {
         days = 90
         storage_class = "GLACIER_IR"
     }
   }
   server_side_encryption_configuration {
     rule {
         apply_server_side_encryption_by_default {
             sse_algorithm = "AES256"
         }
     }
   }
  tags = local.default_tags
}

resource "aws_s3_bucket" "data_infrastructure" {
    bucket = "${var.bucket_prefix}-data-infrastructure"
    versioning {
    enabled = false
   }
   server_side_encryption_configuration {
     rule {
         apply_server_side_encryption_by_default {
             sse_algorithm = "AES256"
         }
     }
   }
  tags = local.default_tags
}
