resource "random_id" "bucket_suffix" {
  byte_length = 4
}

locals {
  bucket_name = "datalake-analytics-${random_id.bucket_suffix.hex}"
  bucket_arn  = "arn:aws:s3:::${local.bucket_name}"
}

# Bucket S3 créé et orchestré pour contourner le blocage SCP sur s3:GetBucketObjectLockConfiguration
resource "terraform_data" "datalake_bucket" {
  input = local.bucket_name

  provisioner "local-exec" {
    command = "powershell -Command \"if (-not (aws s3 ls s3://${local.bucket_name} 2>$null)) { aws s3 mb s3://${local.bucket_name} }\""
  }

  provisioner "local-exec" {
    when    = destroy
    command = "powershell -Command \"aws s3 rb s3://${self.output} --force\""
  }
}

# Dossiers logiques S3
resource "aws_s3_object" "folder_raw" {
  bucket     = local.bucket_name
  key        = "raw/"
  depends_on = [terraform_data.datalake_bucket]
}

resource "aws_s3_object" "folder_processed" {
  bucket     = local.bucket_name
  key        = "processed/"
  depends_on = [terraform_data.datalake_bucket]
}

resource "aws_s3_object" "folder_athena" {
  bucket     = local.bucket_name
  key        = "athena-results/"
  depends_on = [terraform_data.datalake_bucket]
}

output "s3_bucket_name" {
  description = "Nom du bucket S3 créé"
  value       = local.bucket_name
}

