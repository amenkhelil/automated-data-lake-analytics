# Rôle IAM pré-existant du Learner Lab
data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

# Upload du script PySpark sur S3
resource "aws_s3_object" "glue_script" {
  bucket     = local.bucket_name
  key        = "scripts/glue_etl.py"
  source     = "${path.module}/scripts/glue_etl.py"
  etag       = filemd5("${path.module}/scripts/glue_etl.py")
  depends_on = [terraform_data.datalake_bucket]
}

# Définition du Job ETL AWS Glue
resource "aws_glue_job" "orders_etl" {
  name     = "orders-etl-job"
  role_arn = data.aws_iam_role.lab_role.arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  timeout           = 5

  command {
    name            = "glueetl"
    script_location = "s3://${local.bucket_name}/scripts/glue_etl.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--continuous-log-logGroup"          = "/aws-glue/jobs"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--S3_BUCKET_NAME"                   = local.bucket_name
  }

  depends_on = [
    aws_s3_object.glue_script,
    aws_s3_object.folder_raw,
    aws_s3_object.folder_processed
  ]
}

output "glue_job_name" {
  description = "Nom du Job AWS Glue"
  value       = aws_glue_job.orders_etl.name
}
