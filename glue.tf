# Rôle IAM pré-existant du Learner Lab
data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

# Upload du script PySpark unifié sur S3
resource "aws_s3_object" "glue_script" {
  bucket     = local.bucket_name
  key        = "scripts/active_etl.py"
  source     = "${path.module}/scripts/active_etl.py"
  etag       = filemd5("${path.module}/scripts/active_etl.py")
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
    script_location = "s3://${local.bucket_name}/scripts/active_etl.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--continuous-log-logGroup"          = "/aws-glue/jobs"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--S3_BUCKET_NAME"                   = local.bucket_name
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.warehouse=s3://${local.bucket_name}/iceberg-warehouse/"
  }

  depends_on = [
    aws_s3_object.glue_script,
    aws_s3_object.folder_raw,
    aws_s3_object.folder_processed,
    aws_s3_object.folder_quarantine,
    aws_s3_object.folder_iceberg
  ]
}

output "glue_job_name" {
  description = "Nom du Job AWS Glue"
  value       = aws_glue_job.orders_etl.name
}
