# 1. Compression automatique du script Python pour la fonction Lambda
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/scripts/lambda_trigger.py"
  output_path = "${path.module}/scripts/lambda_trigger.zip"
}

# 2. Définition de la fonction AWS Lambda d'orchestration
resource "aws_lambda_function" "pipeline_orchestrator" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "datalake-pipeline-orchestrator"
  role             = data.aws_iam_role.lab_role.arn
  handler          = "lambda_trigger.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 60

  environment {
    variables = {
      GLUE_JOB_NAME     = aws_glue_job.orders_etl.name
      GLUE_CRAWLER_NAME = aws_glue_crawler.processed_crawler.name
    }
  }

  depends_on = [
    aws_glue_job.orders_etl,
    aws_glue_crawler.processed_crawler
  ]
}

# 3. Permissions d'invocation pour S3 et EventBridge
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.pipeline_orchestrator.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.datalake.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.pipeline_orchestrator.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.glue_job_succeeded.arn
}

# 4. Déclencheur S3 : Dès qu'un fichier .csv arrive dans raw/, réveille Lambda
resource "aws_s3_bucket_notification" "raw_upload_trigger" {
  bucket = aws_s3_bucket.datalake.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.pipeline_orchestrator.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.allow_s3]
}

# 5. Règle EventBridge : Dès que le job Glue réussit, réveille Lambda pour lancer le Crawler
resource "aws_cloudwatch_event_rule" "glue_job_succeeded" {
  name        = "glue-job-succeeded-rule"
  description = "Capture la fin avec succès du job Glue ETL"

  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.orders_etl.name]
      state   = ["SUCCEEDED"]
    }
  })
}

resource "aws_cloudwatch_event_target" "trigger_crawler_target" {
  rule      = aws_cloudwatch_event_rule.glue_job_succeeded.name
  target_id = "TriggerCrawlerViaLambda"
  arn       = aws_lambda_function.pipeline_orchestrator.arn
}

# 6. Output
output "lambda_function_name" {
  description = "Nom de la fonction Lambda d'orchestration"
  value       = aws_lambda_function.pipeline_orchestrator.function_name
}
