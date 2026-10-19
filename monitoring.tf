# 1. Topic Amazon SNS pour diffuser les alertes d'incident du Data Lake
resource "aws_sns_topic" "pipeline_alerts" {
  name = "datalake-pipeline-alerts"
}

# Autoriser EventBridge et CloudWatch à publier des messages sur le topic SNS
resource "aws_sns_topic_policy" "alerts_policy" {
  arn = aws_sns_topic.pipeline_alerts.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowServicesToPublish"
        Effect = "Allow"
        Principal = {
          Service = [
            "events.amazonaws.com",
            "cloudwatch.amazonaws.com"
          ]
        }
        Action   = "sns:Publish"
        Resource = aws_sns_topic.pipeline_alerts.arn
      }
    ]
  })
}

# 2. Alarme CloudWatch sur les erreurs de la fonction Lambda
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "datalake-lambda-errors-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Déclenchée immédiatement si la fonction Lambda rencontre au moins 1 erreur"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    FunctionName = aws_lambda_function.pipeline_orchestrator.function_name
  }
}

# 3. Règle EventBridge pour capturer les échecs du Job Glue ETL
resource "aws_cloudwatch_event_rule" "glue_job_failed" {
  name        = "glue-job-failed-rule"
  description = "Capture les échecs et timeouts du job Glue ETL"

  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.orders_etl.name]
      state   = ["FAILED", "TIMEOUT", "ERROR"]
    }
  })
}

# Envoi immédiat de l'alerte sur le topic SNS en cas d'échec du job Glue
resource "aws_cloudwatch_event_target" "glue_failed_to_sns" {
  rule      = aws_cloudwatch_event_rule.glue_job_failed.name
  target_id = "SendFailureAlertToSNS"
  arn       = aws_sns_topic.pipeline_alerts.arn
}

# 4. Outputs
output "sns_topic_arn" {
  description = "ARN du topic SNS pour la réception des alertes"
  value       = aws_sns_topic.pipeline_alerts.arn
}

output "lambda_alarm_name" {
  description = "Nom de l'alarme CloudWatch pour la fonction Lambda"
  value       = aws_cloudwatch_metric_alarm.lambda_errors.alarm_name
}
