# Base de données dans AWS Glue Data Catalog
resource "aws_glue_catalog_database" "datalake_db" {
  name        = "analytics_datalake_db"
  description = "Base de données Glue Catalog pour les données nettoyées"
}

# Glue Crawler pour découvrir le schéma Parquet
resource "aws_glue_crawler" "processed_crawler" {
  database_name = aws_glue_catalog_database.datalake_db.name
  name          = "orders-processed-crawler"
  role          = data.aws_iam_role.lab_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.datalake.bucket}/processed/"
  }

  table_prefix = "clean_"

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "LOG"
  }

  depends_on = [
    aws_glue_catalog_database.datalake_db,
    aws_s3_object.folder_processed
  ]
}

# Workgroup Amazon Athena
resource "aws_athena_workgroup" "analytics_workgroup" {
  name        = "analytics_workgroup"
  description = "Workgroup Athena pour les requêtes du Data Lake"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.datalake.bucket}/athena-results/"
    }
  }

  depends_on = [aws_s3_object.folder_athena]
}

# Requêtes SQL types
resource "aws_athena_named_query" "kpi_overview" {
  name        = "01_kpi_overview"
  workgroup   = aws_athena_workgroup.analytics_workgroup.id
  database    = aws_glue_catalog_database.datalake_db.name
  description = "KPIs généraux : nombre de commandes, CA total et panier moyen"
  query       = <<-EOT
    SELECT 
        COUNT(*) AS total_commandes,
        ROUND(SUM(montant_total), 2) AS chiffre_affaires_global,
        ROUND(AVG(montant_total), 2) AS panier_moyen
    FROM "analytics_datalake_db"."clean_processed";
  EOT
}

resource "aws_athena_named_query" "sales_by_category" {
  name        = "02_sales_by_category"
  workgroup   = aws_athena_workgroup.analytics_workgroup.id
  database    = aws_glue_catalog_database.datalake_db.name
  description = "Analyse du chiffre d'affaires par catégorie"
  query       = <<-EOT
    SELECT 
        categorie,
        COUNT(*) AS nombre_transactions,
        ROUND(SUM(montant_total), 2) AS ca_categorie
    FROM "analytics_datalake_db"."clean_processed"
    GROUP BY categorie
    ORDER BY ca_categorie DESC;
  EOT
}

output "glue_crawler_name" {
  description = "Nom du Crawler AWS Glue"
  value       = aws_glue_crawler.processed_crawler.name
}

output "athena_workgroup_name" {
  description = "Nom du Workgroup Athena"
  value       = aws_athena_workgroup.analytics_workgroup.name
}
