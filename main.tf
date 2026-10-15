resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# Bucket S3 principal du Data Lake
resource "aws_s3_bucket" "datalake" {
  bucket        = "datalake-analytics-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

# Désactivation des ACLs
resource "aws_s3_bucket_ownership_controls" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Blocage strict de l'accès public
resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Dossiers logiques S3
resource "aws_s3_object" "folder_raw" {
  bucket = aws_s3_bucket.datalake.id
  key    = "raw/"
}

resource "aws_s3_object" "folder_processed" {
  bucket = aws_s3_bucket.datalake.id
  key    = "processed/"
}

resource "aws_s3_object" "folder_athena" {
  bucket = aws_s3_bucket.datalake.id
  key    = "athena-results/"
}

output "s3_bucket_name" {
  description = "Nom du bucket S3 créé"
  value       = aws_s3_bucket.datalake.bucket
}
