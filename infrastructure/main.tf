# Variables
variable "environment" {
  type        = string
  description = "Environment (dev/prod)"
  validation {
    condition     = contains(["dev", "prod"], lower(var.environment))
    error_message = "Environment must be either 'dev' or 'prod'"
  }
}

# Base infrastructure
resource "aws_s3_bucket" "streamer_data" {
  bucket = "streamers-data-lake-${lower(var.environment)}"
}

resource "aws_glue_catalog_database" "streamer_db" {
  name = "weekly_stream_data_${lower(var.environment)}"
}

# Output values for Python script
output "s3_bucket_name" {
  value = aws_s3_bucket.streamer_data.id
}

output "glue_database_name" {
  value = aws_glue_catalog_database.streamer_db.name
}