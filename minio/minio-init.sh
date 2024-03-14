#!/bin/bash

# Set the MinIO server URL
MINIO_SERVER_URL="http://localhost:9000"

# Read root credentials from environment variables
ROOT_ACCESS_KEY=$MINIO_ROOT_ACCESS_KEY
ROOT_SECRET_KEY=$MINIO_ROOT_SECRET_KEY

# New user credentials
SPARK_USER=$MINIO_SPARK_ACCESS_KEY
SPARK_USER_PASSWORD=$MINIO_SPARK_SECRET_KEY

# Configure mc with the MinIO server (alias name: jobsminio)
mc alias set jobsminio $MINIO_SERVER_URL $ROOT_ACCESS_KEY $ROOT_SECRET_KEY

# Add the new user and attach readwrite policy
mc admin user add jobsminio $SPARK_USER $SPARK_USER_PASSWORD
mc admin policy attach jobsminio readwrite --user=$SPARK_USER

# Create Buckets
mc mb jobsminio/bronze
mc mb jobsminio/silver
mc mb jobsminio/$SPARK_HISTORY_LOG_DIR