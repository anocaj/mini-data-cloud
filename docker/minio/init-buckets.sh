#!/bin/bash

# MinIO Bucket Initialization Script for Iceberg Integration
# This script sets up the required buckets and policies for Apache Iceberg

set -e

echo "Waiting for MinIO to be ready..."
sleep 10

# Configure MinIO client
echo "Configuring MinIO client..."
/usr/bin/mc alias set myminio http://minio:9000 minioadmin minioadmin123

# Create buckets if they don't exist
echo "Creating Iceberg buckets..."

# Data bucket - stores actual table data files
if ! /usr/bin/mc ls myminio/iceberg-data > /dev/null 2>&1; then
    /usr/bin/mc mb myminio/iceberg-data
    echo "Created iceberg-data bucket"
else
    echo "iceberg-data bucket already exists"
fi

# Metadata bucket - stores table metadata and manifests
if ! /usr/bin/mc ls myminio/iceberg-metadata > /dev/null 2>&1; then
    /usr/bin/mc mb myminio/iceberg-metadata
    echo "Created iceberg-metadata bucket"
else
    echo "iceberg-metadata bucket already exists"
fi

# Statistics bucket - stores table and partition statistics
if ! /usr/bin/mc ls myminio/iceberg-stats > /dev/null 2>&1; then
    /usr/bin/mc mb myminio/iceberg-stats
    echo "Created iceberg-stats bucket"
else
    echo "iceberg-stats bucket already exists"
fi

# Logs bucket - stores transaction logs and audit trails
if ! /usr/bin/mc ls myminio/iceberg-logs > /dev/null 2>&1; then
    /usr/bin/mc mb myminio/iceberg-logs
    echo "Created iceberg-logs bucket"
else
    echo "iceberg-logs bucket already exists"
fi

# Set bucket policies for public read access (development only)
echo "Setting bucket policies..."

# Create a policy file for read-write access
cat > /tmp/iceberg-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": ["*"]
      },
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::iceberg-data",
        "arn:aws:s3:::iceberg-metadata",
        "arn:aws:s3:::iceberg-stats",
        "arn:aws:s3:::iceberg-logs"
      ]
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": ["*"]
      },
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::iceberg-data/*",
        "arn:aws:s3:::iceberg-metadata/*",
        "arn:aws:s3:::iceberg-stats/*",
        "arn:aws:s3:::iceberg-logs/*"
      ]
    }
  ]
}
EOF

# Apply policies to buckets
/usr/bin/mc anonymous set-json /tmp/iceberg-policy.json myminio/iceberg-data
/usr/bin/mc anonymous set-json /tmp/iceberg-policy.json myminio/iceberg-metadata
/usr/bin/mc anonymous set-json /tmp/iceberg-policy.json myminio/iceberg-stats
/usr/bin/mc anonymous set-json /tmp/iceberg-policy.json myminio/iceberg-logs

# Create directory structure in buckets
echo "Creating directory structure..."

# Data bucket structure
/usr/bin/mc cp /dev/null myminio/iceberg-data/nyc/.keep
/usr/bin/mc cp /dev/null myminio/iceberg-data/analytics/.keep
/usr/bin/mc cp /dev/null myminio/iceberg-data/warehouse/.keep

# Metadata bucket structure
/usr/bin/mc cp /dev/null myminio/iceberg-metadata/tables/.keep
/usr/bin/mc cp /dev/null myminio/iceberg-metadata/namespaces/.keep

# Statistics bucket structure
/usr/bin/mc cp /dev/null myminio/iceberg-stats/table-stats/.keep
/usr/bin/mc cp /dev/null myminio/iceberg-stats/column-stats/.keep
/usr/bin/mc cp /dev/null myminio/iceberg-stats/partition-stats/.keep

# Logs bucket structure
/usr/bin/mc cp /dev/null myminio/iceberg-logs/transactions/.keep
/usr/bin/mc cp /dev/null myminio/iceberg-logs/audit/.keep

echo "MinIO bucket initialization completed successfully!"

# List all buckets to verify
echo "Available buckets:"
/usr/bin/mc ls myminio/

# Clean up
rm -f /tmp/iceberg-policy.json

echo "Bucket setup complete. Exiting..."
exit 0