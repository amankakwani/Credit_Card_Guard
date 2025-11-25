"""
Upload Kaggle Credit Card Fraud Dataset to MinIO
=================================================
This script uploads the creditcard.csv file to MinIO S3 storage.
"""

import boto3
from botocore.exceptions import ClientError

# MinIO connection settings
s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

print("📤 Uploading Kaggle dataset to MinIO...")

try:
    # Upload the CSV file to MinIO
    s3.upload_file(
        'creditcard.csv',
        'transactions',
        'creditcard.csv'
    )
    print("✅ Successfully uploaded creditcard.csv to MinIO!")
    print("📍 Location: s3a://transactions/creditcard.csv")
    
    # Verify the file
    response = s3.head_object(Bucket='transactions', Key='creditcard.csv')
    file_size = response['ContentLength']
    print(f"📊 File size: {file_size / (1024*1024):.2f} MB")
    
except FileNotFoundError:
    print("❌ Error: creditcard.csv not found in current directory!")
    print("Please ensure creditcard.csv is in the same folder as this script.")
except ClientError as e:
    print(f"❌ Error uploading to MinIO: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
