import boto3
from botocore.client import Config

def init_minio():
    s3 = boto3.resource('s3',
                        endpoint_url='http://localhost:9000',
                        aws_access_key_id='minioadmin',
                        aws_secret_access_key='minioadmin',
                        config=Config(signature_version='s3v4'),
                        region_name='us-east-1')

    bucket_name = 'transactions'
    
    try:
        if s3.Bucket(bucket_name) not in s3.buckets.all():
            s3.create_bucket(Bucket=bucket_name)
            print(f"✅ Bucket '{bucket_name}' created successfully!")
        
        # Create MLflow bucket
        mlflow_bucket = 'mlflow'
        if s3.Bucket(mlflow_bucket) not in s3.buckets.all():
            s3.create_bucket(Bucket=mlflow_bucket)
            print(f"✅ Bucket '{mlflow_bucket}' created successfully!")
        else:
            print(f"ℹ️  Bucket '{bucket_name}' already exists.")
            
        # Verify
        print("Current Buckets:")
        for bucket in s3.buckets.all():
            print(f" - {bucket.name}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    init_minio()
