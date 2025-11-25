import boto3
from botocore.client import Config

def check_minio():
    s3 = boto3.resource('s3',
                        endpoint_url='http://localhost:9000',
                        aws_access_key_id='minioadmin',
                        aws_secret_access_key='minioadmin',
                        config=Config(signature_version='s3v4'),
                        region_name='us-east-1')

    bucket = s3.Bucket('transactions')
    print(f"Checking bucket '{bucket.name}'...")
    
    count = 0
    print("\n--- Files in Data Lake ---")
    for obj in bucket.objects.filter(Prefix='data/'):
        print(f" 📄 {obj.key} ({obj.size} bytes)")
        count += 1
        if count >= 10:
            print("... (and more)")
            break
            
    if count == 0:
        print("\n⚠️  Bucket is empty! Data is not being saved.")
    else:
        print(f"\n✅ SUCCESS! Found data files in MinIO.")
        print("The Lambda Architecture is complete: Real-time + Batch Storage.")

if __name__ == "__main__":
    check_minio()
