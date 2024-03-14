import boto3
import logging
import os
import socket

MINIO_IP_ADDRESS = socket.gethostbyname("minio")


class JobS3ClientManager:
    def __init__(self, access_key=None, secret_key=None, endpoint_url=None):
        self.access_key = access_key or os.getenv('MINIO_ROOT_ACCESS_KEY')
        self.secret_key = secret_key or os.getenv('MINIO_ROOT_SECRET_KEY')
        self.endpoint_url = endpoint_url or f"http://{MINIO_IP_ADDRESS}:9000"

    def get_boto_client(self):
        try:
            return boto3.client(
                's3',
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                endpoint_url=self.endpoint_url
            )
        except Exception as e:
            logging.error(f"Error creating S3 client: {str(e)}")
            raise

    def list_objects(self, bucket_name, prefix=''):
        s3_client = self.get_boto_client()
        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=prefix)
            return response.get('Contents', [])
        except Exception as e:
            logging.error(
                f"Error listing objects in bucket {bucket_name} with prefix {prefix}: {str(e)}")
            raise

    def get_object(self, bucket_name, file_key):
        s3_client = self.get_boto_client()
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            return response['Body'].read()
        except Exception as e:
            logging.error(
                f"Error getting object {file_key} from bucket {bucket_name}: {str(e)}")
            raise
