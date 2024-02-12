import boto3
import logging


class S3ClientManager:
    def __init__(self, acces_key, secret_key, endpoint_url):
        # self.AWS_SPARK_ACCESS_KEY = os.getenv('MINIO_SPARK_ACCESS_KEY')
        # self.AWS_SPARK_SECRET_KEY = os.getenv('MINIO_SPARK_SECRET_KEY')
        # self.MINIO_IP_ADDRESS = socket.gethostbyname("minio")
        self.acces_key = acces_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url

    def get_boto_client(self):
        try:
            return boto3.client(
                's3',
                aws_access_key_id=self.acces_key,
                aws_secret_access_key=self.secret_key,
                endpoint_url=self.endpoint_url
            )
        except Exception as e:
            logging.error(f"Error creating S3 client: {str(e)}")
            raise
