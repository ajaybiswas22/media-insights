import boto3

class Boto3Client:

    def __init__(self,storage_client: object):
        
        self._s3 = boto3.client(
            's3',
            endpoint_url=f"http://{storage_client.get_endpoint()}",
            aws_access_key_id=storage_client.get_access_key(),
            aws_secret_access_key=storage_client.get_secret_key(),
        )
    
    def list_objects(self,bucket_name: str, prefix: str) -> dict:
        return self._s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    def get_object(self,bucket_name: str, key: str) -> any:
        return self._s3.get_object(Bucket=bucket_name, Key=key)
