import json
import io
from io import BytesIO
from minio import Minio
from minio.error import S3Error
from datetime import datetime

class MinioClient:
    """
    A wrapper class for interacting with MinIO storage.

    Provides methods to upload JSON data directly to MinIO.
    """

    def __init__(self,minio_endpoint,minio_access_key,minio_secret_key, secure: bool = False):
        """
        Initialize MinIO client with credentials fetched from Vault.
        """

        try:

            # Initialize MinIO client
            self.minio = Minio(
                endpoint=minio_endpoint,
                access_key=minio_access_key,
                secret_key=minio_secret_key,
                secure=secure
            )
        except Exception as e:
            raise Exception(f"Error connecting to Vault or MinIO: {str(e)}")

    def upload_json(self, bucket: str, object_name: str, json_data: dict, overwrite: bool = False) -> None:
        """
        Uploads a JSON object to the specified MinIO bucket.
        If overwrite=False and the file exists, appends a timestamp to create a duplicate.

        Args:
            bucket (str): The name of the MinIO bucket.
            object_name (str): The name to assign to the uploaded object.
            json_data (dict): The JSON data to upload.
            overwrite (bool): Whether to overwrite an existing file. Defaults to False.

        Raises:
            S3Error: If there is an issue uploading the file.
        """
        # Convert JSON object to a byte stream
        json_bytes = json.dumps(json_data, indent=4).encode("utf-8")
        json_stream = io.BytesIO(json_bytes)
        json_size = len(json_bytes)

        # Ensure the bucket exists before uploading
        if not self.minio.bucket_exists(bucket):
            self.minio.make_bucket(bucket)

        # Check if the file already exists
        try:
            self.minio.stat_object(bucket, object_name)
            file_exists = True
        except S3Error:
            file_exists = False

        # Handle file naming based on overwrite flag
        if file_exists and not overwrite:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Format: YYYYMMDD_HHMMSS
            name, ext = object_name.rsplit('.', 1)
            object_name = f"{name}_{timestamp}.{ext}"

        try:
            result = self.minio.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=json_stream,
                length=json_size,
                content_type="application/json"
            )

            print(
                f"Uploaded {result.object_name} to bucket {bucket}; "
                f"ETag: {result.etag}, Version ID: {result.version_id}"
            )

        except S3Error as err:
            print(f"MinIO Upload Error: {err}")

    def download_json(self, bucket_name, object_name):
        """
        Download a JSON file from MinIO and return its content as a dictionary.

        :param bucket_name: Name of the MinIO bucket
        :param object_name: Name of the JSON file in MinIO
        :return: Parsed JSON as a dictionary, or None if an error occurs
        """
        try:
            response = self.minio.get_object(bucket_name, object_name)
            json_data = json.load(BytesIO(response.read()))
            response.close()
            response.release_conn()
            return json_data
        except S3Error as e:
            print(f"Error downloading JSON file: {e}")
            return None
        except json.JSONDecodeError:
            print("Error decoding JSON file")
            return None

        
