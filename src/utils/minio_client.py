import json
import io
from io import BytesIO
import requests
from minio import Minio
from minio.error import S3Error
import datetime
import hashlib
import hmac

class MinioClient:
    """
    A client for interacting with a MinIO server.

    Supports:
    - AWS4-HMAC-SHA256 Signature Authentication
    - Uploading JSON to MinIO
    - Downloading JSON from MinIO
    - MinIO health check
    """

    def __init__(self, minio_endpoint, minio_access_key, minio_secret_key, region="us-east-1", secure=False):
        """
        Initialize MinioClient instance.

        Args:
            endpoint (str): MinIO endpoint e.g., "minio:9000"
            access_key (str): Access key for MinIO
            secret_key (str): Secret key for MinIO
            region (str, optional): AWS region. Defaults to "us-east-1".
            secure (bool, optional): Use HTTPS if True. Defaults to False.
        """
        self.endpoint = minio_endpoint
        self.access_key = minio_access_key
        self.secret_key = minio_secret_key
        self.region = region
        self.service = "s3"

        self.minio = Minio(
            endpoint = self.endpoint,
            access_key = self.access_key,
            secret_key = self.secret_key,
            secure = secure
        )

    def _generate_headers(self, method, uri):
        """
        Generate AWS4-HMAC-SHA256 signed headers for a request.

        Args:
            method (str): HTTP method (GET, PUT, etc.)
            uri (str): URI path starting with '/'

        Returns:
            dict: Signed headers with Authorization and x-amz-date
        """
        amz_date = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
        date_stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d")

        canonical_uri = uri
        canonical_headers = f'host:{self.minio_endpoint}\nx-amz-date:{amz_date}\n'
        signed_headers = 'host;x-amz-date'
        payload_hash = hashlib.sha256(''.encode('utf-8')).hexdigest()

        canonical_request = f'{method}\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}'
        algorithm = 'AWS4-HMAC-SHA256'
        credential_scope = f'{date_stamp}/{self.region}/{self.service}/aws4_request'
        string_to_sign = f'{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()}'

        def sign(key, msg):
            return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

        k_date = sign(('AWS4' + self.secret_key).encode('utf-8'), date_stamp)
        k_region = sign(k_date, self.region)
        k_service = sign(k_region, self.service)
        k_signing = sign(k_service, 'aws4_request')

        signature = hmac.new(k_signing, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

        authorization_header = (
            f'{algorithm} Credential={self.access_key}/{credential_scope}, '
            f'SignedHeaders={signed_headers}, Signature={signature}'
        )

        headers = {
            'x-amz-date': amz_date,
            'Authorization': authorization_header
        }

        return headers

    def health_check(self):
        """
        Perform MinIO health check using signed GET request.

        Prints:
            Health status of MinIO server.
        """
        url = f'http://{self.minio_endpoint}/minio/health/ready'
        headers = self._generate_headers("GET", "/minio/health/ready")

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            print("MinIO Health Check Passed")
        else:
            print(f"Health Check Failed: {response.text}")

    def upload_json(self, bucket, object_name, json_data, overwrite=False):
        """
        Upload a JSON object to MinIO.

        Args:
            bucket (str): Target MinIO bucket.
            object_name (str): Object name for the file (e.g., data.json)
            json_data (dict): JSON data to upload.
            overwrite (bool, optional): Overwrite if file exists. Defaults to False.

        Prints:
            Upload status and object details.
        """
        json_bytes = json.dumps(json_data, indent=4).encode("utf-8")
        json_stream = io.BytesIO(json_bytes)
        json_size = len(json_bytes)

        if not self.minio.bucket_exists(bucket):
            self.minio.make_bucket(bucket)

        try:
            self.minio.stat_object(bucket, object_name)
            file_exists = True
        except S3Error:
            file_exists = False

        if file_exists and not overwrite:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
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
            print(f"Uploaded {result.object_name} to bucket {bucket}; ETag: {result.etag}")
        except S3Error as e:
            print(f"MinIO Upload Error: {e}")

    def download_json(self, bucket, object_name):
        """
        Download a JSON object from MinIO.

        Args:
            bucket (str): Source MinIO bucket.
            object_name (str): Object name (e.g., data.json)

        Returns:
            dict or None: JSON data if found else None.

        Prints:
            Errors if download fails.
        """
        try:
            response = self.minio.get_object(bucket, object_name)
            json_data = json.load(BytesIO(response.read()))
            response.close()
            response.release_conn()
            return json_data
        except S3Error as e:
            print(f"Download Error: {e}")
            return None
        except json.JSONDecodeError:
            print("JSON Decode Error")
            return None
    
    def list_files(self, bucket, prefix="") -> list[str]:
        """
        List all files in a MinIO bucket.

        Args:
            bucket (str): Name of the bucket.
            prefix (str, optional): Filter files with this prefix. Defaults to "".

        Returns:
            List[str]: List of object names (file paths).
        """
        if not self.minio.bucket_exists(bucket):
            print(f"Bucket '{bucket}' does not exist.")
            return []

        try:
            objects = self.minio.list_objects(bucket, prefix=prefix, recursive=True)
            file_list = [obj.object_name for obj in objects]
            return file_list
        except S3Error as e:
            print(f"Error while listing files: {e}")
            return []

