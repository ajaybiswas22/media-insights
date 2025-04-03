from utils.youtube_client import YoutubeClient
from utils.vault_client import VaultClient
from utils.minio_client import MinioClient
import os
import sys
import json
import datetime
from dotenv import load_dotenv

def convert_date_to_iso(date_string: str) -> tuple[str,str]:
    """
    Convert a date string (YYYY-MM-DD) to ISO 8601 format with the time set to start or end of the day.
    """
    try:
        date_obj = datetime.datetime.strptime(date_string, "%Y-%m-%d")
        start_of_day = date_obj.replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + "Z"
        end_of_day = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999).isoformat() + "Z"
        return start_of_day, end_of_day
    except ValueError:
        raise ValueError("Invalid date format. Please use YYYY-MM-DD format.")

def perform_search(date: str, start_time: str, end_time: str) -> str:

    vault_address = os.getenv("VAULT_ADDR", "http://vault:8200")
    vault_token = os.getenv("VAULT_TOKEN")
    hvac_client = VaultClient(vault_addr=vault_address,token=vault_token)
    YOUTUBE_API_KEY = hvac_client.get_secret("media_insights", "YOUTUBE_API_KEY")

    minio_endpoint = os.getenv("MINIO_ENDPOINT","minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY","minio")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY","minio")
    
    youtube = YoutubeClient(YOUTUBE_API_KEY)
    minio = MinioClient(minio_endpoint=minio_endpoint, minio_access_key=minio_access_key, minio_secret_key=minio_secret_key)

    try:
        with open('metadata/region.json') as f:
            f_content= f.read()
        region_json = json.loads(f_content)
    except FileNotFoundError:
        return "Error: metadata/region.json not found"
    except ValueError:
        return "Error: metadata/region.json decode failed"
    except Exception:
        return "Error: metadata/region.json"

    for region in region_json.keys():
        response = youtube.search('snippet',
                                  'news today',
                                  region_code=region,
                                  published_after=start_time,
                                  published_before=end_time,
                                  media_type="video",
                                  order="relevance",
                                  max_result=10)
        if 'error' in response.keys():
            return "Error: " + response['error']
        try:
            minio.upload_json("youtube", f"search_{region}_news_today_{date}.json", response, overwrite=True)
        except Exception:
            return "Error: upload to s3 failed"
    return "success"

if __name__ == "__main__":
    load_dotenv()
    if len(sys.argv) != 2:
        print("Error: Date missing")
        sys.exit(1)

    date_input = sys.argv[1]  # Date string in YYYY-MM-DD format
    try:
        start_time, end_time = convert_date_to_iso(date_input)
    except ValueError as e:
        print("Error: Cannot convert Date")
        sys.exit(1)
    
    result = perform_search(date_input,start_time,end_time)
    if result.startswith('Error:'):
        print(result)
        sys.exit(1)

    print(result)