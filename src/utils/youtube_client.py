import googleapiclient.discovery
from googleapiclient.errors import HttpError
import hvac
import os
from dotenv import load_dotenv
class YoutubeClient:
    """
    A client for interacting with the YouTube Data API v3.
    
    Methods:
        search(part: str, query: str, max_result: int) -> dict:
            Searches YouTube for videos based on the query and returns the response.
    """
    def __init__(self):
        """
        Initializes the YouTube client with the provided developer key.
        
        :param developer_key: API key for authenticating requests to YouTube Data API.
        """
        load_dotenv()
        vault_addr = os.getenv("VAULT_ADDR", "http://vault:8200")
        vault_token = os.getenv("VAULT_TOKEN", "root")
        try:
            self.vault_client = hvac.Client(url=vault_addr, token=vault_token)
            secret = self.vault_client.secrets.kv.read_secret_version(path="data_ingestion")
            print(secret)
            api_key = secret["data"]["data"]["YOUTUBE_API_KEY"]
            self.youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)
        except Exception as e:
            print(f"Failed to initialize YouTube client: {e}")
            self.youtube = None

    def search(self, part: str, query: str, max_result: int = 50) -> dict:
        """
        Searches for YouTube videos using the given query.
        Iteratively retrieves more results if max_result exceeds the current page limit.
        
        :param part: The part parameter specifies which resource properties to include in the API response.
        :param query: The search query.
        :param max_result: The maximum number of results to retrieve.
        :return: A dictionary containing the API response or an error message.
        """
        if not self.youtube:
            return {"error": "YouTube client is not initialized."}
        
        results = []
        page_token = None
        
        try:
            while len(results) < max_result:
                request = self.youtube.search().list(
                    part=part,
                    maxResults=min(max_result - len(results), 50),  # YouTube API allows max 50 results per request
                    q=query,
                    pageToken=page_token,
                )
                response = request.execute()
                results.extend(response.get("items", []))
                
                if "nextPageToken" in response:
                    page_token = response["nextPageToken"]
                else:
                    break
            
            return {"items": results}
        except HttpError as e:
            return {"error": f"YouTube API request failed: {e}"}
        except Exception as e:
            return {"error": f"An unexpected error occurred: {e}"}