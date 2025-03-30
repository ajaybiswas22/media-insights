import googleapiclient.discovery
from googleapiclient.errors import HttpError

class YoutubeClient:
    """
    A client for interacting with the YouTube Data API v3.
    
    Methods:
        search(part: str, query: str, max_result: int) -> dict:
            Searches YouTube for videos based on the query and returns the response.
    """
    def __init__(self, developer_key):
        """
        Initializes the YouTube client with the provided developer key.
        
        :param developer_key: API key for authenticating requests to YouTube Data API.
        """
        try:
            self.youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=developer_key)
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
