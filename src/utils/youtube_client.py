import googleapiclient.discovery
from googleapiclient.errors import HttpError
class YoutubeClient:
    """
    A client for interacting with the YouTube Data API v3.
    
    Methods:
        search(part: str, query: str, max_result: int) -> dict:
            Searches YouTube for videos based on the query and returns the response.
    """
    def __init__(self,developer_key):
        """
        Initializes the YouTube client with the provided developer key.
        
        :param developer_key: API key for authenticating requests to YouTube Data API.
        """
        try:
            self.youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=developer_key)
        except Exception as e:
            print(f"Failed to initialize YouTube client: {e}")
            self.youtube = None

    def search(self, part: str, 
               query: str, 
               region_code: str,
               published_after: str = None,
               published_before: str = None,
               media_type: str = "video",
               order: str = "relevance",
               max_result: int = 50) -> dict:
        """
        Searches for YouTube videos using the given query.
        Iteratively retrieves more results if max_result exceeds the current page limit.
        
        :param part: The part parameter specifies which resource properties to include in the API response.
        :param query: The search query.
        :param region_code: Country specific search results.
        :param published_after: published after date
        :param published_before: published before date
        :param media_type: media type ["channel","playlist","video"]
        :param order: order videos by ["date","rating","viewCount","relevance","title","videoCount"]
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
                    type=media_type,
                    regionCode=region_code,
                    maxResults=min(max_result - len(results), 50),  # YouTube API allows max 50 results per request
                    q=query,
                    publishedAfter=published_after,
                    publishedBefore=published_before,
                    order=order,
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