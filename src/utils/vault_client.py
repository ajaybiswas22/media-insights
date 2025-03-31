import requests

class VaultClient:
    def __init__(self, vault_addr: str, token: str):
        """
        Initializes the Vault client.

        :param vault_addr: URL of the Vault server (e.g., "http://vault:8200")
        :param token: Vault authentication token
        """
        self.vault_addr = vault_addr.rstrip("/")
        self.token = token
        self.headers = {
            "X-Vault-Token": self.token,
            "Content-Type": "application/json"
        }

    def get_secret(self, secret_path: str, key: str, mount_point: str = "secret") -> str:
        """
        Retrieves a specific key from a Vault KV v2 secret.

        :param secret_path: The path of the secret (e.g., "media_insights")
        :param key: The specific key to retrieve (e.g., "YOUTUBE_API_KEY")
        :param mount_point: The mount point of the KV store (default: "secret")
        :return: The value of the requested key as a string
        """
        url = f"{self.vault_addr}/v1/{mount_point}/data/{secret_path}"
        
        response = requests.get(url, headers=self.headers)

        if response.status_code == 200:
            return response.json().get("data", {}).get("data", {}).get(key, "")

        response.raise_for_status()
