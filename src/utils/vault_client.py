import hvac
import os
from dotenv import load_dotenv
class VaultClient:
    """
    A client for interacting with HashiCorp Vault using HVAC.
    
    This class handles authentication using a Vault token and allows secure retrieval of secrets.
    It supports self-signed certificates for secure connections.
    """

    def __init__(self):
        """
        Initializes the Vault client with the required configuration.

        Environment Variables:
        - VAULT_ADDR: The Vault server address (default: "https://vault:8200").
        - VAULT_TOKEN: The authentication token for Vault.
        - VAULT_CERT_PATH: Path to the self-signed certificate for SSL verification.

        Raises:
        - Exception: If authentication with Vault fails.
        """
        load_dotenv()
        self.vault_addr = os.getenv("VAULT_ADDR", "https://vault:8200")
        self.vault_token = os.getenv("VAULT_TOKEN")

        # Initialize Vault client with SSL verification
        self.client = hvac.Client(
            url=self.vault_addr,
            token=self.vault_token
        )

        # Check authentication status
        if not self.client.is_authenticated():
            raise Exception("Failed to authenticate with Vault")

    def read_secret(self, path: str, key: str):
        """
        Retrieve a secret from Vault.
        :param path: Vault path (e.g., "secret/youtube_api")
        :param key: Secret key to fetch (e.g., "api_key")
        :return: Secret value or None if not found
        """
        response = self.client.secrets.kv.read_secret_version(path=path)
        return response["data"]["data"].get(key) if response and "data" in response else None

# Test the Vault connection
if __name__ == "__main__":
    try:
        vault_client = VaultClient()
        print("Successfully connected to Vault!")
    except Exception as e:
        print(f"Vault connection failed: {e}")
