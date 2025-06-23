import json
import os

from prefect.blocks.system import Secret


class Config:
    def __get_secret(self, key: str, default=None):
        """
        Attempting to load a secret from the Prefect Secret Store raises an error
        if the secret does not exist. This function will return None if the secret
        does not exist.
        """

        try:
            return Secret.load(key).get()
        except Exception:
            return default

    def __init__(self, root_path: str):
        self.snowflake_username = None
        self.snowflake_account = None
        self.rsa_passphrase = None
        self.paychex_client_id: str = None
        self.paychex_client_secret: str = None

        config_location = os.path.join(root_path, "creds.json")
        self.rsa_location = os.path.join(root_path, "keys", "rsa_key.pem")

        if os.path.exists(config_location):
            connect = json.loads(open(f"{config_location}").read())
            if connect["secrets"].get("snowflake"):
                self.snowflake_username = connect["secrets"]["snowflake"]["username"]
                self.snowflake_account = connect["secrets"]["snowflake"]["account"]

            if connect["secrets"].get("rsa"):
                self.rsa_passphrase = connect["secrets"]["rsa"]["passphrase"]

            if connect["secrets"].get("paychex"):
                self.paychex_client_id = connect["secrets"]["paychex"]["client_id"]
                self.paychex_client_secret = connect["secrets"]["paychex"]["client_secret"]

        # override with env vars, secrets or default
        self.snowflake_username = (
            os.getenv("SNOWFLAKE_USERNAME")
            or self.snowflake_username
            or self.__get_secret("snowflake-username", "kcbigring")
        )

        self.snowflake_account = (
            os.getenv("SNOWFLAKE_ACCOUNT")
            or self.snowflake_account
            or self.__get_secret("snowflake-account", "AYB56057")
        )

        self.rsa_passphrase = os.getenv("RSA_PASSPHRASE") or self.rsa_passphrase or self.__get_secret("rsa-passphrase")

        self.paychex_client_id = (
            os.getenv("PAYCHEX_CLIENT_ID") or self.paychex_client_id or self.__get_secret("paychex-client-id")
        )

        self.paychex_client_secret = (
            os.getenv("PAYCHEX_CLIENT_SECRET")
            or self.paychex_client_secret
            or self.__get_secret("paychex-client-secret")
        )

    def rsa_private_key(self):
        self.rsa_private_key = ""
        if os.path.exists(self.rsa_location):
            with open(self.rsa_location, "r") as file:
                self.rsa_private_key = bytes(file.read(), encoding="utf-8")

        return os.getenv("RSA_PRIVATE_KEY") or self.rsa_private_key or self.__get_secret("rsa-private-key")
