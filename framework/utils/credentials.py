class APICredentials:
    def __init__(self, credentials_dict):
        self._credentials = credentials_dict

    @property
    def workspace_id(self):
        return self._credentials.get("workspace_id")

    @property
    def auth_token(self):
        return self._credentials.get("auth_token")

    @property
    def host(self):
        return self._credentials.get("host")