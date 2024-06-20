import json
import requests
import os
from framework.utils.retryable_session import RetryableSession
from framework.utils.singleton import SingletonMeta

class UserServiceClient(metaclass = SingletonMeta):
    def __init__(self):
        self.base_url = "https://userservice-sandbox-http.internal.cleartax.co"
        self.headers = {
            'x-api-token': '3d578eb0-0f9a-47e3-a553-020e42ef866a',
            'accept': 'application/json'
        }
    
    def get_user_id(self,auth_token):
        session = RetryableSession()
        url = f"{self.base_url}/v2/authtokens/{auth_token}/isauthenticated"
        
        try:
            response = session.get(url,headers=self.headers,timeout=45)
            response.raise_for_status()
            response_data = response.json()
            return response_data["response"]["externalId"]
        except requests.exceptions.HTTPError as http_err:
            raise Exception(f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(f"Connection error occurred: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            raise Exception(f"Timeout error occurred: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            raise Exception(f"An error occurred: {req_err}")
        
        