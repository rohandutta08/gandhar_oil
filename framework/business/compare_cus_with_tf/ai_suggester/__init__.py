from typing import Dict

import requests
import json

from framework.utils.retryable_session import RetryableSession
from framework.utils.singleton import SingletonMeta


class AiMatcherClient(metaclass=SingletonMeta):
    def __init__(self):
        self._api_key = "bf5effc8-637e-4d02-87eb-a8b0352dd546"
        self._base_url = "https://api.clear.in/ai-support"
        self._headers = {
            'accept': 'application/json',
            'X-Clear-AI-Support-API-Key': self._api_key,
            'Content-Type': 'application/json'
        }

    def do_column_matching(self, source, destination) -> Dict[str, str]:
        session = RetryableSession()
        url = f"{self._base_url}/task/stringMatch"
        payload = json.dumps({
            "source": source,
            "destination": destination
        })

        try:
            response = session.post(url, headers=self._headers, data=payload,timeout=45)
            response.raise_for_status()
            response_data = response.json()

            # Parsing the 'text' field in the response
            text_field = response_data.get("text", "{}")
            parsed_dict = json.loads(text_field)
            string_dict = {str(key): str(value) for key, value in parsed_dict.items() if value != ""}

            return string_dict
        except requests.exceptions.HTTPError as http_err:
            raise Exception(f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            raise Exception(f"Connection error occurred: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            raise Exception(f"Timeout error occurred: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            raise Exception(f"An error occurred: {req_err}")
