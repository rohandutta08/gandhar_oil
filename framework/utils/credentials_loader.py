import json

from framework.utils.credentials import APICredentials
from framework.utils.singleton import SingletonMeta


class CredentialsLoader(metaclass=SingletonMeta):
    def __init__(self, path):
        self.path = path

    def load(self):
        with open(self.path, 'r') as file:
            credentials_dict = json.load(file)
        return APICredentials(credentials_dict)