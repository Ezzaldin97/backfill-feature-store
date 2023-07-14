import yaml
import os

class Config:
    def __init__(self):
        """
        Init Method
        """
        with open(os.path.join("conf", "credentials.yaml")) as creds:
            self.credentials = yaml.safe_load(creds)
        with open(os.path.join("conf", "api.yaml")) as api:
            self.api = yaml.safe_load(api)