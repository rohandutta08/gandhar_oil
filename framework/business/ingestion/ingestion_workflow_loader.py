import json

from framework.business.ingestion.ingestion_request import IngestionRequest
from framework.business.ingestion.ingestion_metadata import IngestionMetadata
from framework.utils.singleton import SingletonMeta

class IngestionWorkflowLoader(metaclass = SingletonMeta):
    def __init__(self,path,metadata_path):
        self.path = path
        self.metadata_path = metadata_path
        
    def load(self):
        with open(self.path,'r') as file:
            request_dict = json.load(file)
        return IngestionRequest(request_dict)  
    
    def get_ingestion_metadata(self,file_type):
        with open(self.metadata_path,'r') as file:
            metadata_dict = json.load(file)
        return IngestionMetadata(metadata_dict,file_type)       