from framework.utils.singleton import SingletonMeta

class IngestionMetadata(metaclass = SingletonMeta):
    def __init__(self,request_dict,file_type):
        self._request = request_dict
        self.file_type = file_type
        
    
    @property        
    def baseUrl(self):
         return self._request.get("baseUrl")  
    
    @property 
    def mode(self):
        return self._request[self.file_type]['mode']
    
    @property
    def tenant(self):
        return self._request[self.file_type]['tenant']
    
    @property
    def customTemplateId(self):
        return self._request[self.file_type]['customTemplateId']
      