import json

from framework.utils.workflow_request import WorkflowRequest
from framework.utils.singleton import SingletonMeta


class ReconWorkflowLoader(metaclass=SingletonMeta):
    def __init__(self,path):
        self.path = path
    
    def load(self):
        with open(self.path,'r') as file:
            request_dict = json.load(file)    
        return WorkflowRequest(request_dict) 
    
    def get_recon_request(self,request):
        return {
            "matchingRequestType": request.matchingRequestType,
            "panNodeId": request.panNodeId,
            "pullReturnPeriodStart": request.returnPeriodStart,
            "pullReturnPeriodEnd": request.returnPeriodEnd,
            "reconReturnPeriodStart": request.returnPeriodStart,
            "reconReturnPeriodEnd": request.returnPeriodEnd,
            "workspaceId": request.workspaceId,
            "disableDataPosting": True
        } 