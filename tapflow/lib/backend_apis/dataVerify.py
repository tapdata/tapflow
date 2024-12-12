import json
from typing import Tuple
from tapflow.lib.backend_apis.common import BaseBackendApi

class DataVerifyApi(BaseBackendApi):
    
    def create_data_verify(self, data_verify: dict) -> Tuple[dict, bool]:
        res = self.req.post("/Inspects", json=data_verify)
        if res.status_code == 200 and res.json().get("code") == "ok":
            return res.json(), True
        return res.json(), False
    
    def update_data_verify(self, data_verify_id: str, status: str) -> Tuple[dict, bool]:
        res = self.req.put("/Inspects/update", params={"where": json.dumps({"id": data_verify_id})}, json={"status": status})
        if res.status_code == 200 and res.json().get("code") == "ok":
            return res.json(), True
        return res.json(), False
    
    def get_data_verify_results(self, data_verify_id: str) -> Tuple[dict, bool]:
        res = self.req.get("/InspectResults", params={"filter": json.dumps({"where": {"inspect_id":data_verify_id}})})
        if res.status_code == 200 and res.json().get("code") == "ok":
            return res.json(), True
        return res.json(), False
    
    def delete_data_verify(self, data_verify_id: str) -> Tuple[dict, bool]:
        res = self.req.delete("/Inspects/" + data_verify_id)
        if res.status_code == 200 and res.json().get("code") == "ok":
            return res.json(), True
        return res.json(), False
