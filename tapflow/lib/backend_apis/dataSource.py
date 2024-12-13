import json
from typing import Tuple
from tapflow.lib.backend_apis.common import BaseBackendApi


class DataSourceApi(BaseBackendApi):

    def get_all_data_sources(self, limit: int = 20, skip: int = 0) -> Tuple[list, bool]:
        payload = {"order":"last_updated DESC","limit":limit,"noSchema":1,"skip":skip,"where":{"createType":{"$ne":"System"}}}
        res = self.req.get("/Connections", params={"filter", json.dumps(payload)})
        if res.status_code != 200 or res.json()["code"] != "ok":
            return res.json(), False
        return res.json()["data"], True
    
    def get_all_data_sources_ignore_error(self, limit: int = 20, skip: int = 0) -> list:
        data, ok = self.get_all_data_sources(limit, skip)
        if not ok:
            return []
        return data
    
    def filter_data_sources(self, query: str, limit: int = 20, skip: int = 0) -> Tuple[list, bool]:
        payload = {"order":"last_updated DESC","limit":limit,"noSchema":1,"skip":skip,"where":{"createType":{"$ne":"System"}, "name":{"like":query,"options":"i"}}}
        res = self.req.get("/Connections", params={"filter", json.dumps(payload)})
        if res.status_code != 200 or res.json()["code"] != "ok":
            return res.json(), False
        return res.json()["data"], True
    
    def filter_data_sources_ignore_error(self, query: str, limit: int = 20, skip: int = 0) -> list:
        data, ok = self.filter_data_sources(query, limit, skip)
        if not ok:
            return []
        return data
    
    def update_data_source(self, data_source: dict) -> Tuple[dict, bool]:
        data_source_id = data_source.get("id")
        if data_source_id is None:
            return None, False
        res = self.req.patch(f"/Connections/{data_source_id}", json=data_source)
        ok = res.status_code == 200 and res.json()["code"] == "ok"
        if ok:
            return res.json()["data"], True
        return res.json(), False
    
    def create_data_source(self, data_source: dict) -> Tuple[dict, bool]:
        res = self.req.post("/Connections", json=data_source)
        ok = res.status_code == 200 and res.json()["code"] == "ok"
        if ok:
            return res.json()["data"], True
        return res.json(), False
    
    def delete_data_source(self, data_source_id: str) -> Tuple[dict, bool]:
        res = self.req.delete(f"/Connections/{data_source_id}")
        ok = res.status_code == 200 and res.json()["code"] == "ok"
        if ok:
            return res.json(), True
        return res.json(), False
    
    def get_data_source(self, data_source_id: str) -> Tuple[dict, bool]:
        res = self.req.get(f"/Connections/{data_source_id}")
        ok = res.status_code == 200 and res.json()["code"] == "ok"
        if ok:
            return res.json()["data"], True
        return res.json(), False
