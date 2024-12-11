from typing import Tuple
from tapflow.lib.backend_apis.common import BaseBackendApi

class ApiServersApi(BaseBackendApi):

    def get_all_api_servers(self) -> list:
        params = {"order": "createAt DESC", "limit": 20, "skip": 0, "where": {}}
        res = self.req.get("/Modules", params=params)
        return res.json().get("data", {}).get("items", [])

    def unpublish(self, id: str, tablename: str) -> Tuple[dict, bool]:
        payload = {
            "id": id,
            "tablename": tablename,
            "status": "pending"
        }
        res = self.req.patch(f"/Modules", json=payload)
        if res.status_code == 200 and res.json().get("code") == "ok":
            return res.json().get("data", {}), True
        return res.json(), False
    
    def publish(self, base_path: str, db: str, table: str, fields: list) -> Tuple[dict, bool]:
        payload = {
            "apiType": "defaultApi",
            "apiVersion": "v1",
            "basePath": base_path,
            "createType": "",
            "datasource": db,
            "describtion": "",
            "name": base_path,
            "path": "/api/v1/" + base_path,
            "readConcern": "majority",
            "readPreference": "primary",
            "status": "active",
            "tablename": table,
            "fields": fields,
            "paths": [
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Create a new record",
                    "method": "POST",
                    "name": "create",
                    "path": "/api/v1/" + base_path,
                    "result": "Document",
                    "type": "preset"
                },
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Get records based on id",
                    "method": "GET",
                    "name": "findById",
                    "params": [
                        {
                            "defaultvalue": 1,
                            "description": "document id",
                            "name": "id",
                            "type": "string"
                        }
                    ],
                    "path": "/api/v1/" + base_path + "/{id}",
                    "result": "Document",
                    "type": "preset"
                },
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Update record according to id",
                    "method": "PATCH",
                    "name": "updateById",
                    "params": [
                        {
                            "defaultvalue": 1,
                            "description": "document id",
                            "name": "id",
                            "type": "string"
                        }
                    ],
                    "path": "/api/v1/" + base_path + "{id}",
                    "result": "Document",
                    "type": "preset"
                },
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Delete records based on id",
                    "method": "DELETE",
                    "name": "deleteById",
                    "params": [
                        {
                            "description": "document id",
                            "name": "id",
                            "type": "string"
                        }
                    ],
                    "path": "/api/v1/" + base_path + "{id}",
                    "type": "preset"
                },
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Get records by page",
                    "method": "GET",
                    "name": "findPage",
                    "params": [
                        {
                            "defaultvalue": 1,
                            "description": "page number",
                            "name": "page",
                            "type": "int"
                        },
                        {
                            "defaultvalue": 20,
                            "description": "max records per page",
                            "name": "limit",
                            "type": "int"
                        },
                        {
                            "description": "sort setting,Array ,format like [{'propertyName':'ASC'}]",
                            "name": "sort",
                            "type": "object"
                        },
                        {
                            "description": "search filter object,Array",
                            "name": "filter",
                            "type": "object"
                        }
                    ],
                    "path": "/api/v1/" + base_path,
                    "result": "Page<Document>",
                    "type": "preset"
                }
            ]
        }
        res = self.req.post(f"/Modules", json=payload)
        if res.status_code == 200 and res.json().get("code") == "ok":
            return res.json().get("data", {}), True
        return {}, False
    
    def publish_with_payload(self, payload: dict) -> Tuple[dict, bool]:
        res = self.req.post(f"/Modules", json=payload)
        if res.status_code == 200 and res.json().get("code") == "ok":
            return res.json().get("data", {}), True
        return {}, False

    def update_with_payload(self, payload: dict) -> Tuple[dict, bool]:
        res = self.req.patch(f"/Modules", json=payload)
        if res.status_code == 200 and res.json().get("code") == "ok":
            return res.json().get("data", {}), True
        return {}, False
    
    def activate(self, id: str, tablename: str) -> Tuple[dict, bool]:
        payload = {
            "id": id,
            "status": "active",
            "tableName": tablename,
        }
        return self.update_with_payload(payload)

    def delete_api_server(self, id: str) -> Tuple[dict, bool]:
        res = self.req.delete(f"/Modules/{id}")
        if res.status_code == 200 and res.json().get("code") == "ok":
            return res.json().get("data", {}), True
        return res.json(), False