import copy

from tapflow.lib.request import req
from tapflow.lib.cache import client_cache
from tapflow.lib.utils.log import logger
from tapflow.lib.backend_apis.apiServers import ApiServersApi


class Api:
    def __init__(self, id=None, name=None, table=None):
        if id is not None and name is None:
            name = id
        self.id = None
        self.name = name

        if name is None:
            return
        else:
            if self.get(name):
                table = f"{self.db}.{self.tablename}"

        if table is None:
            return

        base_path = name

        client_cache["apis"]["name_index"] = {}

        db = table.split(".")[0]
        table2 = table.split(".")[1]
        from tapflow.lib.op_object import show_connections
        if client_cache.get("connections") is None:
            show_connections(quiet=True)
        if db not in client_cache["connections"]["name_index"]:
            show_connections(quiet=True)
        if db not in client_cache["connections"]["name_index"]:
            logger.fwarn("no Datasource {} found in system", db)
            return
        db = client_cache["connections"]["name_index"][db]

        from tapflow.lib.connections.connection import get_table_fields
        fields = get_table_fields(table2, whole=True, source=db["id"])
        for index, field in enumerate(fields):
            field["comment"] = ""
            fields[index] = field
        self.base_path = base_path
        self.tablename = table2
        self.payload = {
            "apiType": "defaultApi",
            "apiVersion": "",
            "basePath": base_path,
            "connectionId": db["id"],
            "connectionName": db["name"],
            "connectionType": db["database_type"],
            "datasource": db["id"],
            "fields": fields,
            "listtags": [],
            "name": name,
            "operationType": "GET",
            "prefix": "",
            "readConcern": "",
            "readPreference": "",
            "readPreferenceTag": "",
            "tableName": table2,
            "tablename": table2,
            "status": "generating",
            "paths": [{
                "acl": [
                    "admin"
                ],
                "fields": fields,
                "description": "Get records by page",
                "method": "POST",
                "name": "findPage",
                "params": [
                    {
                        "defaultvalue": 1,
                        "description": "page number",
                        "name": "page",
                        "type": "number",
                        "require": True,
                    },
                    {
                        "defaultvalue": 20,
                        "description": "max records per page",
                        "name": "limit",
                        "type": "number",
                        "require": True,
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
                "path": f"/api/{base_path}",
                "result": "Page<Document>",
                "type": "preset",
                "sort": [],
                "where": [],
            }]
        }

    def publish(self):
        if self.id is None:
            data, ok = ApiServersApi(req).publish_with_payload(self.payload)
            id = data["id"]
            payload = copy.deepcopy(self.payload)
            payload.update({
                "id": data["id"],
                "status": "pending"
            })
            data, ok = ApiServersApi(req).update_with_payload(payload)
            data, ok = ApiServersApi(req).activate(data["id"], data["tableName"])
            if ok:
                #logger.finfo("publish api {} success, you can test it by: {}", self.base_path,
                #            "http://" + server + "#/apiDocAndTest?id=" + self.base_path + "_v1")
                self.id = data["id"]
            else:
                logger.fwarn("publish api {} fail, err is: {}", self.base_path, data["message"])
        else:
            payload = {
                "id": self.id,
                "status": "active",
                "tableName": self.tablename,
            }
            data, ok = ApiServersApi(req).update_with_payload(payload)
            if ok:
                pass
                #logger.finfo("publish {} success", self.name)
            else:
                logger.fwarn("publish {} fail, err is: {}", self.name, data["message"])

    def get(self, name):
        if len(client_cache.get("apis", {}).get("name_index")) == 0:
            from tapflow.lib.op_object import show_apis
            show_apis(quiet=True)
        api = client_cache["apis"]["name_index"].get(name)
        if api is None:
            return False
        api_id = api["id"]
        self.id = api_id
        self.db = api["database"]
        self.tablename = api["tableName"]
        return True

    def status(self, name):
        data, ok = ApiServersApi(req).get_all_api_servers()
        for i in data:
            if i["name"] == name:
                return i["status"]
        return None

    def unpublish(self):
        if self.id is None:
            return
        data, ok = ApiServersApi(req).unpublish(self.id, self.tablename)
        if ok:
            pass
            #logger.finfo("unpublish {} success", self.id)
        else:
            logger.fwarn("unpublish {} fail, err is: {}", self.id, data["message"])

    def delete(self):
        if self.id is None:
            logger.fwarn("delete api {} fail, err is: {}", self.name, "api not find")
            return
        data, ok = ApiServersApi(req).delete_api_server(self.id)
        if ok:
            pass
            #logger.finfo("delete api {} success", self.name)
        else:
            logger.fwarn("delete api {} fail, err is: {}", self.name, data["message"])
