from IPython.core.magic import Magics, magics_class, line_magic

from tapflow.lib.cache import client_cache
from tapflow.lib.connections.connection import get_table_fields
from tapflow.lib.request import req
from tapflow.lib.op_object import show_apis, show_connections
from tapflow.lib.utils.log import logger


@magics_class
class ApiCommand(Magics):
    @line_magic
    def unpublish(self, line):
        if len(client_cache["apis"]["name_index"]) == 0:
            show_apis()
        payload = {
            "id": client_cache["apis"]["name_index"][line]["id"],
            "tablename": client_cache["apis"]["name_index"][line]["table"],
            "status": "pending"
        }
        res = req.patch("/Modules", json=payload)
        res = res.json()
        if res["code"] == "ok":
            pass
            #logger.info("unpublish {} success", line)
        else:
            logger.warn("unpublish {} fail, err is: {}", line, res)

    @line_magic
    def publish(self, line):
        if " " not in line:
            return

        base_path = line.split(" ")[0]
        line = line.split(" ")[1]

        if client_cache.get("connections") is None and "." not in line:
            logger.warn("no DataSource set, only table is not enough")
            return
        db = client_cache.get("connection")
        table = line
        if "." in line:
            db = line.split(".")[0]
            table = line.split(".")[1]
            if client_cache.get("connections") is None:
                show_connections(quiet=True)
            if db not in client_cache["connections"]["name_index"]:
                show_connections(quiet=True)
            if db not in client_cache["connections"]["name_index"]:
                logger.warn("no Datasource {} found in system", db)
            db = client_cache["connections"]["name_index"][db]["id"]

        fields = get_table_fields(table, whole=True, source=db)
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
        res = req.post("/Modules", json=payload).json()
        if res["code"] == "ok":
            pass
            #logger.info(
            #    "publish api {} success, you can test it by: {}",
            #    base_path,
            #    "http://" + server + "#/apiDocAndTest?id=" + base_path + "_v1"
            #)
        else:
            pass
            logger.warn("publish api {} fail, err is: {}", base_path, res["message"])
