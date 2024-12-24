import asyncio
import copy
import json
import time

import requests
import websockets
from tapflow.lib.backend_apis.metadataInstance import MetadataInstanceApi
from tapflow.lib.help_decorator import help_decorate
from tapflow.lib.op_object import show_connections, show_tables
from tapflow.lib.request import req

from tapflow.lib.data_pipeline.base_node import BaseNode, node_config, node_config_sync
from tapflow.lib.cache import client_cache, system_server_conf


@help_decorate("source is start of a pipeline", "source = Source($Datasource, $table)")
class Source(BaseNode):
    def __getattr__(self, name):
        return Source(self.connection, table=name)
    
    @classmethod
    def to_instance(cls, node_dict: dict) -> "Source":
        """
        to_dict方法的逆向操作
        :param node_dict: API 返回的节点dict
        :return: 节点实例
        """
        try:
            s = cls(
                node_dict["attrs"]["connectionName"],
                node_dict["tableName"],
            )
            s.id = node_dict["id"]
            s.setting["id"] = node_dict["id"]
            s.setting.update(node_dict)
            return s
        except KeyError as e:
            raise ValueError(f"Invalid node_dict, {e}")

    def __init__(self, connection, table=None, table_re=None):
        if table_re or isinstance(table, list) or isinstance(table, tuple) or table is None:
            mode = "migrate"
        else:
            mode = "sync"
        super().__init__(connection, table, table_re, mode=mode)
        try:
            if mode == "sync":
                meta = MetadataInstanceApi(req).get_table_metadata(self.connectionId, table)
                self.setting.update({
                    "previewQualifiedName": meta["qualifiedName"],
                    "previewTapTable": meta["tapTable"]
                })
        except Exception as e:
            pass

        self.update_node_config({})
        if self.mode == "migrate":
            self.config_type = node_config
            if table is not None:
                self.setting.update({
                    "tableNames": self.table,
                    "migrateTableSelectType": "custom",
                })
            else:
                if table_re is not None:
                    self.setting.update({
                        "tableExpression": table_re,
                        "migrateTableSelectType": "expression"
                    })
                else:
                    self.setting.update({
                        "tableExpression": ".*",
                        "migrateTableSelectType": "expression"
                    })

        else:
            self.config_type = node_config_sync
            _ = self._getTableId(table)  # to set self.primary_key, don't delete this line
            self.setting.update({
                "tableName": table,
                "name": table,
                "isFilter": False,
            })
        if str(self.databaseType).lower() == "csv":
            self.update_node_config({
                "dataStartLine": 2,
                "fileEncoding": "UTF-8",
                "headerLine": 1,
                "includeRegString": "*.csv",
                "justString": False,
                "lineEnd": "0x",
                "lineEndType": "\\n",
                "modelName": table,
                "offStandard": False,
                "quoteChar": '\\"',
                "recursive": True,
                "separator": "0x",
                "separatorType": ",",
                "enableSaveDeleteData": False,
                "fileNameExpression": "tap.csv",
                "writeFilePath": "./",
            })

    def enableDDL(self):
        self.setting.update({
            "enableDDL": True,
            "ddlConfiguration": "SYNCHRONIZATION"
        })
        return self

    def enablePreImage(self):
        self.update_node_config({
            "enableFillingModifiedData": False,
            "preImage": True,
            "skipDeletedEventsOnFilling": True,
        })
        return self

    def disable_filling_modified_data(self):
        self.update_node_config({
            "enableFillingModifiedData": False,
            "noCursorTimeout": False
        })
        return self

    def disableDDL(self):
        self.setting.update({
            "enableDDL": False
        })
        return self

    def increase_read_size(self, size: int):
        self.setting.update({
            "increaseReadSize": size
        })
        return self

    def initial_read_size(self, size: int):
        self.setting.update({
            "readBatchSize": size
        })
        return self

    def initial_hash_read_size(self, size: int):
        self.update_node_config({
            "hashSplit": True,
            "maxSplit": size,
        })
        return self

    def initial_read_threads(self, size: int):
        self.update_node_config({
            "batchReadThreadSize": size
        })
        return self
    
    def exists(self):
        if self.mode == "migrate":
            return True
        show_tables(source=self.connectionId, quiet=True)
        return self.table in client_cache["tables"][self.connectionId]["name_index"]
    
    def connection_type(self):
        return client_cache["connections"]["id_index"][self.connectionId]["connection_type"]

    def load_schema(self) -> bool:
        schema = copy.deepcopy(client_cache["connections"]["name_index"][self.name])
        schema.update({
            "disabledLoadSchema": False,
            "everLoadSchema": True,
            "heartbeatEnable": False,
            "loadSchemaField": True,
            "updateSchema": True,
        })
        
        async def load():
            if req.mode == "cloud":
                ws_uri = f"{req.server.replace('https://', 'wss://')}/tm/ws/agent?id={self.id}"
                cookies = req.cookies.get_dict()
            else:
                ws_uri = system_server_conf["ws_uri"]
                cookies = system_server_conf["cookies"]
            cookies_header = "; ".join([f"{key}={value}" for key, value in cookies.items()])
            async with websockets.connect(ws_uri, extra_headers=[("Cookie", cookies_header)]) as websocket:
                payload = {
                    "type": "testConnection",
                    "data": schema,
                }
                await websocket.send(json.dumps(payload))
                while True:
                    time.sleep(1)
                    recv = await websocket.recv()
                    loadResult = json.loads(recv)
                    if "type" not in loadResult:
                        continue
                    if loadResult["type"] != "pipe":
                        continue
                    if loadResult["data"]["type"] != "testConnectionResult":
                        continue
                    if loadResult["data"]["result"]["status"] is None:
                        continue

                    if loadResult["data"]["result"]["status"] != "ready":
                        res = False
                    else:
                        res = True

                    if loadResult["data"]["result"] is None:
                        continue

                    await websocket.close()
                    return res
                
        res = asyncio.run(load())
        if res:
            show_connections(quiet=True)
            return True
        else:
            return False
        