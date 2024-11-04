from tapflow.lib.help_decorator import help_decorate
from tapflow.lib.op_object import show_tables
from tapflow.lib.request import req

from tapflow.lib.data_pipeline.base_node import BaseNode, node_config, node_config_sync
from tapflow.lib.cache import client_cache


@help_decorate("source is start of a pipeline", "source = Source($Datasource, $table)")
class Source(BaseNode):
    def __getattr__(self, name):
        return Source(self.connection, table=name, mode="sync")

    def __init__(self, connection, table=None, table_re=None, mode=None):
        if mode is None:
            if table is not None:
                mode = "sync"
            else:
                mode = "migrate"
        super().__init__(connection, table, table_re, mode=mode)
        try:
            if mode == "sync":
                res = req.post("/MetadataInstances/metadata/v3", json={
                    self.connectionId: {
                        "metaType": "table",
                        "tableNames": [table]
                    }
                }).json()
                meta = list(res["data"].items())[0][1][0]
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
