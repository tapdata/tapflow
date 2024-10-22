from tapflow.lib.help_decorator import help_decorate
from tapflow.lib.data_pipeline.nodes.source import Source
from tapflow.lib.data_pipeline.base_node import node_config_sync
from tapflow.lib.data_pipeline.job import JobType


@help_decorate("sink is end of a pipeline", "sink = Sink($Datasource, $table)")
class Sink(Source):
    def __init__(self, connection, table=None, mode=None):
        if mode is None:
            if table is not None:
                mode = JobType.sync
            else:
                mode = JobType.migrate
        super().__init__(connection, table, mode=mode)
        self.update_node_config({
            "syncIndex": False,
            "enableSaveDeleteData": False,
            "shardCollection": False,
            "hashSplit": False,
            "maxSplit": 32,
        })

        if self.mode == JobType.sync:
            self.config_type = node_config_sync
            self.config_type = node_config_sync
            _ = self._getTableId(table)  # to set self.primary_key, don't delete this line
            self.setting.update({
                "tableName": table,
                "name": table,
            })

    @classmethod
    def to_instance(cls, node_dict: dict) -> "Sink":
        """
        to_dict方法的逆向操作
        :param node_dict: API 返回的节点dict
        :return: 节点实例
        """
        try:
            s = cls(
                node_dict["attrs"]["connectionName"],
                node_dict["tableName"],
                mode=JobType.sync if node_dict["type"] == "table" else JobType.migrate
            )
            s.id = node_dict["id"]
            s.setting["id"] = node_dict["id"]
            return s
        except KeyError as e:
            raise ValueError(f"Invalid node_dict, {e}")

    def keepData(self):
        self.setting.update({
            "existDataProcessMode": "keepData"
        })
    def keep_data(self):
        self.setting.update({
            "existDataProcessMode": "keepData"
        })

    def cleanData(self):
        self.setting.update({
            "existDataProcessMode": "removeData"
        })
    def clean_data(self):
        self.setting.update({
            "existDataProcessMode": "removeData"
        })
    def clean_table(self):
        self.setting.update({
            "existDataProcessMode": "dropTable"
        })

    def set_cdc_threads(self, t=1):
        if t > 1:
            self.setting.update({
                "cdcConcurrent": True
            })
        else:
            self.setting.update({
                "cdcConcurrent": False
            })


        self.setting.update({
            "cdcConcurrentWriteNum": t,
        })
        return self
    def set_init_thread(self, t):
        if t > 1:
            self.setting.update({
                "initialConcurrent": True
            })
        else:
            self.setting.update({
                "initialConcurrent": False
            })
        self.setting.update({
            "initialConcurrentWriteNum": t,
        })
        return self

    def set_write_batch(self, batch=500):
        self.setting.update({
            "writeBatchSize": batch
        })
        return self

    def set_write_wait(self, t=500):
        self.setting.update({
            "writeBatchWaitMs": t
        })
        return self

