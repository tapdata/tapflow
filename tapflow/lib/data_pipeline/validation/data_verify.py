import json
import uuid
import time
import urllib

from tapflow.lib.backend_apis.dataVerify import DataVerifyApi
from tapflow.lib.backend_apis.metadataInstance import MetadataInstanceApi
from tapflow.lib.utils.log import logger
from tapflow.lib.request import req
from tapflow.lib.cache import system_server_conf


def get_table_pk(connection_id, table_name):
    table_id = MetadataInstanceApi(req).get_table_id(table_name, connection_id)
    primary_key = []
    if table_id is not None:
        fields = MetadataInstanceApi(req).get_fields_value(table_id)
        for field in fields:
            if field.get("primaryKey", False):
                primary_key.append(field["field_name"])
    return primary_key


class DataVerify:
    def __init__(self, pipeline, mode="row_count"):
        self.pipeline = pipeline
        self.mode = mode
        #self.flow_id = str(bson.ObjectId())
        self.flow_id = pipeline.job.id

    def save(self):
        verify_job = {
            "flowId": self.flow_id,
            "lastUpdBy": system_server_conf["user_id"],
            "createUser": "admin@admin.com",
            "agentTags": [
                "private"
            ],
            "scheduleTimes": 0,
            "name": self.pipeline.name + " - 校验",
            "mode": "manual",
            "inspectMethod": self.mode,
            "inspectDifferenceMode": "All",
            "platformInfo": {
                "agentType": "private"
            },
            "limit": {
                "keep": 100
            },
            "enabled": True,
            "status": "waiting",
            "lastStartTime": 0,
            "byFirstCheckId": "",
            "browserTimezoneOffset": 0,
            "cdcDuration": 0,
            "alarmSettings": [
                {
                    "type": "INSPECT",
                    "open": True,
                    "key": "INSPECT_TASK_ERROR",
                    "sort": 0,
                    "notify": [
                        "SYSTEM",
                        "EMAIL"
                    ],
                    "interval": 0
                },
                {
                    "type": "INSPECT",
                    "open": True,
                    "key": "INSPECT_COUNT_ERROR",
                    "sort": 0,
                    "notify": [
                        "SYSTEM",
                        "EMAIL"
                    ],
                    "interval": 0,
                    "params": {
                        "maxDifferentialRows": 0
                    }
                }
            ],
            "user_id": system_server_conf["user_id"],
        }
        source_node = None
        sink_node = None
        nodes = self.pipeline.job.job["dag"]["nodes"]
        if len(nodes) != 2:
            logger.fwarn("{}", "data verify nodes length must be 2")
            return
        for node in nodes:
            if "tableNames" in node:
                source_node = node
            else:
                sink_node = node

        tasks = []
        for table in source_node["tableNames"]:
            if get_table_pk(source_node["connectionId"], table) == []:
                logger.fwarn("table {} has no primary key, skip data verify", table)
                continue
            t = {
                "taskId": str(uuid.uuid4()).replace("-", ""),
                "fullMatch": True,
                "script": "",
                "showAdvancedVerification": False,
                "source": {
                    "fields": [],
                    "connectionId": source_node["connectionId"],
                    "connectionName": source_node["name"],
                    "sortColumn": ",".join(get_table_pk(source_node["connectionId"], table)),
                    "table": table,
                    "nodeId": source_node["id"],
                    "nodeName": source_node["name"],
                    "databaseType": source_node["databaseType"],
                    "isFilter": False,
                    "conditions": [],
                    "enableCustomCommand": False,
                },
                "target": {
                    "fields": [],
                    "connectionId": sink_node["connectionId"],
                    "connectionName": sink_node["name"],
                    "sortColumn": ",".join(get_table_pk(sink_node["connectionId"], table)),
                    "table": table,
                    "nodeId": sink_node["id"],
                    "nodeName": sink_node["name"],
                    "databaseType": sink_node["databaseType"],
                    "isFilter": False,
                    "conditions": [],
                    "enableCustomCommand": False
                },
                "webScript": ""
            }
            tasks.append(t)
        verify_job["tasks"] = tasks
        res, ok = DataVerifyApi(req).create_data_verify(verify_job)
        if ok:
            self.id = res["data"]["id"]
            return True
        else:
            logger.fwarn("create data verify task failed")
            print(res)
        return False


    def start(self):
        _, ok = DataVerifyApi(req).update_data_verify(self.id, "scheduling")
        return ok


    def wait_finish(self, t=100):
        start_time = time.time()
        # wait for starting
        while True:
            if time.time() - start_time > t:
                logger.error("validation task start running timeout")
                return False
            last_result = self.last_result()
            if last_result is None:
                continue
            if last_result["status"] == "running":
                logger.finfo("validation task start running")
                break
            time.sleep(1)
        # wait for done
        while True:
            if time.time() - start_time > t:
                logger.error("validation task execution timeout")
                return False
            time.sleep(1)
            last_result = self.last_result()
            if last_result["status"] == "done":
                return True

    def status(self):
        last_result = self.last_result()
        if last_result is None:
            return None
        return last_result["status"]

    def result(self):
        res, ok = DataVerifyApi(req).get_data_verify_results(self.id)
        if ok:
            return res["data"]["items"]
        return []

    def last_result(self):
        results = self.result()
        if len(results) == 0:
            return None
        return results[0]

    def is_pass(self):
        return self.last_pass()

    def last_pass(self):
        self.wait_finish()
        last_result = self.last_result()
        if last_result is None:
            return False
        if last_result["result"] == "passed":
            return True
        return False

    def delete(self):
        _, ok = DataVerifyApi(req).delete_data_verify(self.id)
        return ok
