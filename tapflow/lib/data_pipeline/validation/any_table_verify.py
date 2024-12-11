import time
import urllib
import uuid

from tapflow.lib.backend_apis.dataVerify import DataVerifyApi
from tapflow.lib.request import req
from tapflow.lib.utils.log import logger


any_table_verify_body = {
            "flowId": "",
            "name": " - 校验",
            "mode": "manual",
            "inspectDifferenceMode": "All",
            "inspectMethod": "field",
            "cdcBeginDate": "",
            "cdcEndDate": "",
            "cdcDuration": "",
            "enabled": True,
            "status": "waiting",
            "lastStartTime": 0,
            "byFirstCheckId": "",
            "browserTimezoneOffset": 0,
            "limit": {
                "keep": 100
            },
            "platformInfo": {
                "agentType": "private"
            },
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
            ]
        }


class AnyTableDataVeryfy:
    def __init__(self, id, name, tasks, mode="row_count"):
        self.mode = mode
        self.id = id
        verify_job = any_table_verify_body
        verify_job["name"] = name + " - 校验"
        verify_job["inspectMethod"] = mode
        verify_job["tasks"] = tasks
        res, ok = DataVerifyApi(req).create_data_verify(verify_job)
        if ok:
            self.id = res["data"]["id"]

    def start(self):
        _, ok = DataVerifyApi(req).update_data_verify(self.id, "scheduling")
        if ok:
            return True
        return False

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

    def last_pass(self):
        self.wait_finish()
        last_result = self.last_result()
        if last_result is None:
            return False
        if last_result["result"] == "passed":
            return True
        return False

    def inspect_result(self):
        self.wait_finish()
        last_result = self.last_result()
        return last_result

    def delete(self):
        _, ok = DataVerifyApi(req).delete_data_verify(self.id)
        return ok


class ConnectionDataVerify:
    def __init__(self, id, name, table_name, pk):
        self.id = id
        self.name = name
        self.tableName = table_name
        self.pk = pk


class VerifyTask:
    def __init__(self, source: ConnectionDataVerify, target: ConnectionDataVerify):
        self.task = {
            "source": {
                "nodeId": "",
                "connectionId": source.id,
                "connectionName": source.name,
                "table": source.tableName,
                "sortColumn": source.pk,
                "fields": [],
                "columns": None,
                "isFilter": False,
                "conditions": [],
                "capabilities": [],
                "nodeSchema": []
            },
            "target": {
                "nodeId": "",
                "connectionId": target.id,
                "connectionName": target.name,
                "table": target.tableName,
                "sortColumn": target.pk,
                "fields": [],
                "columns": None,
                "isFilter": False,
                "conditions": [],
                "capabilities": [],
                "nodeSchema": []
            },
            "taskId": str(uuid.uuid4()).replace("-", ""),
            "fullMatch": True,
            "script": "",
            "showAdvancedVerification": False,
            "webScript": ""
        }

