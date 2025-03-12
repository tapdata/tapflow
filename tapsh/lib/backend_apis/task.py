import json
import time
from typing import Tuple

import requests

from .common import BaseBackendApi


class TaskApi(BaseBackendApi):

    def get_all_tasks(self) -> list:
        payload = {
            "limit": 10000,
            "fields": {
                "syncType": True,
                "id": True,
                "name": True,
                "status": True,
                "last_updated": True,
                "createTime": True,
                "user_id": True,
                "startTime": True,
                "agentId": True,
                "statuses": True,
                "type": True,
                "desc": True,
                "stats": True
            }
        }
        res = self.req.get("/Task", params={"filter": json.dumps(payload)})
        return res.json()["data"]["items"]
    
    def filter_tasks_by_name(self, name: str, limit: int = 20, skip: int = 0) -> list:
        """
        根据任务名称过滤任务,过滤模式为LIKE
        :param name: 任务名称
        :param limit: 限制数量, 默认20
        :param skip: 跳过数量, 默认0
        :return: 任务列表
        """
        payload = {
            "order": "last_updated DESC",
            "limit": limit,
            "fields": {
                "id": True,
                "name": True,
                "status": True,
                "last_updated": True,
                "createTime": True,
                "user_id": True,
                "startTime": True,
                "agentId": True,
                "statuses": True,
                "type": True,
                "desc": True,
                "listtags": True,
                "syncType": True,
                "stoppingTime": True,
                "pingTime": True,
                "canForceStopping": True,
                "currentEventTimestamp": True,
                "crontabExpressionFlag": True,
                "crontabExpression": True,
                "crontabScheduleMsg": True,
                "lastStartDate": True,
                "functionRetryStatus": True,
                "taskRetryStatus": True,
                "shareCdcStop": True,
                "shareCdcStopMessage": True,
                "taskRetryStartTime": True,
                "errorEvents": True,
                "syncStatus": True,
                "restartFlag": True,
                "attrs": True
            },
            "skip": skip,
            "where": {
                "name": {
                    "like": name,
                    "options": "i"
                }
            }
        }
        
        res = self.req.get("/Task", params={"filter": json.dumps(payload)})
        if res.status_code != 200 or res.json()["code"] != "ok":
            return []
        return res.json()["data"]["items"]
    
    def list_heartbeat_tasks(self) -> list:
        filter_param = {"order": "createTime DESC", "limit": 1000, "skip": 0, "where": {"syncType": "connHeartbeat"}}
        res = self.req.get("/Task", params={"filter": json.dumps(filter_param)})
        return res.json()["data"]["items"]
    
    def reset_task(self, task_id: str) -> bool:
        res = self.req.patch(f"/Task/batchRenew", params={"taskIds": task_id})
        return res.status_code == 200 and res.json()["code"] == "ok"
    
    def get_task_by_name(self, task_name: str) -> dict:
        """
        get task by name
        :param task_name: task name
        :return: task, bool
        """
        import urllib.parse
        param = '{"where":{"name":{"like":"%s"}}}' % (task_name)
        param = urllib.parse.quote(param, safe="().")
        param = param.replace(".", "%5C%5C.")
        param = param.replace("(", "%5C%5C(")
        param = param.replace(")", "%5C%5C)")
        res = self.req.get(f"/Task?filter={param}")
        for task in res.json().get("data", {}).get("items", []):
            if task.get("name") == task_name:
                return task
        return None
    
    def get_task_id_by_name(self, task_name: str) -> str:
        task = self.get_task_by_name(task_name)
        if task is None:
            return None
        return task["id"]
    
    def get_task_by_id(self, task_id: str) -> dict:
        res = self.req.get(f"/Task/{task_id}")
        if res.status_code != 200:
            return None
        return res.json()["data"]
    
    def stop_task(self, task_id: str, force: bool = False) -> bool:
        res = self.req.put(f"/Task/batchStop", params={"taskIds": task_id, "force": force})
        return res.status_code == 200 and res.json()["code"] == "ok"
    
    def delete_task(self, task_id: str) -> bool:
        res = self.req.delete(f"/Task/batchDelete", params={"taskIds": task_id})
        return res.status_code == 200 and res.json()["code"] == "ok"

    def copy_task(self, task_id: str) -> Tuple[dict, bool]:
        res = self.req.put(f"/Task/copy/{task_id}")
        ok = res.status_code == 200 and res.json()["code"] == "ok"
        if ok:
            return res.json()["data"], True
        return None, False
    
    def get_task_relations(self, task_id: str) -> list:
        res = self.req.post("/task-console/relations", json={"taskId": task_id})
        return res.json()["data"] if res.status_code == 200 and res.json()["code"] == "ok" else []
    
    def update_task(self, task: dict) -> Tuple[dict, bool]:
        res = self.req.patch("/Task", json=task)
        return res.json()["data"], res.status_code == 200 and res.json()["code"] == "ok"
    
    def create_task(self, task: dict) -> Tuple[dict, bool]:
        res = self.req.post("/Task", json=task)
        ok = res.status_code == 200 and res.json()["code"] == "ok"
        if ok:
            return res.json()["data"], True
        else:
            if res.json()["code"] == "Task.RepeatName":
                task_id = self.get_task_id_by_name(task["name"])
                if task_id is None:
                    return None, False
                task["id"] = task_id
                return self.update_task(task), True
        return res.json(), False
    
    def confirm_task(self, task_id: str, task: dict) -> Tuple[dict, bool]:
        res = self.req.patch(f"/Task/confirm/{task_id}", json=task)
        ok = res.status_code == 200 and res.json()["code"] == "ok"
        if ok:
            return res.json()["data"], True
        return None, False

    def start_task(self, task_id: str) -> Tuple[dict, bool]:
        res = self.req.put("/Task/batchStart", params={"taskIds": task_id})
        return res.json()["data"], res.status_code == 200 and res.json()["code"] == "ok"

    def rename_task(self, task_id: str, new_name: str) -> Tuple[dict, bool]:
        res = self.req.patch(f"/Task/rename/{task_id}", params={"newName": new_name})
        return res.json()["data"], res.status_code == 200 and res.json()["code"] == "ok"
    
    def model_deduction(self, node_id: str, connection_id: str, node_config: dict) -> Tuple[dict, bool]:
        """
        模型推演
        :param node_id: 节点id
        :param connection_id: 连接id
        :param node_config: 节点配置
        :return: 推演结果, 是否成功
        """
        res = self.req.post("/proxy/call", json={
            "className": "DiscoverSchemaService",
            "method": "discoverSchema",
            "nodeId": node_id,
            "args": [
                connection_id,
                node_config,
            ]
        })
        return res.json()["data"], res.status_code == 200 and res.json()["code"] == "ok"
    
    def get_task_measurement(self, task_id: str, task_record_id: str) -> dict:
        payload = {
            "totalData": {
                "uri": "/api/measurement/query/v2",
                "param": {
                    "startAt": int(time.time() * 1000)-300000,
                    "endAt": int(time.time() * 1000),
                    "samples": {
                        "data": {
                            "endAt": int(time.time() * 1000),
                            "fields": [
                                "inputInsertTotal",
                                "inputUpdateTotal",
                                "inputDeleteTotal",
                                "inputDdlTotal",
                                "inputOthersTotal",
                                "outputInsertTotal",
                                "outputUpdateTotal",
                                "outputDeleteTotal",
                                "outputDdlTotal",
                                "outputOthersTotal",
                                "tableTotal",
                                "createTableTotal",
                                "snapshotTableTotal",
                                "initialCompleteTime",
                                "sourceConnection",
                                "targetConnection",
                                "snapshotDoneAt",
                                "snapshotRowTotal",
                                "snapshotInsertRowTotal",
                                "inputQps",
                                "outputQps",
                                "currentSnapshotTableRowTotal",
                                "currentSnapshotTableInsertRowTotal",
                                "replicateLag",
                                "snapshotStartAt",
                                "snapshotTableTotal",
                                "currentEventTimestamp",
                                "snapshotDoneCost",
                                "outputQpsMax",
                                "outputQpsAvg",
                                "lastFiveMinutesQps"
                            ],
                            "tags": {
                                "taskId": task_id,
                                "taskRecordId": task_record_id,
                                "type": "task"
                            },
                            "type": "instant",
                        }
                    }
                }
            }
        }
        res = self.req.post("/measurement/batch", json=payload, timeout=3)
        return res.json()["data"] if res.status_code == 200 and res.json()["code"] == "ok" else None
    
    def get_task_logs(self, level: str, limit: int, task_id: str, task_record_id: str, start: int, end: int) -> Tuple[list, bool]:
        """
        获取任务日志
        :param level: 日志级别
        :param limit: 日志数量
        :param task_id: 任务id
        :param task_record_id: 任务记录id
        :param start: 开始时间
        :param end: 结束时间
        :return: 日志列表, 是否成功
        """
        payload = {
            "levels": [level],
            "order": "desc",
            "page": 1,
            "pageSize": limit,
            "taskId": task_id,
            "taskRecordId": task_record_id,
            "start": start,
            "end": end
        }
        res = self.req.post("/MonitoringLogs/query", json=payload)
        return res.json()["data"] if res.status_code == 200 and res.json()["code"] == "ok" else [], res.status_code == 200 and res.json()["code"] == "ok"
    
    def task_preview(self, task: dict) -> Tuple[dict, bool]:
        """
        任务预览
        :param task: 任务
        :return: 预览结果, 是否成功
        """
        res = self.req.post("/proxy/call", json={
            "className": "TaskPreviewService",
            "method": "preview",
            "args": [
                json.dumps(task),
                None,
                1
            ]
        })
        return res.json()["data"], res.status_code == 200 and res.json()["code"] == "ok"
    
    def preview_task(self, connection_id: str, table_name: str) -> requests.Response:
        res = self.req.post("/proxy/call", json={
            "className": "QueryDataBaseDataService",
            "method": "getData",
            "args": [
                connection_id,
                table_name
            ]
        })
        return res.json()
