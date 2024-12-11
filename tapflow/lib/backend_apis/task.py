import json
from typing import Tuple

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

