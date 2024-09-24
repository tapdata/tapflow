import json

from tapflow.lib.request import req
from tapflow.lib.data_pipeline.pipeline import Pipeline


def list_heartbeat_tasks():
    filter_param = {"order": "createTime DESC", "limit": 1000, "skip": 0, "where": {"syncType": "connHeartbeat"}}
    res = req.get("/Task", params={"filter": json.dumps(filter_param)})
    return res.json()["data"]["items"]


def reset_heartbeat_tasks():
    tasks = list_heartbeat_tasks()
    for task in tasks:
        p = Pipeline(id=task.get("id"))
        p.stop()
        p.reset()