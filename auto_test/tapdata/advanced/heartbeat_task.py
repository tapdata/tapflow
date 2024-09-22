import json

from auto_test.init.global_vars import req
from auto_test.tapdata.data_pipeline.pipeline import Pipeline


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

