import json

from tapsh.lib.backend_apis.task import TaskApi
from tapsh.lib.request import req
from tapsh.lib.data_pipeline.pipeline import Pipeline


def list_heartbeat_tasks():
    return TaskApi(req).list_heartbeat_tasks()


def reset_heartbeat_tasks():
    tasks = list_heartbeat_tasks()
    for task in tasks:
        p = Pipeline(id=task.get("id"))
        p.stop()
        p.reset()