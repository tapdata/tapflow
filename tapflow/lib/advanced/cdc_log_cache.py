from tapflow.lib.backend_apis.common import LogCollectorApi
from tapflow.lib.request import req

from tapflow.lib.data_pipeline.pipeline import Pipeline

def list_share_cdc_tasks():
    items = LogCollectorApi(req).get_all_log_collectors()
    return items

def reset_share_cdc_tasks():
    tasks = list_share_cdc_tasks()
    for task in tasks:
        p = Pipeline(id=task.get("id"))
        p.stop()
        p.reset()