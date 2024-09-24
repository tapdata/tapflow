from tapflow.lib.request import req

from tapflow.lib.data_pipeline.pipeline import Pipeline

def list_share_cdc_tasks():
    res = req.get("/logcollector")
    data = res.json()["data"]
    items = data["items"] if "items" in data else []  # oss 版本返回没有items
    return items

def reset_share_cdc_tasks():
    tasks = list_share_cdc_tasks()
    for task in tasks:
        p = Pipeline(id=task.get("id"))
        p.stop()
        p.reset()