from auto_test.init.global_vars import req

from auto_test.tapdata.data_pipeline.pipeline import Pipeline

def list_share_cdc_tasks():
    res = req.get("/logcollector")
    data = res.json()["data"]
    items = data["items"] if "items" in data else []  # oss 版本返回没有items
    return items
    # return res.json()["data"]["items"]


def reset_share_cdc_tasks():
    tasks = list_share_cdc_tasks()
    for task in tasks:
        p = Pipeline(id=task.get("id"))
        p.stop()
        p.reset()

