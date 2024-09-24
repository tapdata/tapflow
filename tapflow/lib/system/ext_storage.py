from tapflow.lib.request import req

default_external_storage_id = None


def list_external_storages():
    res = req.get("/ExternalStorage/list").json()
    if res["code"] != "ok":
        return []
    return res["data"]["items"]


def get_default_external_storage_id():
    global default_external_storage_id
    if default_external_storage_id:
        return default_external_storage_id
    res = req.get("/ExternalStorage/list").json()
    if res["code"] != "ok":
        return None
    for i in res["data"]["items"]:
        if i["defaultStorage"]:
            default_external_storage_id = i["id"]
            return default_external_storage_id
    return None


def set_default_external_storage_id():
    global default_external_storage_id
    default_external_storage_id = get_default_external_storage_id()


def create_rocksdb_cache():
    req.post("/ExternalStorage", json={
        "name": "taptest-rocksdb",
        "type": "rocksdb",
        "uri": "/tmp/taptest",
        "defaultStorage": False
    })
