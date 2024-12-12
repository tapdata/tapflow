from tapflow.lib.backend_apis.common import ExternalStorageApi
from tapflow.lib.request import req

default_external_storage_id = None


def list_external_storages():
    res, ok = ExternalStorageApi(req).get_all_external_storages()
    if not ok:
        return []
    return res["data"]["items"]


def get_default_external_storage_id():
    global default_external_storage_id
    if default_external_storage_id:
        return default_external_storage_id
    res, ok = ExternalStorageApi(req).get_all_external_storages()
    if not ok:
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
    ExternalStorageApi(req).create_external_storage({
        "name": "taptest-rocksdb",
        "type": "rocksdb",
        "uri": "/tmp/taptest",
        "defaultStorage": False
    })
