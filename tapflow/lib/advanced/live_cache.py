from tapflow.lib.backend_apis.common import ShareCacheApi
from tapflow.lib.request import req
from tapflow.lib.connections.connection import get_table_fields
from tapflow.lib.op_object import get_connection
from tapflow.lib.system.ext_storage import list_external_storages
from tapflow.lib.data_pipeline.job import Job
from tapflow.lib.data_pipeline.pipeline import Pipeline


def list_share_cache_tasks():
    return ShareCacheApi(req).get_all_share_caches()


def clean_share_cache_tasks():
    tasks = list_share_cache_tasks()
    for task in tasks:
        try:
            p = Pipeline(id=task.get("id"))
            p.stop()
            p.delete()
        except Exception as e:
            pass


class ShareCache:
    def list(self):
        return ShareCacheApi(req).get_all_share_caches()

    def __init__(self, name, connection_name, table_name, fields, cache_keys, max_memory, external_storage_type):
        caches = self.list()
        for cache in caches:
            if name == cache["name"]:
                self.id = cache["id"]
                self.job = Job(id=self.id)
                return
        self.id = None
        self.job = None
        external_storage_id = None
        external_storages = list_external_storages()
        connection = get_connection(connection_name)
        external_storages_type = None

        if fields is None or len(fields) == 0:
            fields = get_table_fields(table_name, source=connection.id)
            fields = list(fields.keys())
        if external_storage_type.lower() == "inmemory":
            for item in external_storages:
                if item["type"] == "memory":
                    external_storage_id = item["id"]
                    external_storages_type = item["type"]
                    break
        else:
            for item in external_storages:
                if item["type"] == external_storage_type:
                    external_storage_id = item["id"]
                    external_storages_type = item["type"]
                    break
        self.data = {
            "id": "",
            "name": name,
            "dag": {
                "nodes": [
                    {
                        "type": "table",
                        "attrs": {
                            "fields": fields,
                        },
                        "tableName": table_name,
                        "connectionName": connection_name,
                        "connectionId": connection.id,
                        "databaseType": external_storage_type
                    },
                    {
                        "cacheKeys": cache_keys,
                        "maxMemory": max_memory,
                        "externalStorageId": external_storage_id,
                        "autoCreateIndex": False,
                    }
                ],
                "edges": []
            }
        }
    def save(self):
        if self.id is not None:
            return True
        res = ShareCacheApi(req).create_share_cache(self.data)
        if not res:
            return False
        self.id = res.get("id")
        self.job = Job(id=self.id)
        return True

    def start(self):
        if self.job is None:
            return False
        self.job.start()
        return True

    def delete(self):
        if self.job is None:
            return False
        self.job.delete()
        return True

    def stop(self):
        if self.job is None:
            return False
        self.job.stop()
        return True

    def status(self):
        if self.job is None:
            return False
        return self.job.status(quiet=True)

    def reset(self):
        if self.job is None:
            return False
        self.job.reset()
        return True

