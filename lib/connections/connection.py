import json
import threading
import time
import asyncio
import websockets

from cli.help_decorator import help_decorate
from lib.utils.log import logger

from lib.cache import client_cache, system_server_conf
from lib.op_object import show_tables, get_index_type, match_line, show_connections
from lib.utils.ws import gen_ws_uri_with_id
from lib.request import req

global_lock = threading.Lock()
global_load_schema_q = {}

@help_decorate("Enum, used to describe a connection readable or writeable")
class ConnectionType:
    source = "source"
    target = "target"
    both = "source_and_target"


# get fields for a certain table
def get_table_fields(t, whole=False, source=None, cache=True):
    if source is None and client_cache.get("connection") is not None:
        source = client_cache.get("connection")
    if source is None:
        return None

    table_id = ""
    index_type = get_index_type(t)
    if index_type == "short_id_index":
        t = match_line(client_cache["tables"]["id_index"], t)
        index_type = "id_index"
    if index_type == "id_index":
        table_id = t
    if client_cache["tables"].get(t) is None:
        show_tables(quiet=True, source=source)

    table = client_cache["tables"][source][index_type].get(t, None)
    if table is None:
        show_tables(quiet=True, source=source)
    table = client_cache["tables"][source][index_type].get(t, None)
    if table is None:
        print("table {} not find in system", t)
        return

    table_id = table["id"]
    table_name = table["original_name"]
    res = req.get("/MetadataInstances/" + table_id)
    print(req.base_url)

    data = res.json()["data"]
    fields = data["fields"]
    if whole:
        return fields
    display_fields = {}
    for f in fields:
        node = display_fields
        field_names = f["field_name"].split(".")
        for i in range(len(field_names)):
            field_name = field_names[i]
            if node.get(field_name) is None:
                node[field_name] = {}
            if i < len(field_names) - 1:
                node = node[field_name]
                continue
            if f["data_type"] == "DOCUMENT":
                continue
            node[field_name] = f["data_type"]
    return display_fields


class Connection:
    @help_decorate("__init__ method",
                   args="id or connection, connection can be a dict, or Object has a to_dict() method")
    def __init__(self, id=None, connection=None, name=None):
        if id is None and connection is None and name is None:
            return

        if id is not None:
            self.id = id
            self.c = Connection.get(id=id)
        if connection is not None:
            self.id = connection.id
            self.c = Connection.get(id=id)
        if name is not None:
            self.c = Connection.get(name=name)

        if self.c is None:
            if type(connection) is not type({}):
                try:
                    self.c = connection.to_dict()
                    self.id = self.c["id"]
                except Exception as e:
                    print(__file__, e)
                    return
            else:
                self.c = connection
        else:
            self.id = self.c["id"]

    @help_decorate("save a connection in idaas system")
    def save(self):
        # self.load_schema(quiet=False)
        res = req.post("/Connections", json=self.c)
        show_connections(quiet=True)
        if res.status_code == 200 and res.json()["code"] == "ok":
            self.id = res.json()["data"]["id"]
            self.c = Connection.get(self.id)
            self.load_schema(quiet=True)
            return True
        else:
            print("save Connection fail, err is: {}", res.json())
        return False

    def delete(self):
        res = req.delete("/Connections/" + self.id, json=self.c)
        if res.status_code == 200 and res.json()["code"] == "ok":
            # logger.finfo("delete {} Connection success", self.id)
            return True
        else:
            print("delete Connection fail, err is: {}", res.json())
        return False

    def __getitem__(self, key):
        return self.c[key]

    def __setitem__(self, key, value):
        self.c[key] = value

    @staticmethod
    @help_decorate("static method, used to check whether a connection exists", args="connection name",
                   res="whether exists, bool")
    def exists(name):
        connections = Connection.list()["data"]
        for c in connections:
            if c["name"] == name:
                return True
        return False

    @staticmethod
    @help_decorate("static method, used to list all connections", res="connection list, list")
    def list():
        return req.get("/Connections").json()["data"]

    @staticmethod
    @help_decorate("get a connection, by it's id or name", args="id or name, using kargs",
                   res="a connection/None if not exists, Connection")
    def get(id=None, name=None):
        if id is not None:
            f = {
                "where": {
                    "id": id,
                }
            }
        else:
            f = {
                "where": {
                    "name": name,
                }
            }

        data = req.get("/Connections", params={"filter": json.dumps(f)}).json()["data"]
        if len(data["items"]) == 0:
            return None
        return data["items"][0]

    @help_decorate("test a connection", res="whether connection valid, bool")
    def test(self):
        return self.load_schema()

    def load_schema(self, quiet=True):
        # global global_load_schema_q
        global_lock.acquire(timeout=60)
        if self.id not in global_load_schema_q:
            global_load_schema_q[self.id] = threading.Lock()
        global_lock.release()

        connection_lock = global_load_schema_q[self.id]
        connection_lock.acquire(timeout=120)

        res = True

        async def load():
            async with websockets.connect(gen_ws_uri_with_id()) as websocket:
                data = self.c
                data["database_password"] = self.c.get("plain_password")
                data["transformed"] = True
                payload = {
                    "type": "testConnection",
                    "data": data,
                    "updateSchema": True
                }
                await websocket.send(json.dumps(payload))

                while True:
                    recv = await websocket.recv()
                    load_result = json.loads(recv)
                    if load_result["type"] != "pipe":
                        continue
                    if load_result["data"]["type"] != "testConnectionResult":
                        continue
                    if load_result["data"]["result"]["status"] is None:
                        continue

                    if load_result["data"]["result"]["status"] != "ready":
                        result = False
                    else:
                        result = True

                    if not quiet:
                        if load_result["data"]["result"] is None:
                            continue
                        for detail in load_result["data"]["result"]["response_body"]["validate_details"]:
                            if detail.get("fail_message", None) is not None:
                                logger.log("{}: {}, message: {}", detail["show_msg"], detail["status"],
                                           detail["fail_message"], "debug", "info",
                                           "info" if detail["status"] == "passed" else "warn")
                            else:
                                logger.log("{}: {}", detail["show_msg"], detail["status"], "debug", "info")
                    await websocket.close()
                    return result

        try:
            asyncio.run(load())
        except Exception as e:
            print(f"ws_uri: {system_server_conf['ws_uri']}")
            logger.fwarn("load schema exception, err is: {}", e)

        while True:
            try:
                time.sleep(1)
                res = req.get("/Connections/" + self.id).json()
                if res["data"] is None:
                    break
                if "loadFieldsStatus" not in res["data"]:
                    continue
                if res["data"]["loadFieldsStatus"] in ["finished", "error"]:
                    break
                if res["data"].get("loadFieldErrMsg") != "":
                    break
            except Exception as e:
                print(__file__, e)
                break
        try:
            connection_lock.release()
        except Exception as e:
            print(__file__, e)
            pass
        time.sleep(20)
        return res
