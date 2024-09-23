import json

from lib.utils.log import logger
from lib.help_decorator import pad

from lib.data_pipeline.data_source import DataSource
from lib.data_services.api import Api
from lib.data_pipeline.job import Job
from lib.cache import client_cache
from lib.request import req

# object that can be operated by command
op_object_command_class = {
    "job": {
        "obj": Job,
        "cache": "jobs"
    },
    "pipeline": {
        "obj": Job,
        "cache": "jobs"
    },
    "datasource": {
        "obj": DataSource,
        "cache": "connections"
    },
    "connection": {
        "obj": DataSource,
        "cache": "connections"
    },
    "db": {
        "obj": DataSource,
        "cache": "connections"
    },
    "api": {
        "obj": Api,
        "cache": "apis"
    },
    "table": {
        "cache": "tables"
    }
}


# get an object with signature
def get_obj(object_type, signature):
    obj = get_signature_v(object_type, signature)
    if obj is None:
        return None
    obj_id = obj["id"]
    obj_name = obj.get("name")
    obj = op_object_command_class[object_type]["obj"](id=obj_id)
    if object_type == "api":
        obj = op_object_command_class[object_type]["obj"](name=obj_name)
    return obj


def get_signature_v(object_type, signature):
    cache_map_index = op_object_command_class[object_type]["cache"]
    if client_cache.get(cache_map_index) is None or object_type == "api":
        exec("show_" + cache_map_index + "(quiet=True)")
    index_type = get_index_type(signature)
    if index_type == "short_id_index":
        signature = match_line(client_cache[cache_map_index]["id_index"], signature)
        index_type = "id_index"
    return client_cache[cache_map_index][index_type].get(signature)


# some global utils, direct relation with this tool
# get signature index type
def get_index_type(s):
    try:
        number_index = int(s)
        return "number_index"
    except Exception as e:
        # print(__file__, "try number index", e)
        pass
    from bson.objectid import ObjectId
    try:
        id_index = ObjectId(s)
        return "id_index"
    except Exception as e:
        # print(__file__, "try id index", e)
        pass
    if len(s) == 6:
        for i in s:
            if ("0" <= i <= "9") or ("a" <= i <= "f"):
                continue
            return "name_index"
    else:
        return "name_index"
    return "short_id_index"


def match_line(m, line):
    for i in m:
        if i.endswith(line):
            return i
    return line


def get_connection(connection_name):
    return get_obj("datasource", connection_name)


def get_table(source, t):
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
    return table


# show tables, must be used after use command
def show_tables(source=None, quiet=False):
    if source is None:
        source = client_cache.get("connection")
    if source is None:
        logger.log(
            "{} before show tables, please use connection first, you can {}, OR {}, OR {}",
            "NO connection USE,",
            "use connection_id",
            "use connection_number",
            "use 'connection_name'",
            "warn", "notice", "notice", "notice"
        )
        return
    source_name = client_cache["connections"]["id_index"][source]["name"]
    f = {"where": {"source.id": source, "sourceType": "SOURCE", "is_deleted": False}, "limit": 999999}
    res = req.get("/MetadataInstances", params={"filter": json.dumps(f)})
    data = res.json()["data"]["items"]
    client_cache["tables"][source] = {"name_index": {}, "id_index": {}, "number_index": {}}
    tables = []
    each_line_table_count = 5
    each_line_tables = []
    max_table_name_len = 0
    for i in range(len(data)):
        if "original_name" not in data[i]:
            continue
        if len(data[i]["original_name"]) > max_table_name_len:
            max_table_name_len = len(data[i]["original_name"])

    for i in range(len(data)):
        if data[i]["meta_type"] == "database":
            continue
        if "original_name" not in data[i]:
            continue
        tables.append(data[i])
        client_cache["tables"][source]["name_index"][data[i]["original_name"]] = data[i]
        client_cache["tables"][source]["id_index"][data[i]["id"]] = data[i]
        client_cache["tables"][source]["number_index"][str(i)] = data[i]

        statement = source_name + "." + data[i]["original_name"] + "=" + '"' + source_name + "." + data[i][
                "original_name"] + '"'
        try:
            exec(statement, globals())
        except Exception as e:
            pass
        if not quiet:
            if len(each_line_tables) == each_line_table_count:
                logger.log("{} " * each_line_table_count, *each_line_tables,
                           *["notice" for i in range(each_line_table_count)])
                each_line_tables = []
            each_line_tables.append(pad(data[i]["original_name"], max_table_name_len))
    if not quiet and len(each_line_tables) > 0:
        logger.log("{} " * len(each_line_tables), *each_line_tables,
                   *["notice" for i in range(len(each_line_tables))])
    return tables


# show datasources
def show_dbs(quiet=True):
    show_connections(quiet=quiet)


# show datasources
def show_datasources():
    show_connections()

def show_agents(quiet=True):
    res = req.get("/agent").json()["data"]
    total = 0
    items = res["items"]
    running_item = []
    if not quiet:
        for item in items:
            if str(item["status"]).lower() == "running":
                running_item.append(item)
    print("="*120)
    logger.info("TapData Cloud Service Running Agent: {}", len(running_item))
    for item in running_item:
        systeminfo = item.get("metric", {}).get("systemInfo", {})
        ip = systeminfo.get("ips", [""])[0]
        logger.info("Agent name: {}, ip: {}, cpu usage: {}%", item["name"], ip, systeminfo.get("cpus"))

show_connections_last_time = 0
# show connections
def show_connections(f=None, quiet=False):
    global show_connections_last_time
    import time
    if show_connections_last_time + 1 > int(time.time()):
        return
    show_connections_last_time = int(time.time())
    f = {"limit": 10000}
    res = req.get("/Connections", params={"filter": json.dumps(f)})
    data = res.json()["data"]["items"]
    client_cache["connections"] = {"name_index": {}, "id_index": {}, "number_index": {}}
    if not quiet:
        logger.log(
            "{} {} {} {}",
            pad("id", 10),
            pad("status", 10),
            pad("database_type", 20),
            pad("name", 35),
            "debug", "debug", "debug", "debug"
        )
    for i in range(len(data)):
        try:
            if "name" not in data[i]:
                continue
            client_cache["connections"]["name_index"][data[i]["name"]] = data[i]
            client_cache["connections"]["id_index"][data[i]["id"]] = data[i]
            client_cache["connections"]["number_index"][str(i)] = data[i]
        except Exception as e:
            continue

        try:
            exec(data[i]["name"] + " = QuickDataSourceMigrateJob()", globals())
            exec(data[i]["name"] + ".__db__ = " + '"' + data[i]["name"] + '"', globals())
        except Exception as e:
            pass

        if not quiet:
            status = data[i].get("status", "unknown")
            name = data[i].get("name", "unknown")
            logger.log(
                "{} {} {} {}",
                pad(data[i]["id"][-6:], 10),
                pad(status, 10),
                pad(data[i]["database_type"], 20),
                pad(name, 35),
                "debug", "info" if status == "ready" else "warn", "notice", "debug"
            )


# show all connectors
def show_connectors(quiet=True):
    res = req.get("/DatabaseTypes")
    data = res.json()["data"]
    o=0
    for i in range(len(data)):
        o += 1
        client_cache["connectors"][data[i]["name"].lower()] = {
            "pdkHash": data[i]["pdkHash"],
            "pdkId": data[i]["pdkId"],
            "pdkType": "pdk",
            "name": data[i]["name"]
        }
        if not quiet:
            x = "Alpha"
            if "Authentication" in data[i]:
                x = data[i]["Authentication"]
            else:
                if "qcType" in data[i]:
                    x = data[i]["qcType"]
            logger.log("{}     {}     [{}]", (str(o)+".").ljust(3), data[i]["name"].ljust(20), x, "info", "notice", "debug")


def get_all_jobs():
    f = {
        "limit": 10000,
        "fields": {
            "syncType": True,
            "id": True,
            "name": True,
            "status": True,
            "last_updated": True,
            "createTime": True,
            "user_id": True,
            "startTime": True,
            "agentId": True,
            "statuses": True,
            "type": True,
            "desc": True
        }
    }
    res = req.get("/Task", params={"filter": json.dumps(f)})
    data = res.json()["data"]["items"]
    return data


# show all jobs
def show_jobs(quiet=False):
    data = get_all_jobs()
    jobs = {"name_index": {}, "id_index": {}, "number_index": {}}
    # logger.finfo("system has {} jobs", len(data))
    for i in range(len(data)):
        if "name" not in data[i]:
            continue
        if not quiet:
            logger.log(
                "{}: " + pad(data[i]["name"], 42) + " {} {}", data[i]["id"][-6:],
                pad(data[i].get("status", "unkownn"), 12),
                data[i].get("syncType", "unknown") + "/" + data[i].get("type", "unknown"),
                "debug", "info" if data[i].get("status", "unkownn") != "error" else "error", "notice"
            )
        jobs["name_index"][data[i]["name"]] = data[i]
        jobs["id_index"][data[i]["id"]] = data[i]
        jobs["number_index"][str(i)] = data[i]
    client_cache["jobs"] = jobs


# show all apis
def show_apis(quiet=False):
    res = req.get("/Modules", params={"order": "createAt DESC", "limit": 20, "skip": 0, "where": {}})
    data = res.json()["data"]["items"]
    client_cache["apis"]["name_index"] = {}
    if not quiet:
        logger.log(
            "{} {} {} {} {}",
            pad("api_name", 20),
            pad("tablename", 20),
            pad("basePath", 20),
            pad("status", 10),
            "test url", "debug", "debug", "debug", "debug", "debug"
        )
    for i in range(len(data)):
        client_cache["connections"]["id_index"][data[i]["datasource"]]["name"]
        client_cache["apis"]["name_index"][data[i]["name"]] = {
            "id": data[i]["id"],
            "table": data[i]["tableName"],
            "name": data[i]["name"],
            "tableName": data[i]["tableName"],
            "database": client_cache["connections"]["id_index"][data[i]["datasource"]]["name"],
        }
        if not quiet:
            logger.log(
                "{} {} {} {} {}",
                pad(data[i]["name"], 20),
                pad(data[i]["tableName"], 20),
                pad(data[i]["basePath"], 20),
                pad(data[i]["status"], 10),
                "http://" + req.server + "#/apiDocAndTest?id=" + data[i]["basePath"] + "_v1",
                "notice", "info", "info", "info" if data[i]["status"] == "active" else "warn", "notice"
            )


# a quick datasource migrate job create direct use db name
# you can use A.syncTo(B) create a migrate job very fast
# QuickDataSourceMigrateJon is called by show_connections() via exec() dynamically, they must be in the same file.
class QuickDataSourceMigrateJob:
    def __init__(self):
        self.__db__ = ""
        self.__p__ = None

    def __getattr__(self, key):
        if key in dir(self):
            return getattr(self, key)
        return self.__db__ + "." + key

    def syncTo(self, target, table=None, prefix="", suffix=""):
        if table is None:
            table = ["_"]
        from lib.data_pipeline.pipeline import Pipeline
        from lib.data_pipeline.nodes.source import Source
        p = Pipeline(self.__db__ + "_sync_to_" + target.__db__)
        source = Source(self.__db__, table=table)
        p.readFrom(source).writeTo(target.__db__, prefix=prefix, suffix=suffix)
        self.__p__ = p
        return self.__p__

    def start(self):
        if self.__p__ is None:
            #logger.fwarn("no sync job create, can not start...")
            return self.__db__ + "." + "start"
        self.__p__.start()
        return self.__p__

    def status(self):
        if self.__p__ is None:
            logger.fwarn("no sync job create, can not status...")
            return self.__db__ + "." + "status"
        self.__p__.status()
        return self.__p__

    def monitor(self):
        if self.__p__ is None:
            logger.fwarn("no sync job create, can not monitor...")
            return self.__db__ + "." + "monitor"
        self.__p__.monitor()
        return self.__p__

    def stop(self):
        if self.__p__ is None:
            logger.fwarn("no sync job create, can not stop...")
            return self.__db__ + "." + "stop"
        self.__p__.stop()
        return self.__p__

    def delete(self):
        ds = get_obj("datasource", self.__db__)
        if ds is not None:
            if ds.delete():
                pass
            else:
                logger.fwarn("delete datasource {} fail, maybe some job is still use it", self.__db__)
            return
        logger.fwarn("datasource {} not found", self.__db__)

