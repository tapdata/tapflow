import json
import sys
from IPython.core.magic import Magics, magics_class, line_magic
from IPython.terminal.interactiveshell import TerminalInteractiveShell
from platform import python_version

from lib.utils.log import logger
from cli.help_decorator import help_decorate, show_help

from lib.connections.connection import get_table_fields
from lib.request import req
from lib.cache import client_cache, system_server_conf

from lib.login import login_with_access_code, login_with_ak_sk
from lib.op_object import (get_obj, get_signature_v, get_index_type, match_line, show_apis, show_tables,
                                         show_connections, show_connectors, show_dbs, op_object_command_class, show_agents)
from lib.op_object import DataSource
from lib.data_pipeline.pipeline import Pipeline, MView
from lib.data_pipeline.nodes.source import Source
from lib.data_pipeline.nodes.sink import Sink

if not python_version().startswith("3"):
    print("python version must be 3.x, please install python3 before using tapdata cli")
    sys.exit(-1)
import os


os.environ['PYTHONSTARTUP'] = '>>>'
os.environ["PROJECT_PATH"] = os.sep.join([os.path.dirname(os.path.abspath(__file__)), ".."])

help_args = {
    "command": "command_help",
    "lib": "lib_help",
}

@magics_class
class global_help(Magics):
    @line_magic
    @help_decorate("show global help", "h command")
    # h line_magic
    def h(self, t=None):
        if not t:
            for k, v in help_args.items():
                logger.log("{}: {}", k, v, "info", "debug")
            return
        try:
            show_help(t)
        except Exception as e:
            logger.warn("no help commands for {} found, please use below command for help, e is: {}", t, e)
            self.h()


@magics_class
# global command for object
class op_object_command(Magics):
    types = op_object_command_class.keys()
    def __common_op(self, op, line):
        try:
            object_type, signature = line.split(" ")[0], line.split(" ")[1]
            if object_type not in self.types:
                object_type = "job"
                signature = line.split(" ")[0]

        except Exception as e:
            object_type = "job"
            signature = line.split(" ")[0]
        args = []
        kwargs = {}
        if len(line.split(" ")) > 2:
            for kv in line.split(" ")[2:]:
                if "=" not in kv:
                    args.append(kv)
                else:
                    v = kv.split("=")[1]
                    try:
                        v = int(v)
                        kwargs[kv.split("=")[0]] = v
                    except Exception as e:
                        if v.lower() == "true":
                            kwargs[kv.split("=")[0]] = True
                            continue
                        if v.lower() == "false":
                            kwargs[kv.split("=")[0]] = False
                            continue
                        kwargs[kv.split("=")[0]] = v
        obj = get_obj(object_type, signature)
        if obj is None:
            return
        if op in dir(obj):
            import inspect
            method_args = inspect.getfullargspec(getattr(obj, op)).args
            if "quiet" in method_args:
                kwargs["quiet"] = False
            getattr(obj, op)(*args, **kwargs)

    @line_magic
    @help_decorate("[Job] stop a running job", "stop job $job_name")
    def stop(self, line):
        return self.__common_op("stop", line)

    @line_magic
    @help_decorate("[Job,Datasource,Api] display a object status", "status datasource $datasource_name")
    def status(self, line):
        return self.__common_op("status", line)

    @line_magic
    @help_decorate("[Job] keep monitor a object status", "monitor job $job_name t=30")
    def monitor(self, line):
        return self.__common_op("monitor", line)

    @line_magic
    @help_decorate("[Job] start a job", "start job $job_name")
    def start(self, line):
        return self.__common_op("start", line)

    @line_magic
    @help_decorate("[Job,Datasource,Api] delete a object", "delete object $object_name")
    def delete(self, line):
        return self.__common_op("delete", line)

    @line_magic
    @help_decorate("[Datasource] validate a datasource, and load it's schema", "validate datasource $datasource_id")
    def validate(self, line):
        return self.__common_op("validate", line)

    @line_magic
    @help_decorate("[Job] display job logs", "logs job $job_name limit=100 tail=True")
    def logs(self, line):
        return self.__common_op("logs", line)

    @line_magic
    @help_decorate("[Job] display a job stats", "stats job $job_name")
    def stats(self, line):
        return self.__common_op("stats", line)

    @line_magic
    @help_decorate("[Job,Datasource,Api,Table] desc a object", "desc object $object_name")
    def desc(self, line):
        if line == "":
            logger.warn("no desc datasource found")
            return
        if " " not in line or line.split(" ")[0] == "table":
            if " " in line:
                line = line.split(" ")[1]
            return desc_table(line, quiet=False)
        return self.__common_op("desc", line)


@magics_class
class ApiCommand(Magics):
    @line_magic
    def unpublish(self, line):
        if len(client_cache["apis"]["name_index"]) == 0:
            show_apis()
        payload = {
            "id": client_cache["apis"]["name_index"][line]["id"],
            "tablename": client_cache["apis"]["name_index"][line]["table"],
            "status": "pending"
        }
        res = req.patch("/Modules", json=payload)
        res = res.json()
        if res["code"] == "ok":
            pass
            #logger.info("unpublish {} success", line)
        else:
            logger.warn("unpublish {} fail, err is: {}", line, res)

    @line_magic
    def publish(self, line):
        if " " not in line:
            return

        base_path = line.split(" ")[0]
        line = line.split(" ")[1]

        if client_cache.get("connections") is None and "." not in line:
            logger.warn("no DataSource set, only table is not enough")
            return
        db = client_cache.get("connection")
        table = line
        if "." in line:
            db = line.split(".")[0]
            table = line.split(".")[1]
            if client_cache.get("connections") is None:
                show_connections(quiet=True)
            if db not in client_cache["connections"]["name_index"]:
                show_connections(quiet=True)
            if db not in client_cache["connections"]["name_index"]:
                logger.warn("no Datasource {} found in system", db)
            db = client_cache["connections"]["name_index"][db]["id"]

        fields = get_table_fields(table, whole=True, source=db)
        payload = {
            "apiType": "defaultApi",
            "apiVersion": "v1",
            "basePath": base_path,
            "createType": "",
            "datasource": db,
            "describtion": "",
            "name": base_path,
            "path": "/api/v1/" + base_path,
            "readConcern": "majority",
            "readPreference": "primary",
            "status": "active",
            "tablename": table,
            "fields": fields,
            "paths": [
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Create a new record",
                    "method": "POST",
                    "name": "create",
                    "path": "/api/v1/" + base_path,
                    "result": "Document",
                    "type": "preset"
                },
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Get records based on id",
                    "method": "GET",
                    "name": "findById",
                    "params": [
                        {
                            "defaultvalue": 1,
                            "description": "document id",
                            "name": "id",
                            "type": "string"
                        }
                    ],
                    "path": "/api/v1/" + base_path + "/{id}",
                    "result": "Document",
                    "type": "preset"
                },
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Update record according to id",
                    "method": "PATCH",
                    "name": "updateById",
                    "params": [
                        {
                            "defaultvalue": 1,
                            "description": "document id",
                            "name": "id",
                            "type": "string"
                        }
                    ],
                    "path": "/api/v1/" + base_path + "{id}",
                    "result": "Document",
                    "type": "preset"
                },
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Delete records based on id",
                    "method": "DELETE",
                    "name": "deleteById",
                    "params": [
                        {
                            "description": "document id",
                            "name": "id",
                            "type": "string"
                        }
                    ],
                    "path": "/api/v1/" + base_path + "{id}",
                    "type": "preset"
                },
                {
                    "acl": [
                        "admin"
                    ],
                    "description": "Get records by page",
                    "method": "GET",
                    "name": "findPage",
                    "params": [
                        {
                            "defaultvalue": 1,
                            "description": "page number",
                            "name": "page",
                            "type": "int"
                        },
                        {
                            "defaultvalue": 20,
                            "description": "max records per page",
                            "name": "limit",
                            "type": "int"
                        },
                        {
                            "description": "sort setting,Array ,format like [{'propertyName':'ASC'}]",
                            "name": "sort",
                            "type": "object"
                        },
                        {
                            "description": "search filter object,Array",
                            "name": "filter",
                            "type": "object"
                        }
                    ],
                    "path": "/api/v1/" + base_path,
                    "result": "Page<Document>",
                    "type": "preset"
                }
            ]
        }
        res = req.post("/Modules", json=payload).json()
        if res["code"] == "ok":
            pass
            #logger.info(
            #    "publish api {} success, you can test it by: {}",
            #    base_path,
            #    "http://" + server + "#/apiDocAndTest?id=" + base_path + "_v1"
            #)
        else:
            pass
            logger.warn("publish api {} fail, err is: {}", base_path, res["message"])


@magics_class
class show_command(Magics):
    @line_magic
    @help_decorate("[Job,Datasource,Api,Table] show objects", "show objects")
    def show(self, line):
        if not line:
            pass
        try:
            eval("show_" + line + "(quiet=False)")
        except Exception as e:
            eval("show_dbs('" + line + "')")

    @line_magic
    @help_decorate("[Datasource] switch datasource context", "use $object_name")
    def use(self, line):
        if line == "":
            logger.warn("no use datasource found")
            return
        connection = get_signature_v("datasource", line)
        connection_id = connection["id"]
        connection_name = connection["name"]
        client_cache["connection"] = connection_id
        logger.info("datasource switch to: {}", connection_name)

    @line_magic
    @help_decorate("[Table] peek 5 table content for preview", "peek $table_name")
    def peek(self, line):
        if line == "":
            logger.warn("no peek datasource found")
            return
        if client_cache.get("connections") is None:
            show_connections(quiet=True)
        connection_id = client_cache.get("connection")
        table = line
        if "." in line:
            db = line.split(".")[0]
            table = line.split(".")[1]
            connection = get_signature_v("datasource", db)
            connection_id = connection["id"]
        limit = 5
        if " " in line:
            try:
                limit = int(line.split(" ")[1])
            except Exception as e:
                pass

        table_id = ""
        index_type = get_index_type(line)
        if index_type == "short_id_index":
            line = match_line(client_cache["tables"]["id_index"], line)
            index_type = "id_index"
        if index_type == "id_index":
            table_id = line
        if client_cache["tables"].get(connection_id) is None:
            show_tables(source=connection_id, quiet=True)
        table = client_cache["tables"][connection_id][index_type][table]

        table_id = table["id"]
        table_name = table["original_name"]

        body = {
            "className": "QueryDataBaseDataService",
            "method": "getData",
            "args": [
                connection_id,
                table_name
            ]
        }
        res = req.post("/proxy/call", json=body).json()
        try:
            sample_data = res["data"]["sampleData"]
            x = 0
            for i in sample_data:
                x += 1
                if x > limit:
                    break
                print(i)
        except Exception as e:
            pass

    @line_magic
    @help_decorate("[Table] count table rows", "count $table_name")
    def count(self, line):
        if line == "":
            logger.warn("no count datasource found")
            return
        if client_cache.get("connections") is None:
            show_connections(quiet=True)
        connection_id = client_cache.get("connection")
        table = line
        if "." in line:
            db = line.split(".")[0]
            table = line.split(".")[1]
            connection = get_signature_v("datasource", db)
            connection_id = connection["id"]
        table_id = ""
        index_type = get_index_type(line)
        if index_type == "short_id_index":
            line = match_line(client_cache["tables"]["id_index"], line)
            index_type = "id_index"
        if index_type == "id_index":
            table_id = line
        if client_cache["tables"].get(connection_id) is None:
            show_tables(source=connection_id, quiet=True)
        table = client_cache["tables"][connection_id][index_type][table]
        table_id = table["id"]
        table_name = table["original_name"]


@help_decorate("display table struct", "table signature")
def desc_table(line, quiet=True):
    connection_id = client_cache.get("connection")
    db = connection_id
    if "." in line:
        db = line.split(".")[0]
        line = line.split(".")[1]
    index_type = get_index_type(db)
    if index_type == "short_id_index":
        db = match_line(client_cache["connections"]["id_index"], db)
        index_type = "id_index"
    if index_type == "id_index":
        client_cache["connection"] = db
    if client_cache.get("connections") is None:
        show_connections(quiet=True)
    if db is None:
        logger.warn("please {} before desc table, or {} to get a valid result", "use db", "use db.table")
        return

    connection = client_cache["connections"][index_type][db]
    connection_id = connection["id"]

    if connection_id is None:
        return

    display_fields = get_table_fields(line, source=connection_id)
    if not quiet:
        print(json.dumps(display_fields, indent=2))


def main():
    # ipython settings
    ip = TerminalInteractiveShell.instance()
    ip.register_magics(global_help)
    ip.register_magics(show_command)
    ip.register_magics(op_object_command)
    ip.register_magics(ApiCommand)
    ini_config = "etc/config.ini"
    if os.path.exists(ini_config):
        import configparser
        config = configparser.ConfigParser()
        config.read(ini_config)
        global server, access_token
        ini_dict = {section: dict(config.items(section)) for section in config.sections()}
        server = ini_dict.get("backend", {}).get("server")
        access_token = ini_dict.get("backend", {}).get("access_code")
        ak = ini_dict.get("backend", {}).get("ak")
        sk = ini_dict.get("backend", {}).get("sk")
        if ak and sk:
            login_with_ak_sk(ak, sk)
            show_agents(quiet=False)
        else:
            if server and access_token:
                login_with_access_code(server, access_token)
    else:
        sys.exit(-1)
    show_connections(quiet=True)
    show_connectors(quiet=True)


if __name__ == "__main__":
    main()
