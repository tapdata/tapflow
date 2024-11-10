from datetime import datetime
import getpass
import shlex
import os, sys

# 获取当前脚本文件所在的目录
current_dir = os.path.dirname(os.path.abspath(__file__))

# 获取 lib 目录的路径
lib_path = os.path.join(current_dir, '..')

# 将 lib 目录加入到 Python 搜索路径中
sys.path.append(lib_path)

from IPython.core.magic import Magics, magics_class, line_magic
from IPython.terminal.interactiveshell import TerminalInteractiveShell
from platform import python_version
from tapflow.lib.utils.log import logger
from tapflow.lib.connections.connection import get_table_fields
from tapflow.lib.request import req
from tapflow.lib.cache import client_cache
from tapflow.lib.data_pipeline.data_source import DataSource
from tapflow.lib.data_pipeline.nodes.source import Source
from tapflow.lib.data_pipeline.nodes.sink import Sink
from tapflow.lib.data_pipeline.nodes.union import UnionNode
from tapflow.lib.data_pipeline.pipeline import Pipeline, MView, Flow, _flows

from tapflow.lib.login import login_with_access_code, login_with_ak_sk
from tapflow.lib.op_object import *
from tapflow.lib.op_object import (get_obj, get_signature_v, get_index_type, match_line, show_apis, show_tables,
                           show_connections, show_connectors, op_object_command_class, show_agents, show_dbs, show_jobs)

from tapflow.lib.connections.connection import get_table_fields

from tapflow.lib.login import login_with_access_code, login_with_ak_sk
from tapflow.lib.op_object import (get_obj, get_signature_v, get_index_type, match_line, show_apis, show_tables,
                                   show_connections, show_connectors, op_object_command_class, show_agents)

if not python_version().startswith("3"):
    print("python version must be 3.x, please install python3 before using tapdata cli")
    sys.exit(-1)


os.environ['PYTHONSTARTUP'] = '>>>'
os.environ["PROJECT_PATH"] = os.sep.join([os.path.dirname(os.path.abspath(__file__)), ".."])

help_args = {
    "command": "command_help",
    "lib": "lib_help",
}

@magics_class
# global command for object
class op_object_command(Magics):
    types = op_object_command_class.keys()
    def __common_op(self, op, line):
        try:
            object_type, signature = shlex.split(line)[0], shlex.split(line)[1]
            if object_type not in self.types:
                object_type = "job"
                signature = shlex.split(line)[0]

        except Exception as e:
            object_type = "job"
            signature = shlex.split(line)[0]
        args = []
        kwargs = {}
        if len(shlex.split(line)) > 2:
            for kv in shlex.split(line)[2:]:
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
            if "force" in method_args:
                kwargs["force"] = True
            if op == "delete":
                confirm = input(f"Are you sure you want to delete {object_type if object_type != 'job' else 'flow'} {signature} (y/[n]): ")
                if confirm != "y":
                    return
            getattr(obj, op)(*args, **kwargs)

    @line_magic
    def stop(self, line):
        return self.__common_op("stop", line)

    @line_magic
    def status(self, line):
        return self.__common_op("stats", line)

    @line_magic
    def monitor(self, line):
        return self.__common_op("monitor", line)
    
    @line_magic
    def copy(self, line):
        return self.__common_op("copy", line)

    @line_magic
    def start(self, line):
        return self.__common_op("start", line)

    @line_magic
    def reset(self, line):
        return self.__common_op("reset", line)

    @line_magic
    def delete(self, line):
        return self.__common_op("delete", line)

    @line_magic
    def validate(self, line):
        return self.__common_op("validate", line)

    @line_magic
    def logs(self, line):
        return self.__common_op("logs", line)

    @line_magic
    def stats(self, line):
        return self.__common_op("stats", line)

    @line_magic
    def desc(self, line):
        if line == "":
            logger.warn("no desc datasource found")
            return
        if " " not in line or line.split(" ")[0] == "table":
            if " " in line:
                line = line.split(" ")[1]
            return desc_table(line, quiet=False)
        return self.__common_op("desc", line)
    
    @line_magic
    def tap(self, line):
        line = line.replace("-f", "").strip()
        with open(line, "r") as f:
            exec(f.read())


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
    def show(self, line):
        if not line:
            pass
        if line in _flows:
            print(_flows[line].show())
            return
        for name, value in globals().items():
            if name == line and isinstance(value, Flow):
                print(value.show())
                return
        try:
            if "dbs" == line:
                globals().update(eval("show_dbs(quiet=False)"))
                return
            eval("show_" + line + "(quiet=False)")
        except Exception as e:
            eval("show_dbs('" + line + "')")

    @line_magic
    def preview(self, line):
        if line in _flows:
            print(_flows[line].preview())
            return
        for name, value in globals().items():
            if name == line and isinstance(value, Flow):
                print(value.preview())

    @line_magic
    def use(self, line):
        if line == "":
            logger.warn("no use datasource found")
            return
        connection = get_signature_v("datasource", line)
        if connection is None:
            logger.warn("connection {} not found", line)
            return

        connection_id = connection["id"]
        connection_name = connection["name"]
        client_cache["connection"] = connection_id
        logger.info("datasource switch to: {}", connection_name)

    @line_magic
    def h(self, line):
        self.help(line)

    @line_magic
    def help(self, line):
        def h_command():
            logger.notice("{}", "- show datasource/table")
            logger.info("    1. {}: show datasource list", pad("show dbs", 20))
            logger.info("    2. {}: switch to datasource", pad("use $db_name", 20))
            logger.info("    3. {}: after use $db_name, used to show tables", pad("show tables", 20))
            logger.info("    4. {}: describe table schema", pad("desc $table_name", 20))
            logger.info("    5. {}: peek some records from table", pad("peek $table_name", 20))
            logger.notice("{}", "- jobs command")
            logger.info("    1. {}: show all jobs", pad("show jobs", 20))
            logger.info("    2. {}: start a job", pad("start $job_name", 20))
            logger.info("    3. {}: stop a job", pad("stop $job_name", 20))
            logger.info("    4. {}: status a job", pad("status $job_name", 20))
            logger.info("    5. {}: show metrics", pad("stats $job_name", 20))
            logger.info("    6. {}: delete a job", pad("delete $job_name", 20))
            logger.notice("{}", "- create a datasource")
            logger.info("    1. {}", "x = DataSource('mysql', 'my-mysql').host('localhost').port(3306).username('root').password('<PASSWORD>')")
            logger.info("    2. {}", "x.save()")
            logger.notice("{}", "- create a simple flow")
            logger.info("    1. {}", "x = Flow('name')")
            logger.info("    2. {}", "x.read_from($ds.$source_table)")
            logger.notice("{}", "- add nodes in a flow")
            logger.info("    1. {}", "x = Flow('name')")
            logger.info("    2. {}: x.filter('id > 2 and sex=male')", "filter records")
            logger.info("    3. {}: x.filter_columns(['id', 'name'], 'keep'])", "filter columns")
            logger.info("    4. {}: x.rename_fields(dict: $old_name -> $new_name)", "rename fields")
            logger.info("    5. {}: x.func($func), support js/python code", "add func")
            logger.notice("{}", "- create a lookup flow")
            logger.info("    1. {}", "x = Flow('name')")
            logger.info("    2. {}", "table = x.read_from($ds.$source_table)")
            logger.info("    3. {}", "table.lookup($ds.$table1, path='user', type=dict, relation=[['user_id', 'user_id']], filter='user_id > 1', fields=['user_id', 'user_name'])")
            logger.notice("{}", "- store flow to database")
            logger.info("    1. {}", "x = Flow('name')")
            logger.info("    2. {}", "x.read_from($ds.$source_table)")
            logger.info("    3. {}", "x.write_to($ds.$sink_table)")
            logger.info("    4. {}", "x.start()")

        def h_datasource(l):
            properties = client_cache["connectors"][l].get("properties", {})
            requires = []
            optionals = []
            for k, v in properties.items():
                if k == "OPTIONAL_FIELDS":
                    continue
                if v.get("required"):
                    requires.append([k, v])
                else:
                    optionals.append([k, v])
            if len(requires) > 0:
                print("required config:")
                for r in requires:
                    desc = ": " + r[1].get("apiServerKey", "")
                    logger.info("    {}" + desc, r[0])
            if len(optionals) > 0:
                print("optional config:")
                for r in optionals:
                    desc = ": " + r[1].get("apiServerKey", "")
                    logger.info("    {}" + desc, r[0])


        if line == "":
            h_command()
        if line.lower() in client_cache["connectors"]:
            h_datasource(line.lower())

    def _get_table(self, line):
        connection_id = client_cache.get("connection")
        if connection_id is None:
            logger.warn("no datasource set, please use 'use $datasource_name' to set a valid datasource")
            return None, None
        if client_cache.get("connections") is None:
            show_connections(quiet=True)
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
        connection = get_signature_v("datasource", connection_id)
        return table, connection
    
    def _peek(self, line, connection, table):
        connection_id = client_cache.get("connection")
        capabilities = connection.get("capabilities", [])
        support_peek = False
        for capability in capabilities:
            if capability.get("id") == "query_by_advance_filter_function":
                support_peek = True
                break

        if not support_peek:
            logger.warn("datasource {} not support peek", line)
            return False
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
        return res


    @line_magic
    def peek(self, line):
        """
        peek some records from table, support -n to specify the number of records to peek
        """
        connection_id = client_cache.get("connection")
        if line == "":
            logger.warn("no peek datasource found")
            return
        elif connection_id is None:
            logger.warn("no datasource set, please use 'use $datasource_name' to set a valid datasource")
            return
        # parse line
        try:
            if "-n" in line:
                line_split = line.split(" ")
                limit = int(line_split[line_split.index("-n") + 1])
                line = "".join(set(line_split) - set([str(limit), "-n"]))
            else:
                limit = 5
        except Exception as e:
            limit = 5
        table, connection = self._get_table(line)
        table_name = table["original_name"]
        res = self._peek(line, connection, table)
        if res is False:
            return
        try:
            count = res["data"].get("tableInfo", {}).get("numOfRows", 0)
            logger.info("table {} has {} records", table_name, count)
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
    def count(self, line):
        connection_id = client_cache.get("connection")
        if line == "":
            logger.warn("no peek datasource found")
            return
        elif connection_id is None:
            logger.warn("no datasource set, please use 'use $datasource_name' to set a valid datasource")
            return
        table, connection = self._get_table(line)
        res = self._peek(line, connection, table)
        if res is False:
            return
        try:
            count = res["data"].get("tableInfo", {}).get("numOfRows", 0)
            logger.info("table {} has {} records", table["original_name"], count)
        except Exception as e:
            pass


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

def select_secrets_source():
    """select secrets source in terminal"""
    print("""Tap Flow requires TapData Live Data Platform(LDP) cluster to run. 
If you would like to use with TapData Enterprise or TapData Community, type L to continue. 
If you would like to use TapData Cloud, or you are new to TapData, type C or press ENTER to continue.""")
    prompt_message = "Please type L or C (L/[C]): "
    while True:
        select_option = input(prompt_message)
        if select_option in ["L", "C", ""]:
            return select_option if select_option != "" else "C"
        print("Invalid input, please try again\n")

def set_server_and_access_code():
    server = input("Please enter server:port of TapData LDP server: ")
    access_code = getpass.getpass("Please enter access code: ")
    return server, access_code


def set_ak_sk():
    print("You may obtain the keys by log onto TapData Cloud, and click: 'User Center' on the top right, then copy & paste the accesskey and secret key pair.")
    ak = getpass.getpass("Enter AK: ")
    sk = getpass.getpass("Enter SK: ")
    return ak, sk


def create_sample_config(server="", access_code="", ak="", sk=""):
    if not os.path.exists("etc"):
        os.mkdir("etc")
    with open("etc/config.ini", "w") as f:
        f.write(f'''
[backend]
# If you are using Tapdata Cloud, please provide the access key and secret key(ak & sk).
# You may obtain the keys by log onto Tapdata Cloud, and click "User Center" on the top right, then copy & paste the access key and secret key pair.
# You can sign up for a new account from: https://cloud.tapdata.io if you don't have one
{f"ak = {ak}" if ak else "# ak = "}
{f"sk = {sk}" if sk else "# sk = "}

# If you are using TapData Enterprise, please specify the server URL & access token.
{f"server = {server}" if server else "# server = "}
{f"access_code = {access_code}" if access_code else "# access_code = "}
''')

def show_register():
    print("\n")
    logger.warn("{}", "no valid config file found, you can config etc/config.ini from sample file")
    with open("etc/config.ini", "r") as f:
        print(f.read())

def _set_secrets():
    ini_config = "etc/config.ini"
    """get secrets from config.ini or set in terminal"""
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
            login_with_ak_sk(ak, sk, server=server)
            show_agents(quiet=False)
        else:
            if server and access_token:
                login_with_access_code(server, access_token)
            else:
                show_register()
                os._exit(-1)
    else:
        select_option = select_secrets_source()
        if select_option == "L":
            server, access_code = set_server_and_access_code()
            create_sample_config(server=server, access_code=access_code)
            login_with_access_code(server, access_code)
        elif select_option == "C":
            ak, sk = set_ak_sk()
            create_sample_config(ak=ak, sk=sk)
            login_with_ak_sk(ak, sk)
            show_agents(quiet=False)


def get_default_sink():
    res = req.get("/mdb-instance-assigned")
    if not res.status_code == 200 or not res.json().get("code") == "ok":
        res = req.post("/mdb-instance-assigned/connection")
        show_connections(quiet=True)
        if not res.status_code == 200 or not res.json().get("code") == "ok":
            logger.fwarn("{}", "Failed to create default sink")
            return
    connection_id = res.json().get("data", {}).get("connectionId")
    default_connection_name = client_cache["connections"]["id_index"][connection_id]["name"]
    global DEFAULT_SINK
    DEFAULT_SINK = Sink(default_connection_name)
    client_cache["default_sink"] = DEFAULT_SINK


def main():
    # ipython settings
    ip = TerminalInteractiveShell.instance()
    ip.register_magics(show_command)
    ip.register_magics(op_object_command)
    ip.register_magics(ApiCommand)
    _set_secrets()
    globals().update(show_connections(quiet=True))
    show_connectors(quiet=True)
    show_jobs(quiet=True)
    if req.mode == "cloud":
        get_default_sink()


if __name__ == "__main__":
    main()
