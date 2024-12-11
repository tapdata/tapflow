from IPython.core.magic import Magics, magics_class, line_magic

from tapflow.lib.backend_apis.apiServers import ApiServersApi
from tapflow.lib.cache import client_cache
from tapflow.lib.connections.connection import get_table_fields
from tapflow.lib.request import req
from tapflow.lib.op_object import show_apis, show_connections
from tapflow.lib.utils.log import logger


@magics_class
class ApiCommand(Magics):
    @line_magic
    def unpublish(self, line):
        if len(client_cache["apis"]["name_index"]) == 0:
            show_apis()
        _, ok = ApiServersApi(req).unpublish(client_cache["apis"]["name_index"][line]["id"], client_cache["apis"]["name_index"][line]["table"])
        if ok:
            logger.info("unpublish {} success", line)
        else:
            logger.warn("unpublish {} fail", line)

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
        _, ok = ApiServersApi(req).publish(base_path, db, table, fields)
        if ok:
            pass
            # logger.info(
            #    "publish api {} success, you can test it by: {}",
            #    base_path,
            #    "http://" + server + "#/apiDocAndTest?id=" + base_path + "_v1"
            # )
        else:
            pass
            logger.warn("publish api {} fail, err is: {}", base_path, res["message"])
