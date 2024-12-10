import uuid
import json

from tapflow.lib.backend_apis.metadataInstance import MetadataInstanceApi
from tapflow.lib.help_decorator import help_decorate
from tapflow.lib.params.job import node_config, node_config_sync
from tapflow.lib.utils.log import logger
from tapflow.lib.check import ConfigCheck

from tapflow.lib.data_pipeline.job import JobType
from tapflow.lib.op_object import QuickDataSourceMigrateJob
from tapflow.lib.op_object import get_index_type, match_line
from tapflow.lib.connections.connection import Connection, show_connections, show_tables
from tapflow.lib.cache import client_cache
from tapflow.lib.request import req


@help_decorate("Enum, used to describe write mode for a row")
class WriteMode:
    updateOrInsert = "updateOrInsert"
    updateWrite = "updateWrite"
    updateIntoArray = "updateIntoArray"


upsert = "updateOrInsert"
update = "updateWrite"


@help_decorate("Enum, used to config job type")
class SyncType:
    initial = "initial_sync"
    cdc = "cdc"
    both = "initial_sync+cdc"


@help_decorate("Enum, used to config action before sync data")
class DropType:
    drop_table = "dropTable"
    remove_data = "removeData"
    keep_data = "keepData"


no_drop = "no_drop"
drop_data = "drop_data"
drop_schema = "drop_schema"


class FilterType:
    keep = "keep"
    delete = "delete"

class FilterMode:
    keep = "keep"
    delete = "delete"


class BaseNode:
    @help_decorate("__init__ method", args="connection, table, sql")
    def __init__(self, connection, table=None, table_re=None, mode=JobType.migrate):
        self.mode = mode
        self.id = str(uuid.uuid4())
        self.connection, self.table = self._get_connection_and_table(connection, table, table_re)
        self.tableName = self.table
        self.table_name = self.table
        self.connectionId = str(self.connection.c["id"])
        self.databaseType = self.connection.c["database_type"]
        self.config_type = node_config if mode == JobType.migrate else node_config_sync
        client_cache["connection"] = self.connectionId
        self.name = self.connection.c["name"]

        self.source = None
        self.setting = {
            "connectionId": self.connectionId,
            "databaseType": self.databaseType,
            "id": self.id,
            "name": self.connection.c["name"],
            "attrs": {
                "connectionType": self.connection.c["connection_type"],
                "position": [0, 0],
                "pdkType": "pdk",
                "pdkHash": self.connection.c["pdkHash"],
                "capabilities": self.connection.c["capabilities"],
                "connectionName": self.connection.c["name"]
            },
            "type": "database" if self.mode == "migrate" else "table",
            "tableNames": self.table,
            "dmlPolicy": {
                "insertPolicy": "update_on_exists",
                "updatePolicy": "ignore_on_nonexists",
            }
        }

    def _get_connection_and_table(self, connection, table, table_re):
        if isinstance(connection, QuickDataSourceMigrateJob):
            connection = connection.__db__
        if client_cache.get("connections") is None:
            show_connections(quiet=True)

        # get connection
        if not isinstance(connection, Connection):
            index_type = get_index_type(connection)
            if index_type == "short_id_index":
                connection = match_line(client_cache["connections"]["id_index"], connection)
                index_type = "id_index"
            if index_type == "name_index" and "." in connection:
                connection_and_table = connection.split(".")
                connection = connection_and_table[0]
                table = connection_and_table[1]
            c = client_cache["connections"][index_type][connection]
            connection = Connection(id=c["id"])

        # select all tables default if table not provide (only migrate mode)
        if table is None and self.mode == JobType.migrate:
            if c["id"] not in client_cache["tables"]:
                show_tables(source=connection.id, quiet=True)
            table = list(client_cache["tables"][c["id"]]["name_index"].keys())
        # select first table if table not provide (only sync mode)
        if table is None and self.mode == JobType.sync:
            if c["id"] not in client_cache["tables"]:
                show_tables(source=connection.id, quiet=True)
            try:
                table = list(client_cache["tables"][c["id"]]["name_index"].keys())[0]
            except IndexError:
                logger.ferror("Source {} no table", c["name"])
                return None, None

        # filter table_re if table_re provide
        if table_re is not None and self.mode == JobType.migrate:
            tables = []
            all_tables = show_tables(source=connection.id, quiet=True)
            import re
            for t in all_tables:
                if "original_name" not in t:
                    continue
                if re.match(table_re, t["original_name"]) and t["original_name"] not in tables:
                    tables.append(t["original_name"])
            table = tables
        return connection, table

    def to_dict(self):
        self.config({})
        return self.setting

    def config(self, config: dict = None, keep_extra=True):
        if not isinstance(config, dict):
            logger.fwarn("type {} must be {}", config, "dict", "notice", "notice")
            return False
        self.setting.update(config)
        resp = ConfigCheck(self.setting, self.config_type[type(self).__name__.lower()],
                           keep_extra=keep_extra).checked_config
        self.setting.update(resp)
        return True

    def update_node_config(self, config: dict):
        if "nodeConfig" not in self.setting:
            self.setting["nodeConfig"] = {}
        self.setting["nodeConfig"].update(config)

    def test(self):
        self.connection.test()

    def _getTableId(self, tableName):
        table_id = MetadataInstanceApi(req).get_table_id(tableName, self.connectionId)
        if table_id is not None:
            self.primary_key = []
            fields = MetadataInstanceApi(req).get_fields_instance_by_id(table_id)
            for field in fields:
                if field.get("primaryKey", False):
                    self.primary_key.append(field["field_name"])
        return table_id

    @help_decorate("get cache job status")
    def cache_status(self):
        self.cache_p.status()
