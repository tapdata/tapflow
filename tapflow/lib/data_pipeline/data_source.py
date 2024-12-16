import copy
import json
import time
import traceback

from requests import delete

from tapflow.lib.backend_apis.common import AgentApi
from tapflow.lib.backend_apis.dataSource import DataSourceApi
from tapflow.lib.utils.log import logger
from tapflow.lib.check import ConfigCheck
from tapflow.lib.help_decorator import help_decorate
from tapflow.lib.params.datasource import pdk_config, DATASOURCE_CONFIG

from tapflow.lib.cache import client_cache
from tapflow.lib.system.ext_storage import get_default_external_storage_id
from tapflow.lib.request import req
from tapflow.lib.utils.datasource_field_map import reverse_datasource_field_map


@help_decorate("Data Source, you can see it as database",
               'ds = DataSource("mysql", "mysql-datasource").host("127.0.0.1").port(3306).username().password().db()')
class DataSource:
    def __init__(self, connector="", name=None, config=None, type="source_and_target", id=None):
        """
        @param connector: pdkType
        @param name: datasource name
        @param config: datasource config
        @param type: datasource can be used as source and target at the same time
        @param id: datasource id, it will get datasource config from backend by api if id provide
        """
        self.config_changed = True
        self.pdk_setting = {}
        self.setting = {}
        self.datasource_api = DataSourceApi(req)
        # get datasource config
        if id is not None:
            self.id = id
            self.setting = self.get(connector_id=id)
            if self.setting is not None:
                self.pdk_setting = self.setting.get("config")
            return
        # name is not provide
        name = connector if connector != "" and name is None else name
        # if name is already exists
        from tapflow.lib.op_object import get_obj
        obj = get_obj("datasource", name)
        if obj is not None:
            exists_database_type = obj.setting.get("database_type")
            if exists_database_type.lower() != connector.lower():
                logger.warn("database {} exists but type {} is different with {}, will not create datasource", name, exists_database_type, connector)
                return
            else:
                logger.info("database {} exists, will update it's config", name)
            self.id = obj.id
            self.setting = obj.setting
            self.pdk_setting = obj.setting.get("config")
        else:
            self.id = id
            self.connection_type = type
            self.setting = {
                "name": name,
                "database_type": client_cache["connectors"][connector]["name"],
                "connection_type": self.connection_type
            }
        if config is not None:
            for key, value in config.items():
                if key == "db" and value:
                    self.pdk_setting.update({"database": value})
                if key == "uri" and value:
                    self.pdk_setting.update({"isUri": True, key: value})
                if key == "type" and value:
                    self.setting["connection_type"] = value
                self.pdk_setting.update({key: value})

    def _set_pdk_setting(self, key):
        """closure function to update value into pdk_setting, only be called by __getattribute__
        """
        def _config_pdk_setting(value):
            if key == "db" and value:
                self.pdk_setting.update({"database": value})
                return self
            if key == "uri" and value:
                self.pdk_setting.update({"isUri": True, key: value})
                return self
            if key == "type" and value:
                self.setting["connection_type"] = value
                return self
            if reverse_datasource_field_map.get(key, None):
                self.pdk_setting[reverse_datasource_field_map[key]] = value
                return self
            self.pdk_setting.update({key: value})
            return self

        return _config_pdk_setting

    def set(self, config):
        if "shareCDCExternalStorageId" in config and config["shareCDCExternalStorageId"] == "default":
            config["shareCDCExternalStorageId"] = get_default_external_storage_id()
        self.setting.update(config)
        return self

    def __getattribute__(self, item):
        """
        1. if item is attribute of self, return
        2. if not, return _config_pdk_setting to set pdk_setting
        """

        try:
            if reverse_datasource_field_map.get(item, None):
                item = reverse_datasource_field_map[item]
            return object.__getattribute__(self, item)
        except AttributeError:
            return self._set_pdk_setting(item)

    @staticmethod
    @help_decorate("static method, used to list all datasources", res="datasource list, list")
    def list():
        return DataSourceApi(req).get_all_data_sources_ignore_error()

    @help_decorate("desc a datasource, display readable struct", res="datasource struct")
    def desc(self, quiet=True):
        c = copy.deepcopy(self.setting)
        remove_keys = [
            "response_body",
            "user_id",
            "id",
            "transformed",
            "schemaVersion",
            "schema",
            "username",
            "everLoadSchema",
            "isUrl",
        ]
        # remove field hard to understand
        for k, v in c.items():
            if not v:
                remove_keys.append(k)

        for k in remove_keys:
            if k in c:
                del (c[k])

        if not quiet:
            print(json.dumps(c, indent=4))

        return c

    @help_decorate("get a datasource status", "")
    def status(self, quiet=False):
        if self.id is None:
            logger.fwarn("datasource is not save, please save first")
            return
        info = self.get(self.id)
        status = info.get("status")
        tableCount = info.get("tableCount", "unknown")
        loadCount = info.get("loadCount", 0)
        loadFieldsStatus = info.get("loadFieldsStatus", False)
        loadSchemaDate = info.get("loadSchemaDate", "unknown")
        if not quiet:
            pass
            #logger.finfo("datasource {} status is: {}, it has {} tables, loaded {}, last load time is: {}",
            #            info.get("name"), status, tableCount, loadCount, loadSchemaDate)
        return status

    def to_dict(self):
        """
        1. get datasource config by connector
        2. check the settings by ConfigCheck
        3. add pdk_setting
        """
        # get database_type check rules
        self.setting.update({"pdkHash": self._get_pdkHash()})
        res = ConfigCheck(self.setting, DATASOURCE_CONFIG, keep_extra=True).checked_config
        self.setting.update(res)
        self.setting.update({"config": self.to_pdk_dict()})
        return self.setting

    def _get_pdkHash(self):
        connector = client_cache["connectors"][self.setting["database_type"].lower()]
        return connector["pdkHash"]

    def to_pdk_dict(self):
        """
        1. get datasource config by connector
        2. check the settings by ConfigCheck
        """
        mode = "uri" if self.pdk_setting.get("isUri") else "form"
        # get database_config
        database_type = self.setting["database_type"].lower()
        if pdk_config.get(database_type):
            param_config = pdk_config.get(database_type)[mode]
            res = ConfigCheck(self.pdk_setting, param_config, keep_extra=True).checked_config
            self.pdk_setting.update(res)
            self.pdk_setting["__connectionType"] = self.setting["connection_type"]
        return self.pdk_setting

    @staticmethod
    @help_decorate("get a datasource by it's id or name", args="id or name, using kargs", res="a DataSource Object")
    def get(connector_id=None, connector_name=None):
        if connector_id is not None:
            data, ok = DataSourceApi(req).get_data_source(connector_id)
            if ok:
                return data
        elif connector_name is not None:
            data, ok = DataSourceApi(req).filter_data_sources_ignore_error(connector_name)
            if ok:
                return data["items"][0]
        return None

    def save(self):
        # check agent running
        if req.mode == "cloud":
            agent_count, ok = AgentApi(req).get_agent_count()
            if not ok:
                logger.warn("Failed to check agent running, err is: {}", agent_count)
                return False
            if agent_count == 0:
                logger.warn("{}", "No agent running, please check agent status")
                return False
        if not self.config_changed:
            logger.finfo("datasource {} config not changed, no need to save", self.setting.get("name"))
            return True
        data = self.to_dict()
        if data.get("id") is not None:
            data, ok = DataSourceApi(req).update_data_source(data)
        else:
            logger.info("datasource {} creating, please wait...", self.setting.get("name"))
            data, ok = DataSourceApi(req).create_data_source(data)

        from tapflow.lib.op_object import show_connections
        show_connections(quiet=True)
        if ok:
            self.id = data["id"]
            self.setting = DataSource.get(self.id)
            logger.info("save datasource {} success, will load schema, please wait...", self.setting.get("name"))
            self.validate(quiet=False, load_schema=True)
            return True
        else:
            self.validate(quiet=False, load_schema=True)
            logger.fwarn("save Connection fail, err is: {}", data)
        return False

    def delete(self):
        if self.id is None:
            return False
        data, ok = DataSourceApi(req).delete_data_source(self.id)
        if ok:
            #logger.finfo("delete {} Connection success", self.id)
            return True
        else:
            logger.fwarn("delete Connection fail, err is: {}", data)
        return False

    @help_decorate("validate this datasource")
    def validate(self, quiet=False, load_schema=False):
        res = True
        # save 的时候后端会自动加载模型，没有必要再通过WS再触发一次；直接通过http验证Connection进度和状态即可。
        # 这里的WebSocket可能还有点问题，有些connection的Schema状态会是Loading Error；注释掉这段代码反而都正常了。
        # async def load():
        #     async with websockets.connect(gen_ws_uri_with_id()) as websocket:
        #         data = self.to_dict()
        #         data["updateSchema"] = True
        #         if isinstance(self.id, str):
        #             data.update({
        #                 "id": self.id,
        #             })
        #         payload = {
        #             "type": "testConnection",
        #             "data": data,
        #             "updateSchema": load_schema,
        #         }
        #         #logger.finfo("start validate datasource config, please wait for a while ...")
        #         await websocket.send(json.dumps(payload))
        #
        #         while True:
        #             recv = await websocket.recv()
        #             loadResult = json.loads(recv)
        #             if "type" not in loadResult:
        #                 continue
        #             if loadResult["type"] != "pipe":
        #                 continue
        #             if loadResult["data"]["type"] != "testConnectionResult":
        #                 continue
        #             if loadResult["data"]["result"]["status"] is None:
        #                 continue
        #
        #             if loadResult["data"]["result"]["status"] != "ready":
        #                 res = False
        #             else:
        #                 res = True
        #
        #             if not quiet:
        #                 if loadResult["data"]["result"] is None:
        #                     continue
        #                 for detail in loadResult["data"]["result"]["response_body"]["validate_details"]:
        #                     if "fail_message" in detail and detail["fail_message"] is not None and "(build:" not in detail["fail_message"]:
        #                         logger.log("{}: {}, message: {}", detail["show_msg"], detail["status"],
        #                                    detail["fail_message"], "debug", "info",
        #                                    "info" if detail["status"] == "passed" else "warn")
        #                     else:
        #                         logger.log("{}: {}", detail["show_msg"], detail["status"], "debug", "info")
        #             await websocket.close()
        #             return res
        #
        # try:
        #     asyncio.run(load())
        # except Exception as e:
        #     logger.fwarn("load schema exception, err is: {}", e)
        #     return False
        # logger.finfo("datasource valid finished, will check table schema now, please wait for a while ...")
        start_time = time.time()
        if self.id is not None:
            for _ in range(96):
                try:
                    time.sleep(5)
                    data, ok = DataSourceApi(req).get_data_source(self.id)
                    if not ok:
                        logger.fwarn("No data on load schema response")
                        break
                    if data.get("loadFieldsStatus") in ["invalid", "finished", "error"]:
                        print(f"load schema status: {data.get('loadFieldsStatus')}")
                        break
                    if "loadFieldsStatus" not in data:
                        logger.fwarn("No loadFieldsStatus on load schema response data")
                        continue
                    loadCount = data.get("loadCount", 0)
                    tableCount = data.get("tableCount", 1)
                    #logger.finfo("table schema check percent is: {}%", int(loadCount / tableCount * 100), wrap=False)
                except Exception as e:
                    print(traceback.format_exc())
            #logger.finfo("datasource table schema check finished, cost time: {} seconds", int(time.time() - start_time))
            return res
