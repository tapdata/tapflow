import requests

from tapshell.tapdata_cli.log import logger

from auto_test.tapdata.data_pipeline.base_obj import BaseObj
from auto_test.init.global_vars import system_server_conf


class TypeAdjust(BaseObj):
    """
    sync mode node, update field type. Save pipeline first and add "filed type operation" by this class.
    """
    node_name = "类型修改"
    node_type = "field_mod_type_processor"

    def __init__(self):
        self.fields = {}
        self.convert_field = {}
        self._convert_field = []
        self.pre_connection_id = None
        self.pre_table_name = None
        self.pk_position = 0
        super().__init__()

    def get(self, pre_connection_id, pre_table_name):
        # 获取table id
        params = {
            "access_token": system_server_conf["token"],
            "connectionId": pre_connection_id,
        }
        res = requests.get(f"http://{server}/api/MetadataInstances/tablesValue", params=params)
        if res.status_code != 200:
            logger.error("get table id failed, err is: {}", res.json())
            return False
        elif res.json()["code"] != "ok":
            logger.error("get table id failed, err is: {}", res.json())
            return False
        table_id = None
        for t in res.json()["data"]:
            if t["tableName"] == pre_table_name:
                table_id = t["tableId"]
        if table_id is None:
            logger.error("table {} not found", pre_table_name)
            return False
        # 根据table id获取字段
        res = requests.get(f"http://{server}/api/discovery/storage/overview/{table_id}", params={
            "access_token": system_server_conf["token"]
        })
        if res.status_code != 200:
            logger.error("get table fields failed, err is: {}", res.json())
            return False
        elif res.json()["code"] != "ok":
            logger.error("get table fields failed, err is: {}", res.json())
            return False
        data = res.json()["data"]
        for field in data["fields"]:
            self.fields[field["name"]] = field
        self.pre_connection_id, self.pre_table_name = pre_connection_id, pre_table_name
        return True

    def convert(self, field, field_type):
        self._convert_field.append((field, field_type))

    def _convert(self, field, field_type):
        field_value = self.fields.get(field)
        if field_value is None:
            logger.fwarn("field {} not found", field)
            return False
        if field_value["primaryKey"]:
            self.pk_position += 1
        self.convert_field[field] = {
            "field": field,
            "field_name": field,
            "id": f"{self.pre_connection_id}_{self.pre_table_name}_{field}",
            "label": field,
            "op": "CONVERT",
            "operand": field_type,
            "originalDataType": "bigint(20) unsigned",
            "primary_key_position": self.pk_position,
            "table_name": "table",
            "type": field_type,
        }
        return True

    def to_dict(self):
        for field, field_type in self._convert_field:
            self._convert(field, field_type)
        result = {
            "id": self.id,
            "name": self.node_name,
            "type": self.node_type,
        }
        if len(self.convert_field) != 0:
            result.update({
                "operations": list(self.convert_field.values()),
            })
        return result

