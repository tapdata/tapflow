import json
from .common import BaseBackendApi

class MetadataInstanceApi(BaseBackendApi):

    def get_metadata_instance(self, source_id: str) -> dict:
        """
        获取 source_id 的表格列表
        :param source_id: 源id
        :return: 表格列表
        """
        res = self.req.get("/MetadataInstances", params={
            "filter": json.dumps({"where": {"source.id": source_id, "sourceType": "SOURCE", "is_deleted": False}, "limit": 999999})
        })
        return res.json()["data"]["items"]
    
    def get_fields_instance_by_id(self, table_id: str) -> dict:
        """
        获取 表格id 的字段信息
        :param table_id: 表格id
        :return: 字段信息
        """
        res = self.req.get(f"/MetadataInstances/{table_id}")
        return res.json()["data"]["fields"]
    
    def get_table_id(self, table_name: str, source_id: str) -> str:
        """
        获取 表格名 的表格id
        :param table_name: 表格名
        :param source_id: 源id
        :return: 表格id
        """
        payload = {
            "where": {
                "source.id": source_id,
                "meta_type": {"in": ["collection", "table", "view"]},
                "is_deleted": False,
                "original_name": table_name
            },
            "fields": {"id": True, "original_name": True, "fields": True},
            "limit": 1
        }
        res = self.req.get("/MetadataInstances", params={"filter": json.dumps(payload)})
        table_id = None
        for s in res.json()["data"]["items"]:
            if s["original_name"] == table_name:
                table_id = s["id"]
                break
        return table_id
    
    def load_schema(self, node_id: str) -> dict:
        """
        获取 节点id 的 schema
        :param node_id: 节点id
        :return: schema
        """
        res = self.req.get(f"/MetadataInstances/node/schema", params={"nodeId": node_id})
        return res.json()["data"]
    
    def schema_page(self, node_id: str) -> dict:
        """
        获取 节点id 的 schema 分页
        :param node_id: 节点id
        :return: schema 分页
        """
        res = self.req.get(f"/MetadataInstances/node/schemaPage", params={"nodeId": node_id})
        return res.json()["data"]
    
    def get_table_metadata(self, connection_id: str, table_name: str) -> dict:
        """
        获取 表格id 的 metadata
        :param connection_id: 连接id
        :param table_name: 表格名
        :return: metadata
        """
        res = self.req.post("/MetadataInstances/metadata/v3", json={
            connection_id: {
                "metaType": "table",
                "tableNames": [table_name]
            }
        })
        res = res.json()
        meta = list(res["data"].items())[0][1][0]
        return meta
    
    def get_table_value(self, connection_id: str) -> dict:
        """
        获取 连接id 的 表格值
        :param connection_id: 连接id
        :return: 表格值
        """
        res = self.req.get(f"/MetadataInstances/tablesValue", params={"connectionId": connection_id})
        return res.json()["data"]
    
    def get_fields_value(self, table_id: str) -> list:
        """
        获取 表格id 的字段值
        :param table_id: 表格id
        :return: 字段值
        """
        res = self.req.get(f"/discovery/storage/overview/{table_id}")
        return res.json()["data"]["fields"]
