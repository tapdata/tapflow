import unittest
from unittest.mock import patch, Mock
from tapflow.lib.data_pipeline.nodes.source import Source
from tapflow.tests.test_data_pipeline.test_nodes.mock_utils import (
    mock_req_get_for_metadata_instances,
    mock_req_get_for_metadata_instance_details,
    mock_req_post_for_metadata_v3
)

class TestSource(unittest.TestCase):
    def setUp(self):
        # 初始化通用的mock对象
        self.mock_connection_instance = Mock()
        self.mock_connection_instance.c = {
            "id": "conn_id_123",
            "database_type": "MongoDB",
            "name": "Test Connection",
            "connection_type": "test_type",
            "pdkHash": "hash123",
            "capabilities": {}
        }
        self.fields = {
            "_id": {
                "dataType": "STRING(120)",
                "nullable": True,
                "name": "_id",
                "primaryKey": True
            },
            "SETTLED_DATE": {
                "dataType": "DATE_TIME",
                "nullable": True,
                "name": "SETTLED_DATE",
                "primaryKey": False
            }
        }

    def initialize_client_cache(self, mock_client_cache):
        # 初始化client_cache
        mock_client_cache["connections"] = {
            "id_index": {"conn_id_123": {"id": "conn_id_123", "name": "Test Connection"}}
        }
        mock_client_cache["tables"] = {
            "conn_id_123": {
                "name_index": {"test_table": {}}
            }
        }

    def create_source_node(self, mock_client_cache, mock_get_connection_and_table, mock_get, mock_post, table="test_table"):
        self.initialize_client_cache(mock_client_cache)

        # 模拟_get_connection_and_table方法
        mock_get_connection_and_table.return_value = (self.mock_connection_instance, table)

        # 模拟API调用
        mock_post.return_value = mock_req_post_for_metadata_v3("MongoDB", table, self.fields)
        mock_get.side_effect = [
            mock_req_get_for_metadata_instances(),
            mock_req_get_for_metadata_instance_details()
        ]

        return Source(connection=self.mock_connection_instance, table=table)

    @patch('tapflow.lib.data_pipeline.nodes.source.show_tables')
    @patch('tapflow.lib.data_pipeline.nodes.source.req.post')
    @patch('tapflow.lib.data_pipeline.nodes.source.req.get')
    @patch('tapflow.lib.data_pipeline.nodes.source.BaseNode._get_connection_and_table')
    @patch('tapflow.lib.data_pipeline.nodes.source.client_cache', new_callable=dict)
    def test_exists(self, mock_client_cache, mock_get_connection_and_table, mock_get, mock_post, mock_show_tables):
        self.initialize_client_cache(mock_client_cache)

        # 模拟show_tables方法，直接返回None
        mock_show_tables.return_value = None

        # 模拟_get_connection_and_table方法
        mock_get_connection_and_table.return_value = (self.mock_connection_instance, "test_table")

        # 模拟API调用
        mock_post.return_value = mock_req_post_for_metadata_v3("MongoDB", "test_table", self.fields)
        mock_get.side_effect = [
            mock_req_get_for_metadata_instances(),
            mock_req_get_for_metadata_instance_details()
        ]

        source_node = Source(connection=self.mock_connection_instance, table="test_table")
        self.assertTrue(source_node.exists())

if __name__ == '__main__':
    unittest.main() 