import unittest
from unittest.mock import patch, Mock
from tapflow.lib.data_pipeline.base_node import BaseNode
from tapflow.tests.test_data_pipeline.test_nodes.mock_utils import (
    mock_req_get_for_metadata_instances,
    mock_req_get_for_metadata_instance_details
)

class TestBaseNode(unittest.TestCase):
    def setUp(self):
        # 初始化通用的mock对象
        self.mock_connection_instance = Mock()
        self.mock_connection_instance.c = {
            "id": "conn_id_123",
            "database_type": "test_db",
            "name": "Test Connection",
            "connection_type": "test_type",
            "pdkHash": "hash123",
            "capabilities": {}
        }

    def initialize_client_cache(self, mock_client_cache):
        # 初始化client_cache
        mock_client_cache["connections"] = {
            "id_index": {"conn_id_123": {"id": "conn_id_123"}},
            "name_index": {"Test Connection": {"id": "conn_id_123"}}
        }
        mock_client_cache["tables"] = {
            "conn_id_123": {
                "name_index": {"test_table": {}}
            }
        }

    @patch('tapflow.lib.data_pipeline.base_node.req.get')
    @patch('tapflow.lib.data_pipeline.base_node.BaseNode._get_connection_and_table')
    @patch('tapflow.lib.data_pipeline.base_node.client_cache', new_callable=dict)
    def test_initialization(self, mock_client_cache, mock_get_connection_and_table, mock_get):
        self.initialize_client_cache(mock_client_cache)

        # 模拟_get_connection_and_table方法
        mock_get_connection_and_table.return_value = (self.mock_connection_instance, "test_table")

        # 模拟API调用
        mock_get.side_effect = [
            mock_req_get_for_metadata_instances(),
            mock_req_get_for_metadata_instance_details()
        ]

        base_node = BaseNode(connection=self.mock_connection_instance, table="test_table")
        
        self.assertEqual(base_node.connectionId, "conn_id_123")
        self.assertEqual(base_node.databaseType, "test_db")
        self.assertEqual(base_node.name, "Test Connection")
        self.assertEqual(base_node.table, "test_table")

    @patch('tapflow.lib.data_pipeline.base_node.req.get')
    @patch('tapflow.lib.data_pipeline.base_node.BaseNode._get_connection_and_table')
    @patch('tapflow.lib.data_pipeline.base_node.client_cache', new_callable=dict)
    def test_get_table_id(self, mock_client_cache, mock_get_connection_and_table, mock_get):
        self.initialize_client_cache(mock_client_cache)

        # 模拟_get_connection_and_table方法
        mock_get_connection_and_table.return_value = (self.mock_connection_instance, "test_table")

        # 模拟API调用
        mock_get.side_effect = [
            mock_req_get_for_metadata_instances(),
            mock_req_get_for_metadata_instance_details()
        ]

        base_node = BaseNode(connection=self.mock_connection_instance, table="test_table")
        table_id = base_node._getTableId("test_table")

        self.assertEqual(table_id, "table_id_123")
        self.assertIn("id", base_node.primary_key)
        self.assertNotIn("name", base_node.primary_key)

    @patch('tapflow.lib.data_pipeline.base_node.req.get')
    @patch('tapflow.lib.data_pipeline.base_node.BaseNode._get_connection_and_table')
    @patch('tapflow.lib.data_pipeline.base_node.ConfigCheck')
    @patch('tapflow.lib.data_pipeline.base_node.node_config', new_callable=dict)
    @patch('tapflow.lib.data_pipeline.base_node.client_cache', new_callable=dict)
    def test_config_method(self, mock_client_cache, mock_node_config, mock_config_check, mock_get_connection_and_table, mock_get):
        # 模拟ConfigCheck的返回值
        mock_config_check_instance = mock_config_check.return_value
        mock_config_check_instance.checked_config = {"checked_key": "checked_value"}

        self.initialize_client_cache(mock_client_cache)

        # 模拟_get_connection_and_table方法
        mock_get_connection_and_table.return_value = (self.mock_connection_instance, "test_table")

        # 模拟API调用
        mock_get.side_effect = [
            mock_req_get_for_metadata_instances(),
            mock_req_get_for_metadata_instance_details()
        ]

        # 模拟node_config
        mock_node_config['basenode'] = {}

        # 创建BaseNode实例
        base_node = BaseNode(connection=self.mock_connection_instance, table="test_table")

        # 调用config方法
        config_result = base_node.config({"new_key": "new_value"})

        # 验证config方法的行为
        self.assertTrue(config_result)
        self.assertIn("checked_key", base_node.setting)
        self.assertEqual(base_node.setting["checked_key"], "checked_value")
        self.assertIn("new_key", base_node.setting)
        self.assertEqual(base_node.setting["new_key"], "new_value")

    @patch('tapflow.lib.data_pipeline.base_node.req.get')
    @patch('tapflow.lib.data_pipeline.base_node.BaseNode._get_connection_and_table')
    @patch('tapflow.lib.data_pipeline.base_node.client_cache', new_callable=dict)
    def test_update_node_config(self, mock_client_cache, mock_get_connection_and_table, mock_get):
        self.initialize_client_cache(mock_client_cache)

        # 模拟_get_connection_and_table方法
        mock_get_connection_and_table.return_value = (self.mock_connection_instance, "test_table")

        # 模拟API调用
        mock_get.side_effect = [
            mock_req_get_for_metadata_instances(),
            mock_req_get_for_metadata_instance_details()
        ]

        # 创建BaseNode实例
        base_node = BaseNode(connection=self.mock_connection_instance, table="test_table")

        # 更新节点配置
        base_node.update_node_config({"config_key": "config_value"})

        # 验证节点配置的更新
        self.assertIn("nodeConfig", base_node.setting)
        self.assertIn("config_key", base_node.setting["nodeConfig"])
        self.assertEqual(base_node.setting["nodeConfig"]["config_key"], "config_value")

if __name__ == '__main__':
    unittest.main() 