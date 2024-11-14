import unittest
from unittest.mock import patch, Mock
from tapflow.lib.data_pipeline.nodes.sink import Sink
from tapflow.tests.test_data_pipeline.test_nodes.mock_utils import (
    mock_req_get_for_metadata_instances,
    mock_req_get_for_metadata_instance_details,
    mock_req_post_for_metadata_v3
)

class TestSink(unittest.TestCase):
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

    def create_sink_node(self, mock_client_cache, mock_get_connection_and_table, mock_get, mock_post, table="test_table"):
        self.initialize_client_cache(mock_client_cache)

        # 模拟_get_connection_and_table方法
        mock_get_connection_and_table.return_value = (self.mock_connection_instance, table)

        # 模拟API调用
        mock_post.return_value = mock_req_post_for_metadata_v3("MongoDB", table, self.fields)
        
        # 确保mock_get有足够的返回值
        mock_get.return_value = Mock()
        mock_get.return_value.json.return_value = {
            "data": {
                "items": [
                    {
                        "id": "table_id_123",
                        "original_name": "test_table"
                    }
                ],
                "fields": [
                    {"field_name": "id", "primaryKey": True},
                    {"field_name": "name", "primaryKey": False}
                ]
            }
        }

        return Sink(connection=self.mock_connection_instance, table=table)

    @patch('tapflow.lib.data_pipeline.nodes.source.req.post')
    @patch('tapflow.lib.data_pipeline.nodes.source.req.get')
    @patch('tapflow.lib.data_pipeline.nodes.source.BaseNode._get_connection_and_table')
    @patch('tapflow.lib.data_pipeline.nodes.source.client_cache', new_callable=dict)
    def test_initialization(self, mock_client_cache, mock_get_connection_and_table, mock_get, mock_post):
        sink_node = self.create_sink_node(mock_client_cache, mock_get_connection_and_table, mock_get, mock_post)
        
        # 验证基本属性
        self.assertEqual(sink_node.connectionId, "conn_id_123")
        self.assertEqual(sink_node.databaseType, "MongoDB")
        self.assertEqual(sink_node.name, "Test Connection")
        self.assertEqual(sink_node.table, "test_table")

        # 验证Sink特有的配置
        self.assertFalse(sink_node.setting["nodeConfig"]["syncIndex"])
        self.assertFalse(sink_node.setting["nodeConfig"]["enableSaveDeleteData"])
        self.assertFalse(sink_node.setting["nodeConfig"]["shardCollection"])
        self.assertFalse(sink_node.setting["nodeConfig"]["hashSplit"])
        self.assertEqual(sink_node.setting["nodeConfig"]["maxSplit"], 32)

    @patch('tapflow.lib.data_pipeline.nodes.source.req.post')
    @patch('tapflow.lib.data_pipeline.nodes.source.req.get')
    @patch('tapflow.lib.data_pipeline.nodes.source.BaseNode._get_connection_and_table')
    @patch('tapflow.lib.data_pipeline.nodes.source.client_cache', new_callable=dict)
    def test_keep_data(self, mock_client_cache, mock_get_connection_and_table, mock_get, mock_post):
        sink_node = self.create_sink_node(mock_client_cache, mock_get_connection_and_table, mock_get, mock_post)
        sink_node.keep_data()
        self.assertEqual(sink_node.setting["existDataProcessMode"], "keepData")

    @patch('tapflow.lib.data_pipeline.nodes.source.req.post')
    @patch('tapflow.lib.data_pipeline.nodes.source.req.get')
    @patch('tapflow.lib.data_pipeline.nodes.source.BaseNode._get_connection_and_table')
    @patch('tapflow.lib.data_pipeline.nodes.source.client_cache', new_callable=dict)
    def test_clean_data(self, mock_client_cache, mock_get_connection_and_table, mock_get, mock_post):
        sink_node = self.create_sink_node(mock_client_cache, mock_get_connection_and_table, mock_get, mock_post)
        sink_node.clean_data()
        self.assertEqual(sink_node.setting["existDataProcessMode"], "removeData")

    @patch('tapflow.lib.data_pipeline.nodes.source.req.post')
    @patch('tapflow.lib.data_pipeline.nodes.source.req.get')
    @patch('tapflow.lib.data_pipeline.nodes.source.BaseNode._get_connection_and_table')
    @patch('tapflow.lib.data_pipeline.nodes.source.client_cache', new_callable=dict)
    def test_clean_table(self, mock_client_cache, mock_get_connection_and_table, mock_get, mock_post):
        sink_node = self.create_sink_node(mock_client_cache, mock_get_connection_and_table, mock_get, mock_post)
        sink_node.clean_table()
        self.assertEqual(sink_node.setting["existDataProcessMode"], "dropTable")

    @patch('tapflow.lib.data_pipeline.nodes.source.req.post')
    @patch('tapflow.lib.data_pipeline.nodes.source.req.get')
    @patch('tapflow.lib.data_pipeline.nodes.source.BaseNode._get_connection_and_table')
    @patch('tapflow.lib.data_pipeline.nodes.source.client_cache', new_callable=dict)
    def test_set_cdc_threads(self, mock_client_cache, mock_get_connection_and_table, mock_get, mock_post):
        sink_node = self.create_sink_node(mock_client_cache, mock_get_connection_and_table, mock_get, mock_post)
        
        # 测试单线程
        sink_node.set_cdc_threads(1)
        self.assertFalse(sink_node.setting["cdcConcurrent"])
        self.assertEqual(sink_node.setting["cdcConcurrentWriteNum"], 1)

        # 测试多线程
        sink_node.set_cdc_threads(4)
        self.assertTrue(sink_node.setting["cdcConcurrent"])
        self.assertEqual(sink_node.setting["cdcConcurrentWriteNum"], 4)

    @patch('tapflow.lib.data_pipeline.nodes.source.req.post')
    @patch('tapflow.lib.data_pipeline.nodes.source.req.get')
    @patch('tapflow.lib.data_pipeline.nodes.source.BaseNode._get_connection_and_table')
    @patch('tapflow.lib.data_pipeline.nodes.source.client_cache', new_callable=dict)
    def test_set_init_thread(self, mock_client_cache, mock_get_connection_and_table, mock_get, mock_post):
        sink_node = self.create_sink_node(mock_client_cache, mock_get_connection_and_table, mock_get, mock_post)
        
        # 测试单线程
        sink_node.set_init_thread(1)
        self.assertFalse(sink_node.setting["initialConcurrent"])
        self.assertEqual(sink_node.setting["initialConcurrentWriteNum"], 1)

        # 测试多线程
        sink_node.set_init_thread(4)
        self.assertTrue(sink_node.setting["initialConcurrent"])
        self.assertEqual(sink_node.setting["initialConcurrentWriteNum"], 4)

    @patch('tapflow.lib.data_pipeline.nodes.source.req.post')
    @patch('tapflow.lib.data_pipeline.nodes.source.req.get')
    @patch('tapflow.lib.data_pipeline.nodes.source.BaseNode._get_connection_and_table')
    @patch('tapflow.lib.data_pipeline.nodes.source.client_cache', new_callable=dict)
    def test_set_write_batch(self, mock_client_cache, mock_get_connection_and_table, mock_get, mock_post):
        sink_node = self.create_sink_node(mock_client_cache, mock_get_connection_and_table, mock_get, mock_post)
        sink_node.set_write_batch(1000)
        self.assertEqual(sink_node.setting["writeBatchSize"], 1000)

    @patch('tapflow.lib.data_pipeline.nodes.source.req.post')
    @patch('tapflow.lib.data_pipeline.nodes.source.req.get')
    @patch('tapflow.lib.data_pipeline.nodes.source.BaseNode._get_connection_and_table')
    @patch('tapflow.lib.data_pipeline.nodes.source.client_cache', new_callable=dict)
    def test_set_write_wait(self, mock_client_cache, mock_get_connection_and_table, mock_get, mock_post):
        sink_node = self.create_sink_node(mock_client_cache, mock_get_connection_and_table, mock_get, mock_post)
        sink_node.set_write_wait(1000)
        self.assertEqual(sink_node.setting["writeBatchWaitMs"], 1000)

if __name__ == '__main__':
    unittest.main() 