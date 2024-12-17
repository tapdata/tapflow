import unittest
from unittest.mock import patch, Mock, call
from tapflow.lib.data_pipeline.dag import Dag
from tapflow.lib.data_pipeline.nodes.source import Source
from tapflow.lib.data_pipeline.nodes.sink import Sink
from tapflow.lib.data_pipeline.nodes.merge import MergeNode

class TestDag(unittest.TestCase):
    def setUp(self):
        self.dag = Dag(name="test_dag")
        # 创建测试用的节点
        self.source_node = Mock(spec=Source)
        self.source_node.id = "source_1"
        self.source_node.to_dict.return_value = {"id": "source_1", "type": "source"}
        self.source_node.stage = self.source_node  # 添加stage属性

        self.sink_node = Mock(spec=Sink)
        self.sink_node.id = "sink_1"
        self.sink_node.to_dict.return_value = {"id": "sink_1", "type": "sink"}
        self.sink_node.stage = self.sink_node  # 添加stage属性

        self.merge_node = Mock(spec=MergeNode)
        self.merge_node.id = "merge_1"
        self.merge_node.to_dict.return_value = {"id": "merge_1", "type": "merge"}
        self.merge_node.stage = self.merge_node  # 添加stage属性

        # 创建mock dag对象
        self.mock_source_dag = Mock()
        self.mock_source_dag.node_map = {}
        self.mock_source_dag.graph = {}
        self.mock_source_dag.to_dict.return_value = {"nodes": [], "edges": []}

        # 创建mock stage对象
        self.mock_source_stage = Mock()
        self.mock_source_stage.dag = self.mock_source_dag
        self.mock_source_stage.stage = self.source_node

    def test_initialization(self):
        self.assertEqual(self.dag.name, "test_dag")
        self.assertEqual(self.dag.dag["edges"], [])
        self.assertEqual(self.dag.dag["nodes"], [])
        self.assertEqual(self.dag.graph, {})
        self.assertEqual(self.dag.node_map, {})

    def test_add_node(self):
        self.dag.add_node(self.source_node)
        self.assertIn(self.source_node.id, self.dag.graph)
        self.assertEqual(self.dag.graph[self.source_node.id], [])
        self.assertEqual(self.dag.node_map[self.source_node.id], self.source_node)

    def test_add_edge(self):
        # 添加节点和边
        self.dag.add_node(self.source_node)
        self.dag.add_node(self.sink_node)
        self.dag.add_edge(self.source_node, self.sink_node)

        # 验证边是否正确添加
        self.assertIn(self.sink_node.id, self.dag.graph[self.source_node.id])
        self.assertEqual(len(self.dag.graph[self.source_node.id]), 1)

    def test_add_duplicate_edge(self):
        # 添加相同的边两次
        self.dag.add_node(self.source_node)
        self.dag.add_node(self.sink_node)
        self.dag.add_edge(self.source_node, self.sink_node)
        self.dag.add_edge(self.source_node, self.sink_node)

        # 验证边只被添加一次
        self.assertEqual(len(self.dag.graph[self.source_node.id]), 1)

    def test_delete_node(self):
        # 添加节点和边
        self.dag.add_node(self.source_node)
        self.dag.add_node(self.sink_node)
        self.dag.add_edge(self.source_node, self.sink_node)

        # 删除节点
        self.dag.delete_node(self.source_node.id)

        # 验证节点和相关边是否被删除
        self.assertNotIn(self.source_node.id, self.dag.graph)
        self.assertNotIn(self.source_node.id, self.dag.node_map)

    def test_delete_edge(self):
        # 添加节点和边
        self.dag.add_node(self.source_node)
        self.dag.add_node(self.sink_node)
        self.dag.add_edge(self.source_node, self.sink_node)

        # 删除边
        self.dag.delete_edge(self.source_node.id, self.sink_node.id)

        # 验证边是否被删除
        self.assertEqual(len(self.dag.graph[self.source_node.id]), 0)

    def test_get_source_node(self):
        # 添加节点和边
        self.dag.add_node(self.source_node)
        self.dag.add_node(self.sink_node)
        self.dag.add_edge(self.source_node, self.sink_node)

        # 获取sink_node的源节点
        source = self.dag.get_source_node(self.sink_node.id)
        self.assertEqual(source, self.source_node)

    def test_get_read_from_nodes(self):
        # 添加节点和边
        self.dag.add_node(self.source_node)
        self.dag.add_node(self.sink_node)
        self.dag.add_edge(self.source_node, self.sink_node)

        # 获取所有源节点
        sources = self.dag.get_read_from_nodes()
        self.assertEqual(len(sources), 1)
        self.assertEqual(sources[0], self.source_node)

    def test_get_target_node(self):
        # 添加节点和边
        self.dag.add_node(self.source_node)
        self.dag.add_node(self.sink_node)
        self.dag.add_edge(self.source_node, self.sink_node)

        # 获取目标节点
        target = self.dag.get_target_node()
        self.assertEqual(target, self.sink_node)

    def test_to_dict(self):
        # 添加节点和边
        self.dag.add_node(self.source_node)
        self.dag.add_node(self.sink_node)
        self.dag.add_edge(self.source_node, self.sink_node)

        # 获取DAG字典表示
        dag_dict = self.dag.to_dict()
        
        # 验证节点和边
        self.assertEqual(len(dag_dict["nodes"]), 2)
        self.assertEqual(len(dag_dict["edges"]), 1)
        self.assertEqual(dag_dict["edges"][0], {"source": "source_1", "target": "sink_1"})

    def test_update_node(self):
        # 添加节点
        self.dag.add_node(self.source_node)

        # 更新节点
        updated_source = Mock(spec=Source)
        updated_source.id = "source_1"
        updated_source.to_dict.return_value = {"id": "source_1", "type": "source", "updated": True}

        self.dag.update_node(updated_source)
        self.assertEqual(self.dag.node_map[updated_source.id], updated_source)

    def test_config(self):
        # 测试设置配置
        config = {"key": "value"}
        self.dag.config(config)
        self.assertEqual(self.dag.setting["key"], "value")

        # 测试获取配置
        result = self.dag.config()
        self.assertEqual(result, self.dag.setting)

    def test_replace_node(self):
        # 添加原始节点和边
        self.dag.add_node(self.source_node)
        self.dag.add_node(self.sink_node)
        self.dag.add_edge(self.source_node, self.sink_node)

        # 创建新节点
        new_source = Mock(spec=Source)
        new_source.id = "source_2"
        new_source.to_dict.return_value = {"id": "source_2", "type": "source"}

        # 替换节点
        self.dag.replace_node(self.source_node, new_source)

        # 验证节点替换和边的更新
        self.assertNotIn(self.source_node.id, self.dag.graph)
        self.assertIn(new_source.id, self.dag.graph)
        self.assertIn(self.sink_node.id, self.dag.graph[new_source.id])

    def test_edge(self):
        # 测试edge方法
        self.dag.edge(self.mock_source_stage, self.sink_node)

        # 验证节点和边的添加
        self.assertIn(self.source_node.id, self.dag.graph)
        self.assertIn(self.sink_node.id, self.dag.node_map)

    def test_add_extra_nodes_and_edges_with_merge_node(self):
        # 创建一个带有merge节点的源DAG
        source_dag = Mock()
        source_merge_node = Mock(spec=MergeNode)
        source_merge_node.id = "source_merge_1"
        source_merge_node.to_dict.return_value = {"id": "source_merge_1", "type": "merge"}

        source_dag.node_map = {
            "source_merge_1": source_merge_node,
            "source_1": self.source_node
        }
        source_dag.graph = {
            "source_1": ["source_merge_1"],
            "source_merge_1": []
        }

        # 创建一个带有merge节点的目标DAG
        sink_dag = Mock()
        sink_merge_node = Mock(spec=MergeNode)
        sink_merge_node.id = "sink_merge_1"
        sink_merge_node.to_dict.return_value = {"id": "sink_merge_1", "type": "merge"}

        sink_dag.node_map = {
            "sink_merge_1": sink_merge_node,
            "sink_1": self.sink_node
        }
        sink_dag.graph = {
            "sink_merge_1": ["sink_1"]
        }

        # 创建mock stage对象
        source_stage = Mock()
        source_stage.dag = source_dag
        source_stage.stage = self.source_node

        sink_stage = Mock()
        sink_stage.dag = sink_dag
        sink_stage.stage = self.sink_node

        # 创建一个新的merge节点作为参数
        merge_node = Mock(spec=MergeNode)
        merge_node.id = "merge_1"
        merge_node.to_dict.return_value = {"id": "merge_1", "type": "merge"}

        # 先添加源节点和目标节点到DAG
        self.dag.add_node(self.source_node)
        self.dag.add_node(self.sink_node)
        self.dag.add_node(merge_node)

        # 调用add_extra_nodes_and_edges方法
        self.dag.add_extra_nodes_and_edges(merge_node, source_stage, sink_stage)

        # 验证节点添加
        self.assertIn(self.source_node.id, self.dag.node_map)
        self.assertIn(merge_node.id, self.dag.node_map)
        self.assertIn(self.sink_node.id, self.dag.node_map)

        # 验证边的添加（源节点到merge节点，merge节点到目标节点）
        self.assertIn(merge_node.id, self.dag.graph.get(self.source_node.id, []))
        self.assertIn(self.sink_node.id, self.dag.graph.get(merge_node.id, []))

    def test_add_extra_nodes_and_edges_without_merge_node(self):
        # 创建简单的源DAG
        source_dag = Mock()
        source_dag.node_map = {
            "source_1": self.source_node
        }
        source_dag.graph = {
            "source_1": []
        }

        # 创建简单的目标DAG
        sink_dag = Mock()
        sink_dag.node_map = {
            "sink_1": self.sink_node
        }
        sink_dag.graph = {
            "sink_1": []
        }

        # 创建mock stage对象
        source_stage = Mock()
        source_stage.dag = source_dag
        source_stage.stage = self.source_node

        sink_stage = Mock()
        sink_stage.dag = sink_dag
        sink_stage.stage = self.sink_node

        # 先添加源节点和目标节点到DAG
        self.dag.add_node(self.source_node)
        self.dag.add_node(self.sink_node)
        self.dag.add_edge(self.source_node, self.sink_node)

        # 调用add_extra_nodes_and_edges方法，不传入merge节点
        self.dag.add_extra_nodes_and_edges(None, source_stage, sink_stage)

        # 验证节点添加
        self.assertIn(self.source_node.id, self.dag.node_map)
        self.assertIn(self.sink_node.id, self.dag.node_map)

        # 验证边的添加（源节点直接连接到目标节点）
        self.assertIn(self.sink_node.id, self.dag.graph.get(self.source_node.id, []))

    def test_add_extra_nodes_and_edges_with_exception(self):
        # 创建一个会抛出异常的mock DAG
        source_dag = Mock()
        source_dag.node_map = Mock(side_effect=Exception("Test exception"))
        source_dag.graph = {}

        sink_dag = Mock()
        sink_dag.node_map = Mock(side_effect=Exception("Test exception"))
        sink_dag.graph = {}

        # 创建mock stage对象
        source_stage = Mock()
        source_stage.dag = source_dag
        source_stage.stage = self.source_node

        sink_stage = Mock()
        sink_stage.dag = sink_dag
        sink_stage.stage = self.sink_node

        # 调用add_extra_nodes_and_edges方法
        # 验证方法不会因为异常而中断
        try:
            self.dag.add_extra_nodes_and_edges(None, source_stage, sink_stage)
        except Exception:
            self.fail("add_extra_nodes_and_edges should not raise exception")

    def test_to_instance(self):
        # 创建一个完整的DAG字典
        dag_dict = {
            "nodes": [
                {
                    "id": "source_1",
                    "type": "source",
                    "name": "Source Node",
                    "connectionId": "conn_1",
                    "table": "table1"
                },
                {
                    "id": "sink_1",
                    "type": "sink",
                    "name": "Sink Node",
                    "connectionId": "conn_2",
                    "table": "table2"
                },
                {
                    "id": "merge_1",
                    "type": "merge",
                    "name": "Merge Node"
                }
            ],
            "edges": [
                {"source": "source_1", "target": "merge_1"},
                {"source": "merge_1", "target": "sink_1"}
            ]
        }

        # 调用to_instance方法
        with patch('tapflow.lib.data_pipeline.dag.get_node_instance') as mock_get_node:
            # 模拟get_node_instance的返回值
            mock_source = Mock(spec=Source)
            mock_source.id = "source_1"
            mock_source.to_dict.return_value = dag_dict["nodes"][0]

            mock_sink = Mock(spec=Sink)
            mock_sink.id = "sink_1"
            mock_sink.to_dict.return_value = dag_dict["nodes"][1]

            mock_merge = Mock(spec=MergeNode)
            mock_merge.id = "merge_1"
            mock_merge.to_dict.return_value = dag_dict["nodes"][2]

            mock_get_node.side_effect = [mock_source, mock_sink, mock_merge]

            # 创建DAG实例
            dag = Dag.to_instance(dag_dict, "test_dag")

            # 验证基本属性
            self.assertEqual(dag.name, "test_dag")
            self.assertEqual(len(dag.node_map), 3)
            self.assertEqual(len(dag.graph), 3)

            # 验证节点是否正确添加
            self.assertIn("source_1", dag.node_map)
            self.assertIn("sink_1", dag.node_map)
            self.assertIn("merge_1", dag.node_map)

            # 验证边是否正确添加
            self.assertIn("merge_1", dag.graph["source_1"])
            self.assertIn("sink_1", dag.graph["merge_1"])

            # 验证get_node_instance的调用
            mock_get_node.assert_has_calls([
                call(dag_dict["nodes"][0]),
                call(dag_dict["nodes"][1]),
                call(dag_dict["nodes"][2])
            ])

    def test_to_instance_with_empty_dag(self):
        # 测试空DAG的情况
        dag_dict = {
            "name": "empty_dag",
            "nodes": [],
            "edges": [],
            "setting": {}
        }

        dag = Dag.to_instance(dag_dict, "empty_dag")
        
        # 验证基本属性
        self.assertEqual(dag.name, "empty_dag")
        self.assertEqual(len(dag.node_map), 0)
        self.assertEqual(len(dag.graph), 0)
        self.assertEqual(dag.setting, {})

    def test_to_instance_with_invalid_edge(self):
        # 测试包含无效边的情况
        dag_dict = {
            "nodes": [
                {
                    "id": "source_1",
                    "type": "source",
                    "name": "Source Node"
                }
            ],
            "edges": [
                {"source": "source_1", "target": "non_existent_node"}  # 无效的目标节点
            ]
        }

        with patch('tapflow.lib.data_pipeline.dag.get_node_instance') as mock_get_node:
            mock_source = Mock(spec=Source)
            mock_source.id = "source_1"
            mock_source.to_dict.return_value = dag_dict["nodes"][0]
            mock_get_node.return_value = mock_source

            # 创建DAG实例
            dag = Dag.to_instance(dag_dict, "invalid_edge_dag")
            
            # 验证节点被正确添加
            self.assertIn("source_1", dag.node_map)
            # 验证无效边被忽略
            self.assertEqual(len(dag.graph.get("source_1", [])), 0)

            # 验证get_node_instance的调用
            mock_get_node.assert_called_once_with(dag_dict["nodes"][0])

if __name__ == '__main__':
    unittest.main() 