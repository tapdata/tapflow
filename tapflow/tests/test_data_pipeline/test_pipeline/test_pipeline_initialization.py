# import unittest
# from unittest.mock import patch, Mock, MagicMock, create_autospec
# from tapflow.lib.data_pipeline.pipeline import Pipeline
# from tapflow.lib.data_pipeline.job import JobType, Job
# from tapflow.lib.data_pipeline.nodes.source import Source
# from tapflow.lib.data_pipeline.nodes.sink import Sink
# from tapflow.lib.data_pipeline.nodes.merge import MergeNode, Merge
# from tapflow.lib.data_pipeline.dag import Dag
# from tapflow.lib import request

# class TestPipelineInitialization(unittest.TestCase):
#     def setUp(self):
#         # Mock client_cache
#         self.mock_client_cache_patcher = patch('tapflow.lib.cache.client_cache')
#         self.mock_client_cache = self.mock_client_cache_patcher.start()
#         self.mock_client_cache.__getitem__.return_value = {
#             "jobs": {
#                 "name_index": {},
#                 "id_index": {}
#             }
#         }

#         # Mock Job
#         self.mock_job_patcher = patch('tapflow.lib.data_pipeline.pipeline.Job')
#         self.mock_job_class = self.mock_job_patcher.start()
        
#         # 创建mock job实例
#         self.mock_job = MagicMock()
#         self.mock_job.id = "test_job_id"
#         self.mock_job.jobType = "migrate"
#         self.mock_job.dag = MagicMock(spec=Dag)
#         self.mock_job.dag.dag = {"nodes": [], "edges": []}
#         self.mock_job.dag.graph = {}
#         self.mock_job.dag.node_map = {}
#         self.mock_job.dag.jobType = JobType.migrate
        
#         # 设置Job类的构造函数返回mock_job
#         self.mock_job_class.return_value = self.mock_job

#         # Mock Sink类
#         self.mock_sink_patcher = patch('tapflow.lib.data_pipeline.pipeline.Sink')
#         self.mock_sink_class = self.mock_sink_patcher.start()
        
#         def create_mock_sink(connection, table, **kwargs):
#             mock_sink = MagicMock(spec=Sink)
#             mock_sink.id = f"sink_{connection}_{table}"
#             mock_sink.tableName = table
#             mock_sink.connection = MagicMock()
#             mock_sink.connection.c = {"name": connection}
#             mock_sink.__class__ = Sink
#             return mock_sink
        
#         self.mock_sink_class.side_effect = create_mock_sink

#     def tearDown(self):
#         self.mock_client_cache_patcher.stop()
#         self.mock_job_patcher.stop()
#         self.mock_sink_patcher.stop()

#     def _setup_mock_job(self, mock_dag):
#         """设置mock job的通用方法"""
#         # 创建mock dag
#         mock_dag_obj = MagicMock(spec=Dag)
#         mock_dag_obj.dag = mock_dag
#         mock_dag_obj.jobType = JobType.migrate
#         mock_dag_obj.graph = {
#             node["id"]: [edge["target"] for edge in mock_dag["edges"] if edge["source"] == node["id"]]
#             for node in mock_dag["nodes"]
#         }
#         mock_dag_obj.node_map = {node["id"]: node for node in mock_dag["nodes"]}
        
#         # 设置mock job
#         self.mock_job.dag = mock_dag_obj
#         self.mock_job.jobType = JobType.migrate
#         self.mock_job.id = "test_job_id"
        
#         # Mock _make_node方法
#         def mock_make_node(node_dict):
#             node_type = node_dict.get("type")
#             mock_node = MagicMock()
#             mock_node.id = node_dict["id"]
            
#             if node_type == "table":
#                 mock_node.__class__ = Source
#                 mock_node.tableName = node_dict["tableName"]
#                 mock_node.connectionId = node_dict["attrs"]["connectionId"]
#                 mock_node.connection = MagicMock()
#                 mock_node.connection.c = {"name": node_dict["attrs"]["connectionName"]}
#                 if "setting" in node_dict.get("attrs", {}):
#                     mock_node.setting = node_dict["attrs"]["setting"]
            
#             elif node_type == "merge_table_processor":
#                 mock_node.__class__ = MergeNode
#                 mock_node.mergeProperties = node_dict.get("mergeProperties", [])
#                 mock_node.child = []
#                 if "mergeProperties" in node_dict:
#                     for prop in node_dict["mergeProperties"]:
#                         if "children" in prop:
#                             for child in prop["children"]:
#                                 mock_child = MagicMock()
#                                 mock_child.targetPath = child.get("targetPath")
#                                 mock_child.mergeType = child.get("mergeType")
#                                 mock_child.isArray = child.get("isArray", False)
#                                 mock_child.arrayKeys = child.get("arrayKeys", [])
#                                 mock_node.child.append(mock_child)
            
#             elif node_type == "sink":
#                 mock_node.__class__ = Sink
#                 mock_node.tableName = node_dict["tableName"]
#                 mock_node.connection = MagicMock()
#                 mock_node.connection.c = {"name": node_dict.get("attrs", {}).get("connectionName")}
            
#             elif node_type == "filter":
#                 mock_node.query = node_dict["query"]
            
#             elif node_type == "js":
#                 mock_node.script = node_dict["script"]
            
#             elif node_type == "python":
#                 mock_node.script = node_dict["script"]
            
#             elif node_type == "union":
#                 mock_node.name = node_dict["name"]
            
#             return mock_node

#         # 创建pipeline并设置mock
#         pipeline = Pipeline("test_pipeline")
#         pipeline._make_node = mock_make_node
        
#         return pipeline

#     @patch('tapflow.lib.data_pipeline.pipeline.Job')
#     def test_initialization_with_name(self, _):
#         pipeline = Pipeline("test_pipeline")
#         pipeline.job = self.mock_job
#         pipeline.dag = self.mock_job.dag
        
#         # 验证基本属性
#         self.assertEqual(pipeline.name, "test_pipeline")
#         self.assertEqual(pipeline.dag.jobType, JobType.migrate)
#         self.assertEqual(pipeline.stage, None)
#         self.assertEqual(pipeline.lines, [])
#         self.assertEqual(pipeline.sources, [])
#         self.assertEqual(pipeline.command, [])
#         self.assertEqual(pipeline.validateConfig, None)

#     @patch('tapflow.lib.data_pipeline.pipeline.Job')
#     def test_initialization_with_cached_job(self, _):
#         # 设置mock job的DAG
#         mock_dag = {
#             "nodes": [
#                 {
#                     "id": "source_1",
#                     "type": "table",
#                     "tableName": "main_table",
#                     "attrs": {
#                         "connectionName": "test_conn",
#                         "connectionId": "conn_1"
#                     }
#                 },
#                 {
#                     "id": "merge_1",
#                     "type": "merge_table_processor",
#                     "mergeProperties": [{
#                         "children": [
#                             {
#                                 "id": "lookup_source_1",
#                                 "type": "table",
#                                 "tableName": "lookup_table_1",
#                                 "targetPath": "path1",
#                                 "mergeType": "updateWrite",
#                                 "isArray": False,
#                                 "attrs": {
#                                     "connectionName": "test_conn",
#                                     "connectionId": "conn_2"
#                                 },
#                                 "children": [
#                                     {
#                                         "id": "nested_lookup_1",
#                                         "type": "table",
#                                         "tableName": "nested_table",
#                                         "targetPath": "path1.nested",
#                                         "mergeType": "updateIntoArray",
#                                         "isArray": True,
#                                         "arrayKeys": ["id"],
#                                         "attrs": {
#                                             "connectionName": "test_conn",
#                                             "connectionId": "conn_3"
#                                         }
#                                     }
#                                 ]
#                             }
#                         ]
#                     }]
#                 },
#                 {
#                     "id": "sink_1",
#                     "type": "sink",
#                     "tableName": "target_table"
#                 }
#             ],
#             "edges": [
#                 {"source": "source_1", "target": "merge_1"},
#                 {"source": "merge_1", "target": "sink_1"}
#             ]
#         }
        
#         pipeline = self._setup_mock_job(mock_dag)
        
#         # 验证lookup缓存的设置
#         # 验证第一层lookup
#         lookup_key_1 = "lookup_table_1_path1_<class 'dict'>"
#         self.assertIn(lookup_key_1, pipeline._lookup_cache)
#         lookup_pipeline_1 = pipeline._lookup_cache[lookup_key_1]
#         self.assertIsInstance(lookup_pipeline_1, Pipeline)
#         self.assertEqual(lookup_pipeline_1.lines[0].tableName, "lookup_table_1")
#         self.assertIn("path1", pipeline._lookup_path_cache)
        
#         # 验证嵌套lookup
#         lookup_key_2 = "nested_table_path1.nested_<class 'list'>"
#         self.assertIn(lookup_key_2, pipeline._lookup_cache)
#         lookup_pipeline_2 = pipeline._lookup_cache[lookup_key_2]
#         self.assertIsInstance(lookup_pipeline_2, Pipeline)
#         self.assertEqual(lookup_pipeline_2.lines[0].tableName, "nested_table")
#         self.assertIn("path1.nested", pipeline._lookup_path_cache)

#         # 验证merge node的层级关系
#         self.assertIsInstance(pipeline.mergeNode, Merge)
#         self.assertEqual(len(pipeline.mergeNode.child), 1)
        
#         # 验证第一层lookup的merge node配置
#         first_child = pipeline.mergeNode.child[0]
#         self.assertEqual(first_child.targetPath, "path1")
#         self.assertEqual(first_child.mergeType, "updateWrite")
#         self.assertFalse(first_child.isArray)
        
#         # 验证嵌套lookup的merge node配置
#         nested_child = first_child.child[0]
#         self.assertEqual(nested_child.targetPath, "path1.nested")
#         self.assertEqual(nested_child.mergeType, "updateIntoArray")
#         self.assertTrue(nested_child.isArray)
#         self.assertEqual(nested_child.arrayKeys, ["id"])

#         # 验证生成的command是否包含lookup操作
#         lookup_commands = [cmd for cmd in pipeline.command if cmd[0] == "lookup"]
#         self.assertTrue(len(lookup_commands) > 0)
#         # 验证lookup command的参数
#         for cmd in lookup_commands:
#             self.assertIn(cmd[1], ["lookup_table_1", "nested_table"])  # 表名
#             self.assertTrue(any(path in cmd[2] for path in ["path1", "path1.nested"]))  # 路径

#     @patch('tapflow.lib.data_pipeline.pipeline.Job')
#     def test_set_default_stage(self, _):
#         # 测试场景1: 最后一个节点是merge节点
#         mock_dag_merge_end = {
#             "nodes": [
#                 {
#                     "id": "source_1",
#                     "type": "table",
#                     "tableName": "test_table",
#                     "attrs": {"connectionName": "test_conn", "connectionId": "conn_1"}
#                 },
#                 {
#                     "id": "merge_1",
#                     "type": "merge_table_processor",
#                     "mergeProperties": [{"children": []}]
#                 }
#             ],
#             "edges": [
#                 {"source": "source_1", "target": "merge_1"}
#             ]
#         }

#         # 测试场景2: 最后一个节点是非merge节点
#         mock_dag_source_end = {
#             "nodes": [
#                 {
#                     "id": "source_1",
#                     "type": "table",
#                     "tableName": "test_table",
#                     "attrs": {"connectionName": "test_conn", "connectionId": "conn_1"}
#                 },
#                 {
#                     "id": "source_2",
#                     "type": "table",
#                     "tableName": "test_table_2",
#                     "attrs": {"connectionName": "test_conn", "connectionId": "conn_2"}
#                 }
#             ],
#             "edges": [
#                 {"source": "source_1", "target": "source_2"}
#             ]
#         }

#         # 测试场景3: 只有一个节点
#         mock_dag_single_node = {
#             "nodes": [
#                 {
#                     "id": "source_1",
#                     "type": "table",
#                     "tableName": "test_table",
#                     "attrs": {"connectionName": "test_conn", "connectionId": "conn_1"}
#                 }
#             ],
#             "edges": []
#         }

#         test_cases = [
#             (mock_dag_merge_end, "merge_table_processor", "merge_1"),
#             (mock_dag_source_end, "table", "source_2"),
#             (mock_dag_single_node, "table", "source_1")
#         ]

#         for mock_dag, expected_type, expected_id in test_cases:
#             pipeline = self._setup_mock_job(mock_dag)
            
#             # 验证stage置
#             self.assertIsNotNone(pipeline.stage)
#             self.assertEqual(pipeline.stage.id, expected_id)
            
#             # 验证根据DAG结构正确设置stage
#             if expected_type == "merge_table_processor":
#                 self.assertIsInstance(pipeline.stage, MergeNode)
#             else:
#                 self.assertIsInstance(pipeline.stage, Source)

#             # 验证stage是否是叶子节点（没有子节点的节点）
#             self.assertEqual(len(pipeline.dag.graph[pipeline.stage.id]), 0)

#     @patch('tapflow.lib.data_pipeline.pipeline.Job')
#     def test_set_lines(self, _):
#         # 测试场景1: 简单的线性DAG
#         mock_dag_linear = {
#             "nodes": [
#                 {
#                     "id": "source_1",
#                     "type": "table",
#                     "tableName": "test_table",
#                     "attrs": {"connectionName": "test_conn", "connectionId": "conn_1"}
#                 },
#                 {
#                     "id": "filter_1",
#                     "type": "filter",
#                     "query": "id > 0"
#                 },
#                 {
#                     "id": "sink_1",
#                     "type": "sink",
#                     "tableName": "target_table"
#                 }
#             ],
#             "edges": [
#                 {"source": "source_1", "target": "filter_1"},
#                 {"source": "filter_1", "target": "sink_1"}
#             ]
#         }

#         # 测试场景2: 包含merge节点的DAG
#         mock_dag_with_merge = {
#             "nodes": [
#                 {
#                     "id": "source_1",
#                     "type": "table",
#                     "tableName": "main_table",
#                     "attrs": {"connectionName": "test_conn", "connectionId": "conn_1"}
#                 },
#                 {
#                     "id": "source_2",
#                     "type": "table",
#                     "tableName": "lookup_table",
#                     "attrs": {"connectionName": "test_conn", "connectionId": "conn_2"}
#                 },
#                 {
#                     "id": "merge_1",
#                     "type": "merge_table_processor",
#                     "mergeProperties": [{"children": []}]
#                 },
#                 {
#                     "id": "sink_1",
#                     "type": "sink",
#                     "tableName": "target_table"
#                 }
#             ],
#             "edges": [
#                 {"source": "source_1", "target": "merge_1"},
#                 {"source": "source_2", "target": "merge_1"},
#                 {"source": "merge_1", "target": "sink_1"}
#             ]
#         }

#         # 测试场景3: 包含多种处理节点的复杂DAG
#         mock_dag_complex = {
#             "nodes": [
#                 {
#                     "id": "source_1",
#                     "type": "table",
#                     "tableName": "test_table",
#                     "attrs": {"connectionName": "test_conn", "connectionId": "conn_1"}
#                 },
#                 {
#                     "id": "filter_1",
#                     "type": "filter",
#                     "query": "id > 0"
#                 },
#                 {
#                     "id": "js_1",
#                     "type": "js",
#                     "script": "return record;"
#                 },
#                 {
#                     "id": "python_1",
#                     "type": "python",
#                     "script": "return record"
#                 },
#                 {
#                     "id": "sink_1",
#                     "type": "sink",
#                     "tableName": "target_table"
#                 }
#             ],
#             "edges": [
#                 {"source": "source_1", "target": "filter_1"},
#                 {"source": "filter_1", "target": "js_1"},
#                 {"source": "js_1", "target": "python_1"},
#                 {"source": "python_1", "target": "sink_1"}
#             ]
#         }

#         test_cases = [
#             (mock_dag_linear, ["table", "filter", "sink"]),
#             (mock_dag_with_merge, ["table", "table", "merge_table_processor", "sink"]),
#             (mock_dag_complex, ["table", "filter", "js", "python", "sink"])
#         ]

#         for mock_dag, expected_types in test_cases:
#             pipeline = self._setup_mock_job(mock_dag)
            
#             # 证lines列表的基本属性
#             self.assertEqual(len(pipeline.lines), len(mock_dag["nodes"]))
            
#             # 验证每个节点的类型
#             actual_types = [node["type"] for node in mock_dag["nodes"]]
#             self.assertEqual(actual_types, expected_types)
            
#             # 验证节点的顺序是否与DAG中的顺序一致
#             for i, node in enumerate(pipeline.lines):
#                 self.assertEqual(node.id, mock_dag["nodes"][i]["id"])
                
#             # 验证特定类型节点的属性
#             for node in pipeline.lines:
#                 node_data = next(n for n in mock_dag["nodes"] if n["id"] == node.id)
                
#                 if node_data["type"] == "table":
#                     self.assertEqual(node.tableName, node_data["tableName"])
#                     self.assertEqual(node.connectionId, node_data["attrs"]["connectionId"])
                
#                 elif node_data["type"] == "filter":
#                     self.assertEqual(node.query, node_data["query"])
                
#                 elif node_data["type"] == "js":
#                     self.assertEqual(node.script, node_data["script"])
                
#                 elif node_data["type"] == "python":
#                     self.assertEqual(node.script, node_data["script"])
                
#                 elif node_data["type"] == "merge_table_processor":
#                     self.assertIsInstance(node, MergeNode)

#     @patch('tapflow.lib.data_pipeline.pipeline.Job')
#     def test_set_sources(self, _):
#         # 测试场景1: 单个source节点
#         mock_dag_single_source = {
#             "nodes": [
#                 {
#                     "id": "source_1",
#                     "type": "table",
#                     "tableName": "test_table",
#                     "attrs": {
#                         "connectionName": "test_conn",
#                         "connectionId": "conn_1"
#                     }
#                 },
#                 {
#                     "id": "sink_1",
#                     "type": "sink",
#                     "tableName": "target_table"
#                 }
#             ],
#             "edges": [
#                 {"source": "source_1", "target": "sink_1"}
#             ]
#         }

#         # 测试场景2: 多个source节点（用于merge）
#         mock_dag_multiple_sources = {
#             "nodes": [
#                 {
#                     "id": "source_1",
#                     "type": "table",
#                     "tableName": "main_table",
#                     "attrs": {
#                         "connectionName": "test_conn",
#                         "connectionId": "conn_1"
#                     }
#                 },
#                 {
#                     "id": "source_2",
#                     "type": "table",
#                     "tableName": "lookup_table",
#                     "attrs": {
#                         "connectionName": "test_conn",
#                         "connectionId": "conn_2"
#                     }
#                 },
#                 {
#                     "id": "merge_1",
#                     "type": "merge_table_processor",
#                     "mergeProperties": [{"children": []}]
#                 }
#             ],
#             "edges": [
#                 {"source": "source_1", "target": "merge_1"},
#                 {"source": "source_2", "target": "merge_1"}
#             ]
#         }

#         # 测试场景3: 包含lookup的复杂source结构
#         mock_dag_lookup_sources = {
#             "nodes": [
#                 {
#                     "id": "source_1",
#                     "type": "table",
#                     "tableName": "main_table",
#                     "attrs": {
#                         "connectionName": "test_conn",
#                         "connectionId": "conn_1"
#                     }
#                 },
#                 {
#                     "id": "merge_1",
#                     "type": "merge_table_processor",
#                     "mergeProperties": [{
#                         "children": [
#                             {
#                                 "id": "lookup_source_1",
#                                 "type": "table",
#                                 "tableName": "lookup_table_1",
#                                 "attrs": {
#                                     "connectionName": "test_conn",
#                                     "connectionId": "conn_2"
#                                 }
#                             }
#                         ]
#                     }]
#                 }
#             ],
#             "edges": [
#                 {"source": "source_1", "target": "merge_1"}
#             ]
#         }

#         test_cases = [
#             (mock_dag_single_source, 1),  # 期望1个source
#             (mock_dag_multiple_sources, 2),  # 期望2个source
#             (mock_dag_lookup_sources, 1)  # 期望1个主source（lookup sources通其他方式处理）
#         ]

#         for mock_dag, expected_source_count in test_cases:
#             pipeline = self._setup_mock_job(mock_dag)
            
#             # 验证sources列表的基本属性
#             self.assertEqual(len(pipeline.sources), expected_source_count)
            
#             # 验证每个source节点的属性
#             for source in pipeline.sources:
#                 # 找到对应的node数据
#                 node_data = next(
#                     node for node in mock_dag["nodes"] 
#                     if node["type"] == "table" and node["id"] == source.id
#                 )
                
#                 # 验证source节点的基本属性
#                 self.assertEqual(source.tableName, node_data["tableName"])
#                 self.assertEqual(source.connectionId, node_data["attrs"]["connectionId"])
                
#                 # 验证source节点的连信息
#                 self.assertEqual(
#                     source.connection.c.get("name"), 
#                     node_data["attrs"]["connectionName"]
#                 )
                
#                 # 验证source节点DAG中的位置
#                 # source节点应该是其他节点的输入
#                 has_outgoing_edge = any(
#                     edge["source"] == source.id 
#                     for edge in mock_dag["edges"]
#                 )
#                 self.assertTrue(has_outgoing_edge)
                
#                 # source节点不应该是其他节点的输出
#                 has_incoming_edge = any(
#                     edge["target"] == source.id 
#                     for edge in mock_dag["edges"]
#                 )
#                 self.assertFalse(has_incoming_edge)

#     @patch('tapflow.lib.data_pipeline.pipeline.Job')
#     def test_set_command(self, _):
#         # 测试场景1: 基本的读写流��
#         mock_dag_basic = {
#             "nodes": [
#                 {
#                     "id": "source_1",
#                     "type": "table",
#                     "tableName": "test_table",
#                     "attrs": {
#                         "connectionName": "test_conn",
#                         "connectionId": "conn_1"
#                     }
#                 },
#                 {
#                     "id": "sink_1",
#                     "type": "sink",
#                     "tableName": "target_table",
#                     "attrs": {
#                         "connectionName": "target_conn"
#                     }
#                 }
#             ],
#             "edges": [
#                 {"source": "source_1", "target": "sink_1"}
#             ]
#         }

#         # 测试场景2: 包含过滤和转换的流程
#         mock_dag_transform = {
#             "nodes": [
#                 {
#                     "id": "source_1",
#                     "type": "table",
#                     "tableName": "source_table",
#                     "attrs": {
#                         "connectionName": "test_conn",
#                         "connectionId": "conn_1",
#                         "setting": {
#                             "conditions": [
#                                 {"operator": 5, "key": "id", "value": "100"}
#                             ],
#                             "isFilter": True
#                         }
#                     }
#                 },
#                 {
#                     "id": "filter_1",
#                     "type": "filter",
#                     "query": "age > 18"
#                 },
#                 {
#                     "id": "js_1",
#                     "type": "js",
#                     "script": "record.age = record.age + 1; return record;"
#                 },
#                 {
#                     "id": "sink_1",
#                     "type": "sink",
#                     "tableName": "target_table",
#                     "attrs": {
#                         "connectionName": "target_conn"
#                     }
#                 }
#             ],
#             "edges": [
#                 {"source": "source_1", "target": "filter_1"},
#                 {"source": "filter_1", "target": "js_1"},
#                 {"source": "js_1", "target": "sink_1"}
#             ]
#         }

#         # 测试场景3: 复杂的lookup和merge流程
#         mock_dag_lookup_merge = {
#             "nodes": [
#                 {
#                     "id": "source_1",
#                     "type": "table",
#                     "tableName": "main_table",
#                     "attrs": {
#                         "connectionName": "test_conn",
#                         "connectionId": "conn_1"
#                     }
#                 },
#                 {
#                     "id": "merge_1",
#                     "type": "merge_table_processor",
#                     "mergeProperties": [{
#                         "children": [
#                             {
#                                 "id": "lookup_1",
#                                 "type": "table",
#                                 "tableName": "lookup_table_1",
#                                 "targetPath": "details",
#                                 "mergeType": "updateWrite",
#                                 "isArray": False,
#                                 "attrs": {
#                                     "connectionName": "test_conn",
#                                     "connectionId": "conn_2"
#                                 },
#                                 "children": [
#                                     {
#                                         "id": "nested_lookup_1",
#                                         "type": "table",
#                                         "tableName": "nested_table",
#                                         "targetPath": "details.items",
#                                         "mergeType": "updateIntoArray",
#                                         "isArray": True,
#                                         "arrayKeys": ["id"],
#                                         "attrs": {
#                                             "connectionName": "test_conn",
#                                             "connectionId": "conn_3"
#                                         }
#                                     }
#                                 ]
#                             }
#                         ]
#                     }]
#                 },
#                 {
#                     "id": "sink_1",
#                     "type": "sink",
#                     "tableName": "target_table",
#                     "attrs": {
#                         "connectionName": "target_conn"
#                     }
#                 }
#             ],
#             "edges": [
#                 {"source": "source_1", "target": "merge_1"},
#                 {"source": "merge_1", "target": "sink_1"}
#             ]
#         }

#         # 测试场景4: 包含Union和其他复杂操作的流程
#         mock_dag_union = {
#             "nodes": [
#                 {
#                     "id": "source_1",
#                     "type": "table",
#                     "tableName": "table1",
#                     "attrs": {
#                         "connectionName": "test_conn",
#                         "connectionId": "conn_1"
#                     }
#                 },
#                 {
#                     "id": "source_2",
#                     "type": "table",
#                     "tableName": "table2",
#                     "attrs": {
#                         "connectionName": "test_conn",
#                         "connectionId": "conn_2"
#                     }
#                 },
#                 {
#                     "id": "union_1",
#                     "type": "union",
#                     "name": "Union"
#                 },
#                 {
#                     "id": "js_1",
#                     "type": "js",
#                     "script": "return record;"
#                 },
#                 {
#                     "id": "sink_1",
#                     "type": "sink",
#                     "tableName": "target_table",
#                     "attrs": {
#                         "connectionName": "target_conn"
#                     }
#                 }
#             ],
#             "edges": [
#                 {"source": "source_1", "target": "union_1"},
#                 {"source": "source_2", "target": "union_1"},
#                 {"source": "union_1", "target": "js_1"},
#                 {"source": "js_1", "target": "sink_1"}
#             ]
#         }

#         test_cases = [
#             (mock_dag_basic, [
#                 ("read_from", "test_conn.test_table"),
#                 ("write_to", "target_conn.target_table")
#             ]),
#             (mock_dag_transform, [
#                 ("read_from", "test_conn.source_table", "id = 100"),
#                 ("filter", "age > 18"),
#                 ("js", "record.age = record.age + 1; return record;"),
#                 ("write_to", "target_conn.target_table")
#             ]),
#             (mock_dag_lookup_merge, [
#                 ("read_from", "test_conn.main_table"),
#                 ("lookup", "lookup_table_1", "details", "dict"),
#                 ("lookup", "nested_table", "details.items", "list", ["id"]),
#                 ("write_to", "target_conn.target_table")
#             ]),
#             (mock_dag_union, [
#                 ("read_from", "test_conn.table1"),
#                 ("read_from", "test_conn.table2"),
#                 ("union",),
#                 ("js", "return record;"),
#                 ("write_to", "target_conn.target_table")
#             ])
#         ]

#         for mock_dag, expected_commands in test_cases:
#             pipeline = self._setup_mock_job(mock_dag)
            
#             # 验证command列表长度
#             self.assertEqual(len(pipeline.command), len(expected_commands))
            
#             # 验证每个command的基本结构
#             for actual_cmd, expected_cmd in zip(pipeline.command, expected_commands):
#                 self.assertEqual(actual_cmd[0], expected_cmd[0])
                
#                 # 验证特定类型command的参数
#                 if expected_cmd[0] == "read_from":
#                     self.assertEqual(actual_cmd[1], expected_cmd[1])
#                     if len(expected_cmd) > 2:  # 有filter条件
#                         self.assertIn(expected_cmd[2], str(actual_cmd))
                
#                 elif expected_cmd[0] == "write_to":
#                     self.assertEqual(actual_cmd[1], expected_cmd[1])
                
#                 elif expected_cmd[0] == "filter":
#                     self.assertEqual(actual_cmd[1], expected_cmd[1])
                
#                 elif expected_cmd[0] == "js":
#                     self.assertEqual(actual_cmd[1], expected_cmd[1])
                
#                 elif expected_cmd[0] == "lookup":
#                     self.assertEqual(actual_cmd[1], expected_cmd[1])  # 表名
#                     self.assertEqual(actual_cmd[2], expected_cmd[2])  # 路径
#                     if len(expected_cmd) > 3:  # 有数组配置
#                         self.assertEqual(actual_cmd[3], expected_cmd[3])  # 类型
#                     if len(expected_cmd) > 4:  # 有arrayKeys
#                         self.assertEqual(actual_cmd[4], expected_cmd[4])  # arrayKeys

#             # 验证command的顺序是否正确
#             for i in range(len(pipeline.command) - 1):
#                 current_cmd = pipeline.command[i]
#                 next_cmd = pipeline.command[i + 1]
                
#                 # 验证基本顺序规则
#                 if current_cmd[0] == "read_from":
#                     self.assertNotEqual(next_cmd[0], "read_from")  # read_from后不应直接跟read_from
                
#                 if next_cmd[0] == "write_to":
#                     self.assertNotEqual(current_cmd[0], "write_to")  # write_to前不应是write_to
                
#                 # 验证lookup相关的顺序
#                 if current_cmd[0] == "lookup":
#                     self.assertNotIn(next_cmd[0], ["read_from"])  # lookup后不应是read_from

# if __name__ == '__main__':
#     unittest.main() 