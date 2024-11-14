import unittest
from unittest.mock import patch, Mock
from tapflow.lib.data_pipeline.job import Job

class TestJobInitialization(unittest.TestCase):
    def setUp(self):
        # 初始化通用的mock对象
        self.mock_job_data = {
            "id": "test_job_id",
            "name": "test_job",
            "dag": {
                "nodes": [],
                "edges": []
            },
            "syncType": "migrate",
            "status": "edit",
            "createTime": "2024-03-15T10:00:00Z"
        }

    def initialize_client_cache(self, mock_client_cache, job_id=None):
        # 初始化client_cache
        if job_id:
            mock_job_data = self.mock_job_data.copy()
            mock_job_data["id"] = job_id
            mock_client_cache["jobs"] = {
                "id_index": {
                    job_id: mock_job_data
                },
                "name_index": {
                    "test_job": mock_job_data
                },
                "number_index": {
                    "0": mock_job_data
                }
            }
        else:
            mock_client_cache["jobs"] = {
                "id_index": {
                    "test_job_id": self.mock_job_data
                },
                "name_index": {
                    "test_job": self.mock_job_data
                },
                "number_index": {
                    "0": self.mock_job_data
                }
            }

    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    def test_initialization_with_long_id(self, mock_req_get, mock_get_obj, mock_client_cache):
        # 测试使用24位完整ID初始化
        long_id = "1" * 24
        self.initialize_client_cache(mock_client_cache, job_id=long_id)

        # 模拟远端请求响应
        mock_req_get.return_value.status_code = 200
        mock_req_get.return_value.json.return_value = {
            "data": {
                "id": long_id,
                "name": "test_job",
                "dag": {"nodes": [], "edges": []},
                "syncType": "migrate"
            }
        }

        job = Job(id=long_id)
        
        self.assertEqual(job.id, long_id)
        self.assertEqual(job.name, "test_job")
        self.assertEqual(job.jobType, "migrate")
        # 确保get_obj没有被调用
        mock_get_obj.assert_not_called()
        # 确保req.get被调用
        mock_req_get.assert_called_once_with(f"/Task/{long_id}")

    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    def test_initialization_with_short_id(self, mock_req_get, mock_get_obj, mock_client_cache):
        # 测试使用短ID初始化
        short_id = "short_test_id"
        mock_get_obj.return_value = Mock(id="test_job_id")
        self.initialize_client_cache(mock_client_cache)

        # 模拟远端请求响应
        mock_req_get.return_value.status_code = 200
        mock_req_get.return_value.json.return_value = {
            "data": {
                "id": "test_job_id",
                "name": "test_job",
                "dag": {"nodes": [], "edges": []},
                "syncType": "migrate"
            }
        }

        job = Job(id=short_id)
        
        # 确保get_obj被调用
        mock_get_obj.assert_called_once_with("job", short_id)
        self.assertEqual(job.id, "test_job_id")
        # 确保req.get被调用
        mock_req_get.assert_called_once_with("/Task/test_job_id")

    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    def test_initialization_with_name(self, mock_req_get, mock_get_obj, mock_client_cache):
        # 测试使用名称初始化
        self.initialize_client_cache(mock_client_cache)

        # 模拟远端请求响应
        mock_req_get.return_value.status_code = 200
        mock_req_get.return_value.json.return_value = {
            "data": {
                "id": "test_job_id",
                "name": "test_job",
                "dag": {"nodes": [], "edges": []},
                "syncType": "migrate"
            }
        }

        job = Job(name="test_job")
        
        self.assertEqual(job.id, "test_job_id")
        self.assertEqual(job.name, "test_job")
        # 确保get_obj没有被调用
        mock_get_obj.assert_not_called()
        # 确保req.get被调用
        mock_req_get.assert_called_once_with("/Task/test_job_id")

    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    def test_list_method(self, mock_req_get, mock_get_obj, mock_client_cache):
        # 模拟API响应
        mock_req_get.side_effect = [
            # 第一个响应是任务列表
            Mock(status_code=200, json=lambda: {
                "data": {
                    "items": [
                        {
                            "id": "job_id_1",
                            "name": "job1",
                            "status": "running",
                            "agentId": "agent1",
                            "stats": {}
                        },
                        {
                            "id": "job_id_2",
                            "name": "job2",
                            "status": "stopped",
                            "agentId": "agent2",
                            "stats": {}
                        }
                    ]
                }
            }),
            # 后续响应是每个任务的详细信息
            Mock(status_code=200, json=lambda: {
                "data": {
                    "id": "job_id_1",
                    "name": "job1",
                    "dag": {"nodes": [], "edges": []},
                    "syncType": "migrate",
                    "status": "running",
                    "agentId": "agent1",
                    "stats": {}
                }
            }),
            Mock(status_code=200, json=lambda: {
                "data": {
                    "id": "job_id_2",
                    "name": "job2",
                    "dag": {"nodes": [], "edges": []},
                    "syncType": "migrate",
                    "status": "stopped",
                    "agentId": "agent2",
                    "stats": {}
                }
            })
        ]

        # 初始化client_cache，为每个任务ID添加数据
        mock_client_cache["jobs"] = {
            "id_index": {
                "job_id_1": {
                    "id": "job_id_1",
                    "name": "job1",
                    "status": "running",
                    "agentId": "agent1",
                    "stats": {}
                },
                "job_id_2": {
                    "id": "job_id_2",
                    "name": "job2",
                    "status": "stopped",
                    "agentId": "agent2",
                    "stats": {}
                }
            },
            "name_index": {
                "job1": {"id": "job_id_1"},
                "job2": {"id": "job_id_2"}
            },
            "number_index": {
                "0": {"id": "job_id_1"},
                "1": {"id": "job_id_2"}
            }
        }

        # 模拟get_obj返回值
        mock_get_obj.side_effect = lambda obj_type, obj_id: Mock(
            id=obj_id,
            name=f"job{obj_id[-1]}",
            dag={"nodes": [], "edges": []},
            syncType="migrate"
        )

        # 调用list方法
        jobs = Job.list()

        # 验证API调用
        mock_req_get.assert_any_call(
            "/Task",
            params={"filter": '{"fields":{"id":true,"name":true,"status":true,"agentId":true,"stats":true}}'}
        )

        # 验证返回结果
        self.assertEqual(len(jobs), 2)
        self.assertIsInstance(jobs[0], Job)
        self.assertIsInstance(jobs[1], Job)
        self.assertEqual(jobs[0].id, "job_id_1")
        self.assertEqual(jobs[1].id, "job_id_2")

    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    @patch('tapflow.lib.data_pipeline.job.req.get')
    def test_list_method_with_error(self, mock_req_get, mock_client_cache):
        # 模拟API错误响应
        mock_req_get.return_value.status_code = 500

        # 初始化client_cache
        self.initialize_client_cache(mock_client_cache)

        # 调用list方法
        jobs = Job.list()

        # 验证返回结果
        self.assertIsNone(jobs)

if __name__ == '__main__':
    unittest.main() 