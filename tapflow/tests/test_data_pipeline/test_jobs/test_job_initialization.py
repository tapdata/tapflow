import unittest
from tapflow.tests.test_data_pipeline.test_jobs import BaseJobTest
from unittest.mock import patch, Mock, call
from tapflow.lib.data_pipeline.job import Job

class TestJobInitialization(BaseJobTest):
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    def test_initialization_with_long_id(self, mock_req_get, mock_get_obj, mock_client_cache):
        # 初始化client_cache，确保包含正确的ID
        mock_long_id = "1" * 24
        mock_client_cache["jobs"] = {
            "id_index": {
                mock_long_id: {
                    "id": mock_long_id,
                    "name": "test_job",
                    "dag": {"nodes": [], "edges": []},
                    "syncType": "migrate"
                }
            }
        }

        # 设置mock响应
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "id": mock_long_id,
                "name": "test_job",
                "dag": {"nodes": [], "edges": []},
                "syncType": "migrate"
            }
        }
        mock_req_get.return_value = mock_response

        # 创建Job实例
        job = Job(id=mock_long_id)

        # 验证结果
        self.assertEqual(job.id, mock_long_id)
        self.assertEqual(job.name, "test_job")
        self.assertEqual(job.jobType, "migrate")
        # 确保get_obj没有被调用
        mock_get_obj.assert_not_called()
        # 确保req.get被调用
        mock_req_get.assert_called_once_with(f"/Task/{mock_long_id}")

    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    def test_initialization_with_short_id(self, mock_req_get, mock_get_obj, mock_client_cache):
        # 初始化client_cache
        self.initialize_client_cache(mock_client_cache)

        # 设置mock响应
        mock_get_obj.return_value = Mock(id="test_job_id")
        mock_req_get.return_value.json.return_value = {
            "data": {
                "id": "test_job_id",
                "name": "test_job",
                "dag": {"nodes": [], "edges": []},
                "syncType": "migrate"
            }
        }

        # ���建Job实例
        job = Job(id="short_test_id")
        job.job = mock_req_get.return_value.json()["data"]  # 显式设置job属性
        
        # 确保get_obj被调用
        mock_get_obj.assert_called_once_with("job", "short_test_id")
        self.assertEqual(job.id, "test_job_id")
        # 确保req.get被调用
        mock_req_get.assert_called_once_with("/Task/test_job_id")

    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    def test_initialization_with_name(self, mock_req_get, mock_get_obj, mock_client_cache):
        # 初始化client_cache
        self.initialize_client_cache(mock_client_cache)

        # 创建mock pipeline
        mock_pipeline = self.create_mock_pipeline()

        # 设置mock响应
        mock_req_get.return_value.json.return_value = {
            "data": {
                "id": "test_job_id",
                "name": "test_job",
                "dag": {"nodes": [], "edges": []},
                "syncType": "migrate"
            }
        }

        # 创建Job实例
        job = Job(name="test_job", pipeline=mock_pipeline)
        job.job = mock_req_get.return_value.json()["data"]  # 显式设置job属性
        
        # 验证结果
        self.assertEqual(job.id, "test_job_id")
        self.assertEqual(job.job["name"], "test_job")
        # 确保get_obj没有被调用
        mock_get_obj.assert_not_called()
        # 确保req.get被调用
        mock_req_get.assert_called_once_with("/Task/test_job_id")

    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    def test_list_method(self, mock_req_get, mock_get_obj, mock_client_cache):
        # 初始化client_cache
        mock_client_cache["jobs"] = {
            "id_index": {
                "job_id_1": {
                    "id": "job_id_1",
                    "name": "job1",
                    "status": "running",
                    "agentId": "agent1",
                    "stats": {},
                    "dag": {"nodes": [], "edges": []},
                    "syncType": "migrate"
                },
                "job_id_2": {
                    "id": "job_id_2",
                    "name": "job2",
                    "status": "stopped",
                    "agentId": "agent2",
                    "stats": {},
                    "dag": {"nodes": [], "edges": []},
                    "syncType": "migrate"
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

        # 模拟TaskApi.get_all_tasks的响应
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "items": [
                    {
                        "id": "job_id_1",
                        "name": "job1",
                        "status": "running",
                        "agentId": "agent1",
                        "stats": {},
                        "dag": {"nodes": [], "edges": []},
                        "syncType": "migrate"
                    },
                    {
                        "id": "job_id_2",
                        "name": "job2",
                        "status": "stopped",
                        "agentId": "agent2",
                        "stats": {},
                        "dag": {"nodes": [], "edges": []},
                        "syncType": "migrate"
                    }
                ]
            }
        }

        # 设置mock_req_get的返回值
        mock_req_get.return_value = mock_response

        # 设置mock_get_obj的返回值
        mock_get_obj.side_effect = [
            Mock(id="job_id_1"),
            Mock(id="job_id_2")
        ]

        # 模拟TaskApi.get_task_by_id的返回值
        with patch('tapflow.lib.backend_apis.task.TaskApi.get_task_by_id') as mock_get_task:
            mock_get_task.side_effect = [
                {
                    "id": "job_id_1",
                    "name": "job1",
                    "status": "running",
                    "dag": {"nodes": [], "edges": []},
                    "syncType": "migrate"
                },
                {
                    "id": "job_id_2",
                    "name": "job2",
                    "status": "stopped",
                    "dag": {"nodes": [], "edges": []},
                    "syncType": "migrate"
                }
            ]

            # 调用list方法
            with patch('tapflow.lib.backend_apis.task.TaskApi.get_all_tasks') as mock_get_all_tasks:
                mock_get_all_tasks.return_value = mock_response.json()["data"]["items"]
                jobs = Job.list()

            # 验证返回结果
            self.assertEqual(len(jobs), 2)
            self.assertIsInstance(jobs[0], Job)
            self.assertIsInstance(jobs[1], Job)
            self.assertEqual(jobs[0].id, "job_id_1")
            self.assertEqual(jobs[1].id, "job_id_2")

            # 验证get_all_tasks被调用
            mock_get_all_tasks.assert_called_once()

            # 验证get_obj被正确调用
            mock_get_obj.assert_has_calls([
                call("job", "job_id_1"),
                call("job", "job_id_2")
            ])

            # 验证get_task_by_id被正确调用
            mock_get_task.assert_has_calls([
                call("job_id_1"),
                call("job_id_2")
            ])

if __name__ == '__main__':
    unittest.main() 