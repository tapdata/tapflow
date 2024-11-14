import unittest
from unittest.mock import patch, Mock, call
import time
from tapflow.lib.data_pipeline.job import Job

class TestJobSave(unittest.TestCase):
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
        self.mock_time = int(time.time() * 1000)

    def initialize_client_cache(self, mock_client_cache):
        # 初始化client_cache
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

    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.fwarn')
    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.req.patch')
    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    @patch('tapflow.lib.data_pipeline.job.system_server_conf', new_callable=dict)
    def test_save_new_job_success(self, mock_system_conf, mock_client_cache, mock_get_obj, mock_req_get, 
                                mock_req_post, mock_req_patch, mock_logger_warn, mock_logger_fwarn, mock_time):
        # 初始化测试数据
        self.initialize_client_cache(mock_client_cache)
        mock_get_obj.return_value = Mock(id="test_job_id")
        mock_time.time.return_value = self.mock_time / 1000

        # 模拟system_server_conf
        mock_system_conf.update({
            "user_id": "test_user_id",
            "username": "test_user"
        })

        # 创建mock pipeline和dag
        mock_pipeline = Mock()
        mock_pipeline.target = None
        mock_pipeline.sources = []
        mock_dag = Mock()
        mock_dag.jobType = "migrate"
        mock_dag.dag = {"nodes": [], "edges": []}
        mock_dag.setting = {"syncPoints": []}
        mock_dag.to_dict.return_value = {}

        # 模拟post请求成功
        mock_req_post.return_value.json.return_value = {
            "code": "ok",
            "data": {
                "id": "new_job_id",
                "name": "new_job",
                "dag": {"nodes": [], "edges": []},
                "syncType": "migrate",
                "status": "edit"
            }
        }

        # 模拟patch请求成功
        mock_req_patch.return_value.status_code = 200
        mock_req_patch.return_value.json.return_value = {
            "code": "ok",
            "data": {
                "id": "new_job_id",
                "name": "new_job",
                "dag": {"nodes": [], "edges": []},
                "syncType": "migrate",
                "status": "edit"
            }
        }

        # 创建Job实例
        job = Job(name="new_job", pipeline=mock_pipeline, dag=mock_dag)
        result = job.save()

        # 验证结果
        self.assertTrue(result)
        self.assertEqual(job.id, "new_job_id")

        # 验证post请求被正确调用
        expected_post_data = {
            "editVersion": self.mock_time,
            "syncType": "migrate",
            "name": "new_job",
            "status": "edit",
            "dag": {"nodes": [], "edges": []},
            "user_id": "test_user_id",
            "customId": "test_user_id",
            "createUser": "test_user",
            "syncPoints": [],
            "dynamicAdjustMemoryUsage": True,
            "crontabExpressionFlag": False
        }
        mock_req_post.assert_called_once_with("/Task", json=expected_post_data)

        # 验证patch请求被正确调用
        expected_patch_body = {
            "dag": {
                "nodes": [],
                "edges": []
            },
            "editVersion": self.mock_time,
            "id": "new_job_id"
        }
        mock_req_patch.assert_any_call("/Task", json=expected_patch_body)

        # 验证patch被调用了两次
        self.assertEqual(mock_req_patch.call_count, 2)

        # 验证patch请求包含更新的schema
        expected_confirm_data = {
            "editVersion": self.mock_time,
            "syncType": "migrate",
            "name": "new_job",
            "status": "edit",
            "dag": {"nodes": [], "edges": []},
            "user_id": "test_user_id",
            "customId": "test_user_id",
            "createUser": "test_user",
            "syncPoints": [],
            "dynamicAdjustMemoryUsage": True,
            "crontabExpressionFlag": False
        }
        mock_req_patch.assert_any_call(f"/Task/confirm/new_job_id", json=expected_confirm_data)

if __name__ == '__main__':
    unittest.main() 