import unittest
from unittest.mock import patch, Mock
from tapflow.lib.data_pipeline.job import Job
from tapflow.tests.test_data_pipeline.test_jobs import BaseJobTest

class TestJobRelations(BaseJobTest):
    @patch('tapflow.lib.data_pipeline.job.logger.fwarn')
    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_relations_with_none_id(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                                  mock_req_post, mock_logger_fwarn):
        # 创建Job实例，使用create_job方法并传入pipeline
        mock_pipeline = self.create_mock_pipeline()
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get, name="test_none_id", pipeline=mock_pipeline)

        result = job.relations()

        # 验证结果
        self.assertFalse(result)
        # 验证post请求没有被调用
        mock_req_post.assert_not_called()

    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_relations_request_failure(self, mock_client_cache, mock_get_obj, mock_req_get, mock_req_post):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)

        # 模拟请求失败
        mock_req_post.return_value.status_code = 500

        result = job.relations()

        # 验证结果
        self.assertEqual(result, [])
        # 验证post请求被正确调用
        mock_req_post.assert_called_once_with(
            "/task-console/relations",
            json={"taskId": "test_job_id"}
        )

    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_relations_response_code_not_ok(self, mock_client_cache, mock_get_obj, mock_req_get, mock_req_post):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)

        # 模拟响应code不是ok
        mock_req_post.return_value.status_code = 200
        mock_req_post.return_value.json.return_value = {"code": "error"}

        result = job.relations()

        # 验证结果
        self.assertEqual(result, [])
        # 验证post请求被正确调用
        mock_req_post.assert_called_once_with(
            "/task-console/relations",
            json={"taskId": "test_job_id"}
        )

    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_relations_success(self, mock_client_cache, mock_get_obj, mock_req_get, mock_req_post):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)

        # 模拟成功响应
        mock_req_post.return_value.status_code = 200
        mock_req_post.return_value.json.return_value = {
            "code": "ok",
            "data": [
                {"type": "connHeartbeat", "id": "heartbeat_id"},
                {"type": "logCollector", "id": "log_id"}
            ]
        }

        result = job.relations()

        # 验证结果
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["type"], "connHeartbeat")
        self.assertEqual(result[0]["id"], "heartbeat_id")
        self.assertEqual(result[1]["type"], "logCollector")
        self.assertEqual(result[1]["id"], "log_id")
        # 验证post请求被正确调用
        mock_req_post.assert_called_once_with(
            "/task-console/relations",
            json={"taskId": "test_job_id"}
        )

if __name__ == '__main__':
    unittest.main() 