import unittest
from unittest.mock import patch, Mock
from tapflow.lib.data_pipeline.job import Job
from tapflow.tests.test_data_pipeline.test_jobs import BaseJobTest

class TestJobStatus(BaseJobTest):
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_status_with_quiet_true(self, mock_client_cache, mock_get_obj, mock_req_get, mock_logger_info):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        
        # 设置task_api.get_task_by_id的返回值
        mock_task_response = {
            "id": "test_job_id",
            "status": "running",
            "name": "test_job"
        }
        
        # 模拟TaskApi.get_task_by_id的返回值
        with patch('tapflow.lib.backend_apis.task.TaskApi.get_task_by_id') as mock_get_task:
            mock_get_task.return_value = mock_task_response
            
            # 调用status方法，quiet=True（默认值）
            status = job.status()

            # 验证返回值
            self.assertEqual(status, "running")
            # 验证logger.info没有被调用
            mock_logger_info.assert_not_called()
            # 验证get_task_by_id被调用
            mock_get_task.assert_called_once_with(job.id)

    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_status_with_quiet_false(self, mock_client_cache, mock_get_obj, mock_req_get, mock_logger_info):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        
        # 设置task_api.get_task_by_id的返回值
        mock_task_response = {
            "id": "test_job_id",
            "status": "running",
            "name": "test_job"
        }
        
        # 模拟TaskApi.get_task_by_id的返回值
        with patch('tapflow.lib.backend_apis.task.TaskApi.get_task_by_id') as mock_get_task:
            mock_get_task.return_value = mock_task_response
            
            # 调用status方法，quiet=False
            status = job.status(quiet=False)

            # 验证返回值
            self.assertEqual(status, "running")
            # 验证logger.info被调用，并且参数正确
            mock_logger_info.assert_called_once_with("job status is: {}", "running")
            # 验证get_task_by_id被调用
            mock_get_task.assert_called_once_with(job.id)

    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_status_with_provided_response(self, mock_client_cache, mock_get_obj, mock_req_get, mock_logger_info):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)

        # 提供响应数据
        response = {
            "status": "stopped"
        }

        # 调用status方法，提供响应数据，quiet=False
        status = job.status(res=response, quiet=False)

        # 验证返回值
        self.assertEqual(status, "stopped")
        # 验证logger.info被调用，并且参数正确
        mock_logger_info.assert_called_once_with("job status is: {}", "stopped")

if __name__ == '__main__':
    unittest.main() 