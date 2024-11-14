import unittest
from unittest.mock import patch, Mock
from tapflow.lib.data_pipeline.job import Job

class TestJobStatus(unittest.TestCase):
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

    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_status_with_quiet_true(self, mock_client_cache, mock_get_obj, mock_req_get, mock_logger_info):
        # 初始化测试数据
        self.initialize_client_cache(mock_client_cache)
        
        # 模拟get_obj返回值
        mock_get_obj.return_value = Mock(id="test_job_id")

        # 模拟API响应
        mock_req_get.return_value.json.return_value = {
            "data": {
                "status": "running"
            }
        }

        job = Job(id="test_job_id")
        # 调用status方法，quiet=True（默认值）
        status = job.status()

        # 验证返回值
        self.assertEqual(status, "running")
        # 验证logger.info没有被调用
        mock_logger_info.assert_not_called()

    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_status_with_quiet_false(self, mock_client_cache, mock_get_obj, mock_req_get, mock_logger_info):
        # 初始化测试数据
        self.initialize_client_cache(mock_client_cache)
        
        # 模拟get_obj返回值
        mock_get_obj.return_value = Mock(id="test_job_id")

        # 模拟API响应
        mock_req_get.return_value.json.return_value = {
            "data": {
                "status": "running"
            }
        }

        job = Job(id="test_job_id")
        # 调用status方法，quiet=False
        status = job.status(quiet=False)

        # 验证返回值
        self.assertEqual(status, "running")
        # 验证logger.info被调用，并且参数正确
        mock_logger_info.assert_called_once_with("job status is: {}", "running")

    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_status_with_provided_response(self, mock_client_cache, mock_get_obj, mock_req_get, mock_logger_info):
        # 初始化测试数据
        self.initialize_client_cache(mock_client_cache)
        
        # 模拟get_obj返回值
        mock_get_obj.return_value = Mock(id="test_job_id")

        # 模拟req.get响应
        mock_req_get.return_value.json.return_value = {
            "data": {
                "id": "test_job_id",
                "name": "test_job",
                "dag": {"nodes": [], "edges": []},
                "syncType": "migrate",
                "status": "running"
            }
        }

        job = Job(id="test_job_id")

        # 提供响应数据
        response = {
            "data": {
                "status": "stopped"
            }
        }

        # 调用status方法，提供响应数据，quiet=False
        status = job.status(res=response, quiet=False)

        # 验证返回值
        self.assertEqual(status, "stopped")
        # 验证logger.info被调用，并且参数正确
        mock_logger_info.assert_called_once_with("job status is: {}", "stopped")

if __name__ == '__main__':
    unittest.main() 