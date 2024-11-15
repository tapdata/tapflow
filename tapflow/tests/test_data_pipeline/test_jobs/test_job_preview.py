from tapflow.tests.test_data_pipeline.test_jobs import BaseJobTest
from unittest.mock import patch, Mock
import json
from tapflow.lib.data_pipeline.job import Job

class TestJobPreview(BaseJobTest):
    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_preview_success(self, mock_client_cache, mock_get_obj, mock_req_get, mock_req_post, 
                           mock_logger_info, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        mock_time.time.side_effect = [0, 1]  # 开始时间和结束时间

        # 模拟find_final_target返回值
        job.find_final_target = Mock(return_value=["target_node_1"])

        # 模拟preview请求响应
        mock_req_post.return_value.json.return_value = {
            "code": "ok",
            "data": {
                "nodeResult": {
                    "target_node_1": {
                        "data": [
                            {"id": 1, "name": "test1"},
                            {"id": 2, "name": "test2"}
                        ]
                    },
                    "other_node": {
                        "data": [
                            {"id": 3, "name": "test3"}
                        ]
                    }
                }
            }
        }

        # 调用preview方法
        result = job.preview(quiet=False)

        # 验证结果
        self.assertIsNotNone(result)
        self.assertIn("target_node_1", result)
        self.assertIn("other_node", result)

        # 验证请求参数
        mock_req_post.assert_called_once_with(
            "/proxy/call",
            json={
                "className": "TaskPreviewService",
                "method": "preview",
                "args": [
                    json.dumps(job.job),
                    None,
                    1
                ]
            }
        )

        # 验证日志调用
        mock_logger_info.assert_called_once_with("preview view took {} ms", 1000)

    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_preview_response_code_not_ok(self, mock_client_cache, mock_get_obj, mock_req_get, mock_req_post, 
                                        mock_logger_info, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        mock_time.time.side_effect = [0, 1]

        # 模拟find_final_target返回值
        job.find_final_target = Mock(return_value=["target_node_1"])

        # 模拟preview请求响应返回错误码
        mock_req_post.return_value.json.return_value = {
            "code": "error"
        }

        # 调用preview方法
        result = job.preview(quiet=False)

        # 验证结果
        self.assertIsNone(result)

        # 验证日志调用
        mock_logger_info.assert_called_once_with("preview view took {} ms", 1000)

    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_preview_with_quiet_true(self, mock_client_cache, mock_get_obj, mock_req_get, mock_req_post, 
                                   mock_logger_info, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        mock_time.time.side_effect = [0, 1]

        # 模拟find_final_target返回值
        job.find_final_target = Mock(return_value=["target_node_1"])

        # 模拟preview请求响应
        mock_req_post.return_value.json.return_value = {
            "code": "ok",
            "data": {
                "nodeResult": {
                    "target_node_1": {
                        "data": [{"id": 1, "name": "test1"}]
                    }
                }
            }
        }

        # 调用preview方法，quiet=True
        result = job.preview(quiet=True)

        # 验证结果
        self.assertIsNotNone(result)
        self.assertIn("target_node_1", result)

        # 验证日志没有被调用
        mock_logger_info.assert_not_called()

if __name__ == '__main__':
    unittest.main() 