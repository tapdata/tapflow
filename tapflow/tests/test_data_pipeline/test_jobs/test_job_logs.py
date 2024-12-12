from tapflow.tests.test_data_pipeline.test_jobs import BaseJobTest
from unittest.mock import patch, Mock
from tapflow.lib.data_pipeline.job import Job

class TestJobLogs(BaseJobTest):
    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.TaskApi')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_logs_success(self, mock_client_cache, mock_get_obj, mock_req_get, mock_task_api, 
                         mock_req_post, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        mock_time.time.return_value = self.mock_time / 1000

        # 设置job的id
        job.id = "test_job_id"

        # 创建一个Mock对象来模拟TaskApi实例
        mock_task_api_instance = Mock()
        mock_task_api_instance.get_task_by_id.return_value = {
            "id": "test_job_id",
            "taskRecordId": "record_123"
        }
        mock_task_api_instance.get_task_logs.return_value = (
            {
                "items": [
                    {"message": "log1", "level": "info", "timestamp": 1234567890},
                    {"message": "log2", "level": "warn", "timestamp": 1234567891}
                ]
            },
            True  # ok
        )
        mock_task_api.return_value = mock_task_api_instance

        # 重新设置job的task_api
        job.task_api = mock_task_api_instance

        # 调用logs方法
        logs = job.logs(limit=2, level="info", quiet=True)

        # 验证结果
        self.assertEqual(len(logs), 2)
        self.assertEqual(logs[0]["message"], "log1")
        self.assertEqual(logs[1]["message"], "log2")

        # 验证get_task_logs被正确调用
        mock_task_api_instance.get_task_logs.assert_called_once_with(
            "info",  # level
            2,      # limit
            "test_job_id",  # task_id
            "record_123",   # task_record_id
            int(self.mock_time/1000*1000)-3600*100000,  # start
            int(self.mock_time/1000*1000)  # end
        )

    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.TaskApi')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_logs_request_failure(self, mock_client_cache, mock_get_obj, mock_req_get, mock_task_api, 
                                mock_req_post, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        mock_time.time.return_value = self.mock_time / 1000

        # 设置job的id
        job.id = "test_job_id"

        # 模拟TaskApi响应
        mock_task_api_instance = Mock()
        mock_task_api_instance.get_task_by_id.return_value = {
            "id": "test_job_id",
            "taskRecordId": "record_123"
        }
        # 模拟请求失败
        mock_task_api_instance.get_task_logs.return_value = ([], False)  # 返回空列表和失败状态
        mock_task_api.return_value = mock_task_api_instance

        # 重新设置job的task_api
        job.task_api = mock_task_api_instance

        # 调用logs方法
        logs = job.logs()

        # 验证结果
        self.assertEqual(logs, [])

    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.TaskApi')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_logs_response_code_not_ok(self, mock_client_cache, mock_get_obj, mock_req_get, mock_task_api, 
                                     mock_req_post, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        mock_time.time.return_value = self.mock_time / 1000

        # 设置job的id
        job.id = "test_job_id"

        # 模拟TaskApi响应
        mock_task_api_instance = Mock()
        mock_task_api_instance.get_task_by_id.return_value = {
            "id": "test_job_id",
            "taskRecordId": "record_123"
        }
        # 模拟响应code不是ok
        mock_task_api_instance.get_task_logs.return_value = ([], False)  # 返回空列表和失败状态
        mock_task_api.return_value = mock_task_api_instance

        # 重新设置job的task_api
        job.task_api = mock_task_api_instance

        # 调用logs方法
        logs = job.logs()

        # 验证结果
        self.assertEqual(logs, [])

    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.TaskApi')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_logs_with_print(self, mock_client_cache, mock_get_obj, mock_req_get, mock_task_api, 
                            mock_req_post, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        mock_time.time.return_value = self.mock_time / 1000

        # 设置job的id
        job.id = "test_job_id"  # 显式设置job.id

        # 模拟TaskApi响应
        mock_task_api_instance = Mock()
        mock_task_api_instance.get_task_by_id.return_value = {
            "id": "test_job_id",
            "taskRecordId": "record_123"
        }
        mock_task_api_instance.get_task_logs.return_value = (
            {
                "items": [
                    {"message": "log1", "level": "info", "timestamp": 1234567890},
                    {"message": "log2", "level": "warn", "timestamp": 1234567891}
                ]
            },
            True  # ok
        )
        mock_task_api.return_value = mock_task_api_instance

        # 重新设置job的task_api
        job.task_api = mock_task_api_instance

        # 调用logs方法，quiet=False以打印日志
        logs = job.logs(quiet=False)

        # 验证结果
        self.assertEqual(len(logs), 2)
        self.assertEqual(logs[0]["message"], "log1")
        self.assertEqual(logs[1]["message"], "log2")

        # 验证get_task_logs被正确调用
        mock_task_api_instance.get_task_logs.assert_called_once_with(
            "info",  # level
            100,    # limit
            "test_job_id",  # task_id
            "record_123",   # task_record_id
            int(self.mock_time/1000*1000)-3600*100000,  # start
            int(self.mock_time/1000*1000)  # end
        )

if __name__ == '__main__':
    unittest.main() 