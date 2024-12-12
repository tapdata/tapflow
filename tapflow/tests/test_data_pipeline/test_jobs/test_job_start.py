from tapflow.tests.test_data_pipeline.test_jobs import BaseJobTest
from unittest.mock import patch, Mock
from tapflow.lib.data_pipeline.job import Job

class TestJobStart(BaseJobTest):
    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.put')
    @patch('tapflow.lib.data_pipeline.job.Job.save')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_start_when_status_is_edit(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                                     mock_save, mock_req_put, mock_logger_info, mock_logger_warn, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        mock_time.sleep = Mock()  # 模拟sleep函数

        # 模拟status方法抛出KeyError，然后返回"edit"
        mock_status.side_effect = [KeyError("status"), "edit", "running"]

        # 模拟save方法返回True
        mock_save.return_value = True

        # 模拟start_task的响应
        with patch('tapflow.lib.backend_apis.task.TaskApi.start_task') as mock_start_task:
            mock_start_task.return_value = (
                {"id": "test_job_id", "status": "running"},  # data
                True  # ok
            )

            result = job.start(quiet=False)

            # 验证结果
            self.assertTrue(result)
            # 验证save方法被调用
            mock_save.assert_called_once()
            # 验证start_task被调用
            mock_start_task.assert_called_once_with(job.id)
            # 验证成功日志被调用
            mock_logger_info.assert_called_once_with("{}", "Task start succeed")

    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.req.put')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_start_when_already_running(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                                      mock_req_put, mock_logger_warn):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)

        # 模拟任务状态为running
        mock_status.return_value = "running"

        result = job.start(quiet=False)

        # 验证结果
        self.assertTrue(result)
        # 验证警告日志被调用
        mock_logger_warn.assert_called_once_with(
            "Task {} status is {}, need not start", 
            "test_job", "running"
        )
        # 验证put请求没有被调用
        mock_req_put.assert_not_called()

    @patch('tapflow.lib.data_pipeline.job.logger.fwarn')
    @patch('tapflow.lib.data_pipeline.job.req.put')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_start_with_none_id(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                               mock_req_put, mock_logger_fwarn):
        # 创建Job实例，使用create_job方法并传入pipeline
        mock_pipeline = self.create_mock_pipeline()
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get, name="test_none_id", pipeline=mock_pipeline)

        result = job.start()

        # 验��结果
        self.assertFalse(result)
        # 验证错误日志被调用
        mock_logger_fwarn.assert_called_once_with("save job fail")
        # 验证put请求没有被调用
        mock_req_put.assert_not_called()

    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.req.put')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_start_schedule_limit(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                                mock_req_put, mock_logger_warn, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        mock_time.sleep = Mock()  # 模拟sleep函数

        # 模拟任务状态为edit
        mock_status.return_value = "edit"

        # 模拟start_task的响应
        with patch('tapflow.lib.backend_apis.task.TaskApi.start_task') as mock_start_task:
            mock_start_task.return_value = (
                [{"code": "Task.ScheduleLimit", "message": "Schedule limit reached"}],  # data
                True  # ok
            )

            result = job.start(quiet=False)

            # 验证结果
            self.assertFalse(result)
            # 验证警告日志被调用
            mock_logger_warn.assert_called_once_with("{}", "Schedule limit reached")
            # 验证start_task被调用
            mock_start_task.assert_called_once_with(job.id)

if __name__ == '__main__':
    unittest.main() 