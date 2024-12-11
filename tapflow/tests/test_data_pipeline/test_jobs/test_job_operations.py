import unittest
from unittest.mock import patch, Mock
from tapflow.lib.data_pipeline.job import Job
from tapflow.tests.test_data_pipeline.test_jobs import BaseJobTest

class TestJobOperations(BaseJobTest):
    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.patch')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_reset_when_running(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                              mock_patch, mock_logger_info, mock_logger_warn):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        
        # 模拟任务状态为running
        mock_status.return_value = "running"
        
        result = job.reset(quiet=False)
        
        # 验证结果
        self.assertFalse(result)
        # 验证警告日志被调用
        mock_logger_warn.assert_called_once_with(
            "Task status is {}, can not reset, please stop it first", 
            "running"
        )
        # 验证patch请求没有被调用
        mock_patch.assert_not_called()

    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.patch')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_reset_success(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                          mock_patch, mock_logger_info, mock_logger_warn):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        
        # 模拟任务状态为stopped
        mock_status.return_value = "stopped"
        
        # 模拟patch请求成功
        mock_patch.return_value.status_code = 200
        mock_patch.return_value.json.return_value = {"code": "ok"}
        
        result = job.reset(quiet=False)
        
        # 验证结果
        self.assertTrue(result)
        # 验证patch请求被正确调用
        mock_patch.assert_called_once_with(
            "/Task/batchRenew",
            params={"taskIds": "test_job_id"}
        )
        # 验证成功日志被调用
        mock_logger_info.assert_called_once_with("{}", "Task reset success")

    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.patch')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_reset_failure(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                          mock_patch, mock_logger_info, mock_logger_warn):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        
        # 模拟任务状态为stopped
        mock_status.return_value = "stopped"
        
        # 模拟patch请求失败
        mock_patch.return_value.json.return_value = {"code": "error"}
        
        result = job.reset(quiet=False)
        
        # 验证结果
        self.assertFalse(result)
        # 验证patch请求被正确调用
        mock_patch.assert_called_once_with(
            "/Task/batchRenew",
            params={"taskIds": "test_job_id"}
        )
        # 验证失败日志被调用
        mock_logger_warn.assert_called_once_with("{}", "Task reset failed")

    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.put')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_stop_when_not_running(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                                 mock_put, mock_logger_info, mock_logger_warn, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        
        # 模拟任务状态为stopped
        mock_status.return_value = "stopped"
        
        result = job.stop(quiet=False)
        
        # 验证结果
        self.assertFalse(result)
        # 验证警告日志被调用
        mock_logger_warn.assert_called_once_with(
            "Task status is {}, not running, can not stop it", 
            "stopped"
        )
        # 验证put请求没被调用
        mock_put.assert_not_called()

    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.put')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_stop_timeout(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                         mock_put, mock_logger_info, mock_logger_warn, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        
        # 模拟时间流逝
        mock_time.time.side_effect = [0, 61]  # 第一次调用返回0，第二次调用返回61，超过默认超时时间60
        
        # 模拟任务状态
        mock_status.side_effect = ["running", "running"]  # 状态一直是running
        
        result = job.stop(quiet=False)
        
        # 验证结果
        self.assertFalse(result)
        # 验证警告日志被调用
        mock_logger_warn.assert_called_once_with("{}", "Task stopped failed")

    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.put')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_stop_success(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                         mock_put, mock_logger_info, mock_logger_warn, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        
        # 模拟时间流逝
        mock_time.time.side_effect = [0, 1]  # 确保不会超时
        
        # 模拟任务状态
        mock_status.side_effect = ["running", "stop"]  # 第一次是running，第二次是stop
        
        result = job.stop(quiet=False)
        
        # 验证结果
        self.assertTrue(result)
        # 验证成功日志被调用
        mock_logger_info.assert_called_once_with("{}", "Task stopped successfully")

    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.put')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_stop_with_stopping_status_no_sync(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                                             mock_put, mock_logger_info, mock_logger_warn, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        
        # 模拟时间流逝
        mock_time.time.side_effect = [0, 1]  # 确保不会超时
        
        # 模拟任务状态
        mock_status.side_effect = ["running", "stopping"]  # 第一次是running，第二次是stopping
        
        result = job.stop(sync=False, quiet=False)  # 设置sync=False
        
        # 验证结果
        self.assertTrue(result)
        # 验证没有调用任何日志
        mock_logger_info.assert_not_called()
        mock_logger_warn.assert_not_called()

    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.logger.fwarn')
    @patch('tapflow.lib.data_pipeline.job.req.delete')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_delete_when_running(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                               mock_req_delete, mock_logger_fwarn, mock_logger_warn):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        
        # 模拟任务状态为running
        mock_status.return_value = "running"
        
        result = job.delete(quiet=False)
        
        # 验证结果
        self.assertIsNone(result)  # 当任务运行时，delete应该返回None
        # 验证警告日志被调用
        mock_logger_fwarn.assert_called_once_with(
            "job status is {}, please stop it first before delete it", 
            "running"
        )
        mock_logger_warn.assert_called_once_with(
            "job status is {}, please stop it first before delete it", 
            "running"
        )
        # 验证delete请求没有被调用
        mock_req_delete.assert_not_called()

    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.delete')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_delete_success(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                          mock_req_delete, mock_logger_info, mock_logger_warn):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        
        # 模拟任务状态为stopped
        mock_status.return_value = "stopped"
        
        # 模拟delete请求成功
        mock_req_delete.return_value.status_code = 200
        mock_req_delete.return_value.json.return_value = {"code": "ok"}
        
        result = job.delete(quiet=False)
        
        # 验证结果
        self.assertTrue(result)
        # 验证delete请求被正确调用
        mock_req_delete.assert_called_once_with(
            "/Task/batchDelete",
            params={"taskIds": "test_job_id"}
        )
        # 验证成功日志被调用
        mock_logger_info.assert_called_once_with("{}", "Task deleted successfully")

    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.put')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_copy_success(self, mock_client_cache, mock_get_obj, mock_req_get, mock_req_put, 
                         mock_logger_info, mock_logger_warn):
        # 初始化测试数据
        self.initialize_client_cache(mock_client_cache)
        mock_get_obj.return_value = Mock(id="test_job_id")

        # 模拟req.get的响应，包括原始任务和复制后的任务
        mock_req_get.side_effect = [
            # 原始任务的响应
            Mock(status_code=200, json=lambda: {
                "data": {
                    "id": "test_job_id",
                    "name": "test_job",
                    "dag": {"nodes": [], "edges": []},
                    "syncType": "migrate"
                }
            }),
            # 复制后任务的响应
            Mock(status_code=200, json=lambda: {
                "data": {
                    "id": "copied_job_id",
                    "name": "test_job_copy",
                    "dag": {"nodes": [], "edges": []},
                    "syncType": "migrate"
                }
            })
        ]

        # 模拟copy请求成功
        mock_req_put.return_value.status_code = 200
        mock_req_put.return_value.json.return_value = {
            "code": "ok",
            "data": {
                "id": "copied_job_id",
                "name": "test_job_copy",
                "dag": {"nodes": [], "edges": []},
                "syncType": "migrate"
            }
        }

        job = Job(id="test_job_id")
        job.name = "test_job"  # 显式设置name属性
        copied_job = job.copy(quiet=False)

        # 验证结果
        self.assertIsInstance(copied_job, Job)
        self.assertEqual(copied_job.id, "copied_job_id")
        self.assertEqual(copied_job.name, "test_job_copy")

        # 验证client_cache被正确更新
        self.assertIn("copied_job_id", mock_client_cache["jobs"]["id_index"])
        self.assertIn("test_job_copy", mock_client_cache["jobs"]["name_index"])

        # 验证put请求被正确调用
        mock_req_put.assert_called_once_with("/Task/copy/test_job_id")

        # 验证成功日志被调用
        mock_logger_info.assert_called_once_with(
            "{}",
            "Copy task 'test_job' to 'test_job_copy' success"
        )

    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.req.put')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_copy_request_failure(self, mock_client_cache, mock_get_obj, mock_req_get, mock_req_put, 
                                mock_logger_warn):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)

        # 模拟copy请求失败
        mock_req_put.return_value.status_code = 500

        result = job.copy()

        # 验证结果
        self.assertFalse(result)
        # 验证警告日志被调用
        mock_logger_warn.assert_called_once_with("{}", "Task copy failed")

    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.req.delete')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_delete_with_none_id(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                                mock_req_delete, mock_logger_warn):
        # 初始化测试数据
        self.initialize_client_cache(mock_client_cache)
        mock_get_obj.return_value = Mock(id=None)  # 设置id为None
        mock_req_get.return_value.json.return_value = {
            "data": {
                "id": None,
                "name": "test_job",
                "dag": {"nodes": [], "edges": []},
                "syncType": "migrate"
            }
        }
        
        job = Job(name="test_none_id", pipeline=Mock(id="test_pipeline_id", dag={"nodes": [], "edges": []}))
        result = job.delete()
        
        # 验证结果
        self.assertFalse(result)
        # 验证delete请求没有被调用
        mock_req_delete.assert_not_called()

    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.req.delete')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_delete_request_failure(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                                  mock_req_delete, mock_logger_warn):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        
        # 模拟任务状态为stopped
        mock_status.return_value = "stopped"
        
        # 模拟delete请求失败
        mock_req_delete.return_value.status_code = 500
        
        result = job.delete(quiet=False)
        
        # 验证结果
        self.assertFalse(result)
        # 验证警告日志被调用
        mock_logger_warn.assert_called_once_with("{}", "Task delete failed")

    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.req.delete')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_delete_response_code_not_ok(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                                       mock_req_delete, mock_logger_warn):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        
        # 模拟任务状态为stopped
        mock_status.return_value = "stopped"
        
        # 模拟delete请求返回非ok状态
        mock_req_delete.return_value.status_code = 200
        mock_req_delete.return_value.json.return_value = {"code": "error"}
        
        result = job.delete(quiet=False)
        
        # 验证结果
        self.assertFalse(result)
        # 验证警告日志被调用
        mock_logger_warn.assert_called_once_with("{}", "Task delete failed")

    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.req.put')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_copy_response_code_not_ok(self, mock_client_cache, mock_get_obj, mock_req_get, mock_req_put, 
                                     mock_logger_warn):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)

        # 模拟copy请求返回非ok状态
        mock_req_put.return_value.status_code = 200
        mock_req_put.return_value.json.return_value = {"code": "error"}

        result = job.copy()

        # 验证结果
        self.assertFalse(result)
        # 验证警告日志被调用
        mock_logger_warn.assert_called_once_with("{}", "Task copy failed")

    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.req.put')
    @patch('tapflow.lib.data_pipeline.job.Job.status')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_stop_with_none_id(self, mock_client_cache, mock_get_obj, mock_req_get, mock_status, 
                              mock_put, mock_logger_warn, mock_time):
        # 创建Job实例，使用create_job方法并传入pipeline
        mock_pipeline = self.create_mock_pipeline()
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get, name="test_none_id", pipeline=mock_pipeline)
        
        # 模拟任务状态为running
        mock_status.return_value = "running"
        
        result = job.stop()
        
        # 验证结果
        self.assertFalse(result)
        # 验证put请求没有被调用
        mock_put.assert_not_called()

if __name__ == '__main__':
    unittest.main() 