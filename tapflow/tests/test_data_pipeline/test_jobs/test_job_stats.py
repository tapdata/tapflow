import unittest
from tapflow.tests.test_data_pipeline.test_jobs import BaseJobTest
from unittest.mock import patch, Mock
from tapflow.lib.data_pipeline.job import Job, JobStats

class TestJobStats(BaseJobTest):
    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.TaskApi')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_stats_success(self, mock_client_cache, mock_get_obj, mock_req_get, mock_task_api, 
                          mock_req_post, mock_logger_info, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        mock_time.time.return_value = self.mock_time / 1000

        # 创建一个Mock对象来模拟TaskApi实例
        mock_task_api_instance = Mock()
        mock_task_api.return_value = mock_task_api_instance

        # 设置get_task_by_id的返回值
        mock_response = {
            "status": "running",
            "taskRecordId": "record_123"
        }
        mock_task_api_instance.get_task_by_id = Mock(return_value=mock_response)

        # 设置get_task_measurement的返回值
        mock_measurement = {
            "totalData": {
                "data": {
                    "samples": {
                        "data": [{
                            "outputQps": 100,
                            "tableTotal": 10,
                            "inputInsertTotal": 1000,
                            "inputUpdateTotal": 200,
                            "inputDeleteTotal": 50,
                            "outputInsertTotal": 1000,
                            "outputUpdateTotal": 200,
                            "outputDeleteTotal": 50,
                            "snapshotDoneAt": 1234567890,
                            "snapshotStartAt": 1234567000,
                            "inputQps": 90,
                            "outputQpsAvg": 95,
                            "outputQpsMax": 150,
                            "snapshotRowTotal": 2000,
                            "replicateLag": 100,
                            "snapshotTableTotal": 8,
                            "lastFiveMinutesQps": 98
                        }]
                    }
                }
            }
        }
        mock_task_api_instance.get_task_measurement = Mock(return_value=mock_measurement)

        # 重新设置job的task_api
        job.task_api = mock_task_api_instance

        job_stats = job.stats(quiet=False)

        # 验证结果
        self.assertIsInstance(job_stats, JobStats)
        self.assertEqual(job_stats.qps, 100)
        self.assertEqual(job_stats.total, 10)
        self.assertEqual(job_stats.input_insert, 1000)
        self.assertEqual(job_stats.input_update, 200)
        self.assertEqual(job_stats.input_delete, 50)
        self.assertEqual(job_stats.output_insert, 1000)
        self.assertEqual(job_stats.output_update, 200)
        self.assertEqual(job_stats.output_Delete, 50)
        self.assertEqual(job_stats.snapshot_done_at, 1234567890)
        self.assertEqual(job_stats.snapshot_start_at, 1234567000)
        self.assertEqual(job_stats.input_qps, 90)
        self.assertEqual(job_stats.output_qps, 100)
        self.assertEqual(job_stats.output_qps_avg, 95)
        self.assertEqual(job_stats.output_qps_max, 150)
        self.assertEqual(job_stats.snapshot_row_total, 2000)
        self.assertEqual(job_stats.replicate_lag, 100)
        self.assertEqual(job_stats.table_total, 10)
        self.assertEqual(job_stats.snapshot_table_total, 8)
        self.assertEqual(job_stats.last_five_minutes_qps, 98)

        # 验证日志调用
        mock_logger_info.assert_called_once_with(
            "Flow current status is: {}, qps is: {}, total rows: {}, delay is: {}ms",
            "running", 100, 2000, 100
        )

        # 验证get_task_by_id和get_task_measurement被调用
        mock_task_api_instance.get_task_by_id.assert_called_once_with(job.id)
        mock_task_api_instance.get_task_measurement.assert_called_once_with(job.id, "record_123")

    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.TaskApi')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    def test_stats_with_empty_samples(self, mock_client_cache, mock_get_obj, mock_req_get, mock_task_api, 
                                    mock_req_post, mock_logger_info, mock_time):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        mock_time.time.return_value = self.mock_time / 1000

        # 创建一个Mock对象来模拟TaskApi实例
        mock_task_api_instance = Mock()
        mock_task_api.return_value = mock_task_api_instance

        # 设置get_task_by_id的返回值
        mock_response = {
            "status": "running",
            "taskRecordId": "record_123"
        }
        mock_task_api_instance.get_task_by_id = Mock(return_value=mock_response)

        # 重新设置job的task_api
        job.task_api = mock_task_api_instance

        # 模拟stats请求响应，返回空samples
        mock_req_post.return_value.json.return_value = {
            "data": {
                "totalData": {
                    "data": {
                        "samples": {
                            "data": []
                        }
                    }
                }
            }
        }

        job_stats = job.stats(quiet=False)

        # 验证结果
        self.assertIsInstance(job_stats, JobStats)
        self.assertEqual(job_stats.qps, 0)
        self.assertEqual(job_stats.total, 0)
        self.assertEqual(job_stats.input_insert, 0)
        self.assertEqual(job_stats.input_update, 0)
        self.assertEqual(job_stats.input_delete, 0)
        self.assertEqual(job_stats.output_insert, 0)
        self.assertEqual(job_stats.output_update, 0)
        self.assertEqual(job_stats.output_Delete, 0)

        # 验证get_task_by_id被调用
        mock_task_api_instance.get_task_by_id.assert_called_once_with(job.id)

if __name__ == '__main__':
    unittest.main() 