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

        # 设置job的必要属性
        job.job = {
            "id": "test_job_id",
            "name": "test_job",
            "dag": {"nodes": [], "edges": []},
            "syncType": "migrate"
        }
        
        # 创建mock dag对象
        mock_dag = Mock()
        mock_dag.node_map = {"node1": {"id": "node1"}}
        mock_dag.dag = {"nodes": [], "edges": []}
        mock_dag.get_target_node = Mock(return_value=Mock(id="target_node_1"))
        job.dag = mock_dag

        # 模拟find_final_target返回值
        job.find_final_target = Mock(return_value=["target_node_1"])

        # 模拟task_preview的响应
        with patch('tapflow.lib.backend_apis.task.TaskApi.task_preview') as mock_task_preview:
            mock_task_preview.return_value = (
                {
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
                },
                True  # ok
            )

            # 调用preview方法
            result = job.preview(quiet=False)

            # 验证结果
            self.assertIsNotNone(result)
            self.assertIn("target_node_1", result)
            self.assertIn("other_node", result)
            self.assertEqual(len(result["target_node_1"]["data"]), 2)
            self.assertEqual(len(result["other_node"]["data"]), 1)

            # 验证task_preview被正确调用
            mock_task_preview.assert_called_once_with(job.job)

            # 验证日志调用
            mock_logger_info.assert_called_once_with("preview view took {} ms", 1000)

    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.req.get')
    @patch('tapflow.lib.data_pipeline.job.req.post')
    @patch('tapflow.lib.data_pipeline.job.logger.info')
    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.data_pipeline.job.time')
    def test_preview_response_code_not_ok(self, mock_time, mock_logger_warn, mock_logger_info, 
                                        mock_req_post, mock_req_get, mock_get_obj, mock_client_cache):
        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get)
        mock_time.time.side_effect = [0, 1]

        # 设置job的必要属性
        job.job = {
            "id": "test_job_id",
            "name": "test_job",
            "dag": {"nodes": [], "edges": []},
            "syncType": "migrate"
        }
        
        # 创建mock dag对象
        mock_dag = Mock()
        mock_dag.node_map = {"node1": {"id": "node1"}}
        mock_dag.dag = {"nodes": [], "edges": []}
        mock_dag.get_target_node = Mock(return_value=Mock(id="target_node_1"))
        job.dag = mock_dag

        # 模拟find_final_target返回值
        job.find_final_target = Mock(return_value=["target_node_1"])

        # 模拟task_preview的响应返回错误
        with patch('tapflow.lib.backend_apis.task.TaskApi.task_preview') as mock_task_preview:
            mock_task_preview.return_value = (
                None,  # data
                False  # ok
            )

            # 调用preview方法
            result = job.preview(quiet=False)

            # 验证结果
            self.assertIsNone(result)

            # 验证task_preview被正确调用
            mock_task_preview.assert_called_once_with(job.job)

            # 验证警告日志被调用
            mock_logger_warn.assert_called_once_with("{}", "preview failed")
            # 验证info日志没有被调用
            mock_logger_info.assert_not_called()

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

        # 设置job的必要属性
        job.job = {
            "id": "test_job_id",
            "name": "test_job",
            "dag": {"nodes": [], "edges": []},
            "syncType": "migrate"
        }
        
        # 创建mock dag对象
        mock_dag = Mock()
        mock_dag.node_map = {"node1": {"id": "node1"}}
        mock_dag.dag = {"nodes": [], "edges": []}
        mock_dag.get_target_node = Mock(return_value=Mock(id="target_node_1"))
        job.dag = mock_dag

        # 模拟find_final_target返回值
        job.find_final_target = Mock(return_value=["target_node_1"])

        # 模拟task_preview的��应
        with patch('tapflow.lib.backend_apis.task.TaskApi.task_preview') as mock_task_preview:
            mock_task_preview.return_value = (
                {
                    "nodeResult": {
                        "target_node_1": {
                            "data": [{"id": 1, "name": "test1"}]
                        }
                    }
                },
                True  # ok
            )

            # 调用preview方法，quiet=True
            result = job.preview(quiet=True)

            # 验证结果
            self.assertIsNotNone(result)
            self.assertIn("target_node_1", result)
            self.assertEqual(result["target_node_1"]["data"][0]["id"], 1)
            self.assertEqual(result["target_node_1"]["data"][0]["name"], "test1")

            # 验证task_preview被正确调用
            mock_task_preview.assert_called_once_with(job.job)

            # 验证日志没有被调用
            mock_logger_info.assert_not_called()

if __name__ == '__main__':
    unittest.main() 