import unittest
from tapflow.tests.test_data_pipeline.test_jobs import BaseJobTest
from unittest.mock import patch, Mock, call
from tapflow.lib.data_pipeline.job import Job

class TestJobSave(BaseJobTest):
    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.fwarn')
    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.backend_apis.task.TaskApi.create_task')
    @patch('tapflow.lib.backend_apis.task.TaskApi.update_task')
    @patch('tapflow.lib.backend_apis.task.TaskApi.confirm_task')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    @patch('tapflow.lib.data_pipeline.job.system_server_conf', new_callable=dict)
    @patch('tapflow.lib.data_pipeline.job.req.get')
    def test_save_new_job_success(self, mock_req_get, mock_system_conf, mock_client_cache, mock_get_obj,
                                mock_confirm_task, mock_update_task, mock_create_task,
                                mock_logger_warn, mock_logger_fwarn, mock_time):
        # 创建mock pipeline和dag
        mock_pipeline = self.create_mock_pipeline()
        mock_time.time.return_value = self.mock_time / 1000

        # 模拟system_server_conf
        mock_system_conf.update({
            "user_id": "test_user_id",
            "username": "test_user"
        })

        # 模拟create_task返回值
        mock_create_task.return_value = ({
            "id": "new_job_id",
            "name": "new_job",
            "dag": {"nodes": [], "edges": []},
            "syncType": "migrate",
            "status": "edit"
        }, True)

        # 模拟update_task和confirm_task返回值
        mock_update_task.return_value = ({
            "id": "new_job_id",
            "name": "new_job",
            "dag": {"nodes": [], "edges": []},
            "syncType": "migrate",
            "status": "edit"
        }, True)
        
        mock_confirm_task.return_value = ({
            "id": "new_job_id",
            "name": "new_job",
            "dag": {"nodes": [], "edges": []},
            "syncType": "migrate",
            "status": "edit"
        }, True)

        # 创建Job实例
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get, name="new_job", pipeline=mock_pipeline)
        result = job.save()

        # 验证结果
        self.assertTrue(result)
        self.assertEqual(job.id, "new_job_id")

        # 验证create_task被正确调用
        expected_task_data = {
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
            "crontabExpressionFlag": False,
            "id": "new_job_id"
        }
        mock_create_task.assert_called_once_with(expected_task_data)

        # 验证update_task被正确调用
        expected_update_data = {
            "dag": {
                "nodes": [],
                "edges": []
            },
            "editVersion": self.mock_time,
            "id": "new_job_id"
        }
        mock_update_task.assert_called_once_with(expected_update_data)

        # 验证confirm_task被正确调用
        mock_confirm_task.assert_called_once_with("new_job_id", expected_task_data)

    @patch('tapflow.lib.data_pipeline.job.time')
    @patch('tapflow.lib.data_pipeline.job.logger.fwarn')
    @patch('tapflow.lib.data_pipeline.job.logger.warn')
    @patch('tapflow.lib.backend_apis.task.TaskApi.create_task')
    @patch('tapflow.lib.backend_apis.task.TaskApi.update_task')
    @patch('tapflow.lib.backend_apis.task.TaskApi.confirm_task')
    @patch('tapflow.lib.op_object.get_obj')
    @patch('tapflow.lib.data_pipeline.job.client_cache', new_callable=dict)
    @patch('tapflow.lib.data_pipeline.job.system_server_conf', new_callable=dict)
    @patch('tapflow.lib.data_pipeline.job.req.get')
    def test_save_existing_job_update(self, mock_req_get, mock_system_conf, mock_client_cache, mock_get_obj,
                                    mock_confirm_task, mock_update_task, mock_create_task,
                                    mock_logger_warn, mock_logger_fwarn, mock_time):
        # 创建mock pipeline和dag
        mock_pipeline = self.create_mock_pipeline()
        mock_pipeline.dag.dag = {
            "nodes": [{"id": "node1", "name": "Node 1"}],
            "edges": [{"source": "node1", "target": "node2"}]
        }
        mock_time.time.return_value = self.mock_time / 1000

        # 模拟system_server_conf
        mock_system_conf.update({
            "user_id": "test_user_id",
            "username": "test_user"
        })

        # 模拟update_task和confirm_task返回值
        mock_update_task.return_value = ({
            "id": "test_job_id",
            "name": "test_job",
            "dag": mock_pipeline.dag.dag,
            "syncType": "migrate",
            "status": "edit"
        }, True)
        
        mock_confirm_task.return_value = ({
            "id": "test_job_id",
            "name": "test_job",
            "dag": mock_pipeline.dag.dag,
            "syncType": "migrate",
            "status": "edit"
        }, True)

        # 创建Job实例并设置ID和DAG
        job = self.create_job(mock_client_cache, mock_get_obj, mock_req_get, job_id="test_job_id", pipeline=mock_pipeline)
        job.job = {
            "id": "test_job_id",
            "name": "test_job",
            "dag": mock_pipeline.dag.dag,
            "syncType": "migrate",
            "status": "edit",
            "user_id": "test_user_id",
            "customId": "test_user_id",
            "createUser": "test_user",
            "syncPoints": [],
            "dynamicAdjustMemoryUsage": True,
            "crontabExpressionFlag": False
        }
        result = job.save()

        # 验证结果
        self.assertTrue(result)

        # 验证update_task被正确调用
        expected_update_data = {
            "dag": mock_pipeline.dag.dag,
            "editVersion": self.mock_time,
            "id": "test_job_id"
        }
        mock_update_task.assert_called_once_with(expected_update_data)

        # 验证confirm_task被正确调用
        expected_confirm_data = {
            "id": "test_job_id",
            "name": "test_job",
            "dag": mock_pipeline.dag.dag,
            "syncType": "migrate",
            "status": "edit",
            "user_id": "test_user_id",
            "customId": "test_user_id",
            "createUser": "test_user",
            "syncPoints": [],
            "dynamicAdjustMemoryUsage": True,
            "crontabExpressionFlag": False,
            "editVersion": self.mock_time
        }
        mock_confirm_task.assert_called_once_with("test_job_id", expected_confirm_data)

        # 验证create_task没有被调用（因为是更新现有任务）
        mock_create_task.assert_not_called()

if __name__ == '__main__':
    unittest.main() 