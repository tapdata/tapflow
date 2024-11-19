import unittest
from unittest.mock import Mock
import time

class BaseJobTest(unittest.TestCase):
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
        self.mock_time = int(time.time() * 1000)

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

    def create_job(self, mock_client_cache, mock_get_obj, mock_req_get, job_id="test_job_id", name=None, pipeline=None):
        """
        创建Job实例的通用方法
        """
        # 初始化测试数据
        self.initialize_client_cache(mock_client_cache)

        # 设置mock_get_obj的返回值
        mock_obj = Mock()
        mock_obj.id = job_id
        mock_get_obj.return_value = mock_obj

        # 设置mock_req_get的返回值
        mock_req_get.return_value.json.return_value = {
            "data": {
                "id": job_id,
                "name": name or "test_job",
                "dag": {"nodes": [], "edges": []},
                "syncType": "migrate"
            }
        }

        # 如果需要pipeline但没有提供，创建一个默认的mock pipeline
        if pipeline is None and name is not None:
            pipeline = self.create_mock_pipeline()

        # 根据提供的参数创建Job实例
        if name is not None:
            job = self.create_job_with_name(name, pipeline)
        else:
            job = self.create_job_with_id(job_id)
            job.name = name or "test_job"
            job.jobType = "migrate"
            if pipeline:
                job.pipeline = pipeline
                job.dag = pipeline.dag

        return job

    def create_job_with_id(self, job_id):
        """创建带ID的Job实例"""
        from tapflow.lib.data_pipeline.job import Job
        job = Job(id=job_id)
        job.jobType = "migrate"
        return job

    def create_job_with_name(self, name, pipeline):
        """创建带名称和pipeline的Job实例"""
        from tapflow.lib.data_pipeline.job import Job
        job = Job(name=name, pipeline=pipeline)
        job.jobType = "migrate"
        return job

    def create_mock_pipeline(self):
        """创建模拟的pipeline对象"""
        mock_pipeline = Mock()
        mock_pipeline.dag = Mock()
        mock_pipeline.dag.jobType = "migrate"
        mock_pipeline.dag.dag = {"nodes": [], "edges": []}
        mock_pipeline.dag.setting = {"syncPoints": []}
        mock_pipeline.dag.to_dict.return_value = {}
        mock_pipeline.sources = []  # 添加可迭代的sources属性
        mock_pipeline.target = None
        return mock_pipeline
