from concurrent.futures import ThreadPoolExecutor
import importlib
import os
import queue
import re
import shutil
import time
import traceback
from typing import Callable

import yaml

from tapflow.lib.op_object import show_jobs
from .projectInterface import ProjectInterface
from tapflow.lib.utils.log import logger
from tapflow.lib.data_pipeline.pipeline import Flow, Pipeline
from tapflow.lib.request import req
from tapflow.lib.cache import client_cache


# 当前脚本文件位置
CURRENT_FILE_PATH = os.path.dirname(os.path.abspath(__file__))
# template文件位置
TEMPLATE_FILE_PATH = os.path.join(CURRENT_FILE_PATH, "templates")


class ProjectRuntime:
    """
    ProjectRuntime 记录Project运行时状态
    """
    flows: list[Flow] = []
    depended_flows: dict[str, list[str]] = {} # 记录每个flow依赖的flow
    dag_degree: dict[str, int] = {}
    flows_depends_on: dict[str, list[str]] = {} # 记录每个flow被哪些flow依赖, 用于dag_degree的更新

    def __init__(self, flows: list[Flow], depended_flows: dict[str, list[str]], dag_degree: dict[str, int]):
        self.flows = flows
        self.depended_flows = depended_flows
        self.dag_degree = dag_degree
        self.flows_depends_on = {flow.name: [] for flow in flows}
        for flow_name, depends in depended_flows.items():
            for dep in depends:
                self.flows_depends_on[dep.split('.')[0]].append(flow_name)

    def pop_dag_degree(self):
        """
        1. 找出当前的优先执行队列, 即度为0的节点, 并从dag_degree中删除
        2. 将这些节点的依赖的flow的度-1
        3. 返回优先执行队列
        """
        queue = []
        for flow_name, degree in self.dag_degree.items():
            if degree == 0:
                queue.append(flow_name)
                del self.dag_degree[flow_name]
                for flow_name in self.flows_depends_on[flow_name]:
                    self.dag_degree[flow_name] -= 1
        return queue
    

class ProjectScheduler:
    """
    调度Project的运行
    """
    def __init__(self, project: 'Project'):
        self.project = project
        self.executor = ThreadPoolExecutor(max_workers=project.parallelism)
        # 消息队列
        self._event_queue: queue.Queue = queue.Queue()
        # 任务队列, 队列大小为dag的度
        self._queue: list[queue.Queue] = [queue.Queue() for _ in range(max(project.dag_degree.values()) + 1)]
        # 已发生的事件集合
        self._occurred_events: set[str] = set()
        # 任务等待的事件映射 {flow_name: set(waiting_events)}
        self._waiting_events: dict[str, set[str]] = {}

    def _send_event(self, event: str):
        """
        发送事件到事件队列
        :param event: 事件描述符
        """
        self._event_queue.put(event)
        self._occurred_events.add(event)
        self._check_waiting_flows()

    def _check_waiting_flows(self):
        """
        检查等待事件的flow是否可以执行
        """
        ready_flows = []
        for flow_name, waiting_events in self._waiting_events.items():
            if waiting_events.issubset(self._occurred_events):
                # 所有等待的事件都已发生，将flow移动到执行队列
                flow = next(f for f in self.project.flows if f.name == flow_name)
                self._add_queue(flow, 0)  # 移动到优先级最高的队列

                # Start of Selection
                # 从其他优先级队列中移除
                depth = self.project.dag_degree[flow_name]
                self._queue[depth].queue.remove(flow)

                ready_flows.append(flow_name)
        
        # 从waiting_events中移除已经准备好的flow
        for flow_name in ready_flows:
            del self._waiting_events[flow_name]

    def _add_queue(self, flow: Flow, depth: int):
        """
        添加任务到队列
        :param flow: 任务对应的flow
        :param depth: 任务所在的队列深度, 从0开始, 0为最优先
        """
        if depth == 0:
            # 优先队列直接加入
            self._queue[depth].put(flow)
        else:
            # 其他队列需要注册等待事件
            waiting_events = set()
            for dep in self.project.depended_flows.get(flow.name, []):
                waiting_events.add(dep)
            
            self._waiting_events[flow.name] = waiting_events
            self._queue[depth].put(flow)
    
    def _event_initial_sync_start(self, flow: Flow, status: str, milestone: dict):
        if milestone["SNAPSHOT"]["status"] == "RUNNING" and status == "running":
            self._send_event(f"{flow.name}.start")
            self._send_event(f"{flow.name}.initial_sync.start")

    def _event_initial_sync_end(self, flow: Flow, status: str, milestone: dict):
        if milestone["SNAPSHOT"]["status"] == "FINISH" and status == "running":
            self._send_event(f"{flow.name}.start")
            self._send_event(f"{flow.name}.initial_sync.start")
            self._send_event(f"{flow.name}.initial_sync.end")

    def _event_cdc_start(self, flow: Flow, status: str, milestone: dict):
        if milestone["CDC"]["status"] == "FINISH" and status == "running":
            self._send_event(f"{flow.name}.start")
            self._send_event(f"{flow.name}.cdc.start")

    def _event_cdc_end(self, flow: Flow, status: str, milestone: dict):
        if milestone["CDC"]["status"] == "FINISH" and status == "complete":
            self._send_event(f"{flow.name}.start")
            self._send_event(f"{flow.name}.cdc.start")
            self._send_event(f"{flow.name}.cdc.end")

    def _start_flow(self, flow: Flow, load_schema: bool=True) -> bool:
        """
        启动flow
        1. 首先需要对前置任务的target进行loadSchema
        2. 然后启动flow并监听事件
        """
        # if load_schema:
            # logger.info("Loading schema for flow {}...", flow.name)
            # flow.target.load_schema()
        
        logger.info("Running flow {}...", flow.name)
        flow.save().start()

        edit_times, edit_times_limit = 0, 10

        key_error_times, key_error_times_limit = 0, 10

        while True:

            try:
                res = req.get(f"/Task/{flow.id}").json()
                status = res["data"]["status"]
            except KeyError as e:
                key_error_times += 1
                if key_error_times > key_error_times_limit:
                    logger.error("Flow {} start timeout, please check", flow.name)
                    break
                time.sleep(1)
                continue

            if status == "edit":
                edit_times += 1
            if edit_times > edit_times_limit:
                logger.error("Flow {} start timeout or config error, please check", flow.name)
                time.sleep(1)
                break

            try:
                milestone = res["data"]["attrs"].get("milestone", "")
            except KeyError as e:
                key_error_times += 1
                if key_error_times > 5:
                    logger.error("Flow {} milestone not found, please check", flow.name)
                    break
                time.sleep(1)
                continue

            # 收集当前时刻所有需要发送的事件
            current_events = set()
            
            # 任务开始事件
            if status == "running" and f"{flow.name}.start" not in self._occurred_events:
                current_events.add(f"{flow.name}.start")
            
            if milestone:
                flow_type = res["data"]["type"]
                snapshot_status = milestone.get("SNAPSHOT", {}).get("status", "")
                cdc_status = milestone.get("CDC", {}).get("status", "")
                
                # 根据不同类型的flow收集相应事件
                if flow_type in ["initial_sync", "initial_sync+cdc"]:
                    if snapshot_status == "RUNNING":
                        current_events.add(f"{flow.name}.initial_sync.start")
                    elif snapshot_status == "FINISH":
                        current_events.add(f"{flow.name}.initial_sync.end")
                
                if flow_type in ["cdc", "initial_sync+cdc"]:
                    if cdc_status == "FINISH":
                        if status == "running":
                            current_events.add(f"{flow.name}.cdc.start")
                        elif status == "complete":
                            current_events.add(f"{flow.name}.cdc.end")
            
            # 任务结束事件
            if status == "complete":
                current_events.add(f"{flow.name}.end")
                
            # 批量发送新的事件
            new_events = current_events - self._occurred_events
            for event in new_events:
                self._send_event(event)
            
            # 任务完成时退出循环
            if status == "complete":
                logger.info("Flow {} finished", flow.name)
                break
                
            # 避免频繁请求
            time.sleep(1)
                
        return True

    def execute_flow(self, flow: Flow):
        """
        执行flow并发送完成事件
        :param flow: 要执行的flow
        """
        try:
            self._start_flow(flow)
        except Exception as e:
            logger.warn("Flow {} execution failed", flow.name)
            traceback.print_exc()

    def start(self):
        """
        启动调度器
        """
        logger.info("Running project {}...", self.project.name)

        try:
            # 初始化各级队列
            for flow in self.project.flows:
                depth = self.project.dag_degree[flow.name]
                self._add_queue(flow, depth)

            # 开始执行优先队列中的任务
            while True:
                # 检查是否所有队列都为空且所有任务都已完成
                if all(q.empty() for q in self._queue) and len(self._waiting_events) == 0:
                    break

                # 执行优先队列中的任务
                while not self._queue[0].empty():
                    flow = self._queue[0].get()
                    logger.info("Submitting flow {}...", flow.name)
                    self.executor.submit(self.execute_flow, flow)
                
                # 避免CPU空转
                time.sleep(0.1)

            # 等待所有任务完成
            self.executor.shutdown(wait=True)
            logger.info("Project {} finished", self.project.name)
            
        except Exception as e:
            logger.error("Project {} failed: {}", self.project.name, str(e))
            # 确保出错时也能正确关闭线程池
            self.executor.shutdown(wait=False)
            raise e


class Project(ProjectInterface):
    
    def __init__(self, path: str, name: str="", description: str="", id: str="", parallelism: int=5):
        if name == "" and path == "":
            raise ValueError("Project name and path cannot be empty")
        if name == "" and path != "":
            name = os.path.basename(path.rstrip(os.sep))
        if path == "":
            raise ValueError("Project path cannot be empty")
        self.path = path
        self.name = name
        self.description = description
        self.id = id
        self.cron = ""
        self.exclude_path = []
        self.flows = []
        self.depended_flows = {}
        self.parallelism = parallelism  # 并行度，默认5
        # 记录dag的出度
        self.dag_degree = {}
        self.runtime = None
        self._load_from_file()

    def _load_from_file(self):
        if not os.path.exists(self.project_file_path):
            return
        self._set_attr_after_load(self.load_project())

    def load_module_from_file(self, file_path):
        module_name = os.path.splitext(os.path.basename(file_path))[0]  # 从文件路径中提取模块名称
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def scan(self, quiet=False) -> list[Flow]:
        """
        扫描当前路径下的所有 flow 文件，更新项目配置
        """

        if not quiet:
            logger.info("Scanning all python scripts under {}", self.path)

        # 当前目录及子目录中以.py结尾的path数组


        flow_files = []
        for root, dirs, files in os.walk(self.path):
            for file in files:
                if file.endswith(".py") and file not in self.exclude_path:
                    flow_files.append(os.path.join(root, file))

        if not quiet:
            logger.info("{} TapFlow scripts found, running in alphebetic order: ", len(flow_files))

        def disabled_method(*args, **kwargs):
            pass

        # 遮蔽 save 和 start 方法
        original_flow_save = Flow.save
        original_flow_start = Flow.start
        original_pipeline_save = Pipeline.save
        original_pipeline_start = Pipeline.start

        Flow.save = disabled_method
        Flow.start = disabled_method
        Pipeline.save = disabled_method
        Pipeline.start = disabled_method

        try:
            flows = []
            for flow_file in flow_files:
                if not quiet:
                    logger.info("Scanning {}...", flow_file)
                module = self.load_module_from_file(flow_file)
                # 获取module中所有Flow或者Pipeline的实例
                flows.extend([getattr(module, name) for name in dir(module) if isinstance(getattr(module, name), (Flow, Pipeline))])
            
            return flows

        finally:
            # 恢复原始方法
            Flow.save = original_flow_save
            Flow.start = original_flow_start
            Pipeline.save = original_pipeline_save
            Pipeline.start = original_pipeline_start

    @property
    def project_file_path(self):
        if self.path.endswith(".project"):
            return self.path
        return os.path.join(self.path, self._project_file_name)
    
    @property
    def data_flows_file_path(self):
        return os.path.join(self.path, "data_flows.py")
    
    def init(self) -> bool:
        """
        初始化 .project 文件    
        当 .project 文件不存在时, 扫描当前目录下的所有flow文件创建.project文件
        当 .project 文件存在时, 读取.project文件中的内容
        """
        if not os.path.exists(self.project_file_path):
            shutil.copy(os.path.join(TEMPLATE_FILE_PATH, "template.project"), self.project_file_path)
        if not os.path.exists(self.data_flows_file_path):
            shutil.copy(os.path.join(TEMPLATE_FILE_PATH, "data_flows.py"), self.data_flows_file_path)
        return True

    def setSchedule(self, cron: str):
        self.cron = cron

    def exclude(self, path: str):
        self.exclude_path.append(path)

    def check_flow_name_repeat(self, flow: str | Pipeline):
        if flow in self.flows:
            logger.warn("Flow name {} already exists, skip", flow.name)
            return False
        return True

    def check_depended_flow_valid(self, depended: str | list[str]):
        """depended格式为: flow_name.stage(cdc_or_initial_sync or empty).start_or_end(start or end)"""
        if depended == "" or (isinstance(depended, list) and len(depended) == 0):
            return True
        if isinstance(depended, str):
            depended = [depended]
        for dep in depended:
            if len(dep.split(".")) != 3 and len(dep.split(".")) != 2:
                logger.warn("Dependend flow {} is invalid, skip", dep)
                return False
            # depended_flow_name = dep.split(".")[0]
            # if depended_flow_name not in [flow.name for flow in self.flows]:
            #     logger.warn("Dependend flow {} not in project, skip", depended_flow_name)
            #     return False
        return True

    def add_flow(self, flow: str | Pipeline, depended: str | list[str]=""):
        # 检查flow名称是否重复
        if not self.check_flow_name_repeat(flow):
            return False
        # 检查依赖的flow是否有效
        if not self.check_depended_flow_valid(depended):
            return False
        
        if isinstance(flow, str):
            f = Flow(flow)
            if f.job is None:
                logger.warn("Flow {} not exist in remote, skip", flow)
                return False
        elif isinstance(flow, Pipeline):
            f = flow
        else:
            raise ValueError(f"Invalid flow type: {type(flow)}")
        self.flows.append(f)

        depended = depended or f.depends_on

        if depended:
            if isinstance(depended, str):
                depended = [depended]
            for dep in depended:
                self.dag_degree.setdefault(f.name, 0)
                self.dag_degree[f.name] += 1
                self.depended_flows.setdefault(f.name, []).append(dep)
        else:
            self.dag_degree.setdefault(f.name, 0)
            self.depended_flows.setdefault(f.name, [])

    def reset(self):
        return True

    def delete_project(self):
        pass

    def delete_project_file(self):
        if os.path.exists(self.project_file_path):
            os.remove(self.project_file_path)

    def delete_flows(self, _flows: list[Flow]=None, max_depth: int=5):
        """删除flow，并检查是否存在未删除的任务，如果存在，递归删除，递归深度为5次"""

        if max_depth == 0:
            return

        flows = self.flows.copy() if _flows is None else _flows
        for flow in flows:
            flow.delete()

        # 重新刷新任务列表
        show_jobs(quiet=True)
        exists_flow = []
        for flow in flows:
            if flow.name in client_cache["jobs"]["name_index"].keys():
                exists_flow.append(flow)

        if exists_flow:
            time.sleep(0.5)
            self.delete_flows(exists_flow, max_depth-1)

    def delete(self):
        """delete project, project file and all flows"""
        self.delete_project()  # delete project by api
        self.delete_project_file()  # delete project file
        self.delete_flows()  # delete flows
        logger.info("Project {} deleted", self.name)
        show_jobs(quiet=True)
        return True

    def _stop(self):
        pass

    def _stop_flows(self):
        for flow in self.flows:
            flow.stop()

    def stop(self):
        """stop project and all flows"""
        self._stop()  # stop project by api
        self._stop_flows()  # stop flows
        logger.info("Project {} stopped", self.name)
        return True

    def to_dict(self) -> dict:
        """
        将项目转换为指定格式的 dict
        
        Returns:
            dict: 项目配置
        """
        processed_flows = []
        for flow in self.flows:
            processed_flow = {
                "name": flow.name,
                "depends_on": self.depended_flows.get(flow.name, [])
            }
            processed_flows.append(processed_flow)
        project_dict = {
            'project': {
                'name': self.name,
                'description': self.description,
                'cron': self.cron
            },
            'flows': processed_flows,
            'config': {
                'exclude': self.exclude_path
            }
        }
        return project_dict

    def to_yaml(self) -> str:
        """
        将项目转换为指定格式的 YAML
        
        Returns:
            str: YAML 格式的项目配置
        """
        project_dict = self.to_dict()
        return yaml.dump(project_dict, sort_keys=False)

    def load_project(self) -> dict:
        with open(self.project_file_path, "r") as f:
            project_dict = yaml.load(f, Loader=yaml.FullLoader)

        project_info = project_dict.get("project", {})
        flows = project_dict.get("flows", [])
        config = project_dict.get("config", {})

        result = {
            "project": {
                "name": project_info["name"],
                "description": project_info["description"],
                "cron": project_info["cron"]
            },
            "flows": flows,
            "depended_flows": {flow["name"]: flow["depends_on"] for flow in flows},
        }

        result.update(config)

        return result

    def _set_attr_after_load(self, project_dict: dict):
        self.name = project_dict["project"]["name"]
        self.description = project_dict["project"]["description"]
        self.cron = project_dict["project"]["cron"]
        try:
            self.exclude_path = project_dict["config"]["exclude"]
        except KeyError:
            self.exclude_path = []

        flows = self.scan(quiet=True)
        flow_map = {flow.name: flow for flow in flows}

        try:
            flow_in_dict = [flow_map[flow["name"]] for flow in project_dict["flows"]]
        except KeyError as e:
            logger.error("Flow {} not found in flows", e)
            raise e

        self.flows = flow_in_dict
        self.depended_flows = project_dict["depended_flows"]
        # 设置dag的出度
        for flow in self.flows:
            self.dag_degree[flow.name] = len(self.depended_flows.get(flow.name, []))

    def are_projects_equal(self, old_project: dict, new_project: dict):
        return old_project == new_project

    def checkNameNotNull(self):
        if not self.name:
            raise ValueError("Project name cannot be empty")

    def checkCronValid(self):
        if not self.cron:
            return
        if not re.match(r"^(\*|(\d+|\d+\-\d+)(,\d+|\/\d+)?)( (\*|(\d+|\d+\-\d+)(,\d+|\/\d+)?)){5}$", self.cron):
            raise ValueError("Cron expression is invalid")

    @property
    def _project_file_name(self):
        """默认为当前目录名.project"""
        if not self.path:
            raise ValueError("Project path cannot be empty")
        return f"{os.path.basename(self.path)}.project"

    def save(self, quiet=False):
        """
        保存项目配置
        根据当前项目配置生成.project文件，如果存在.project文件，则更新文件内容
        """
        self.checkNameNotNull()
        self.checkCronValid()
        # if exists, clean and write
        try:
            if os.path.exists(self.project_file_path):
                if self.are_projects_equal(self.load_project(), self.to_dict()):
                    return True
        except Exception as e:
            pass
        with open(self.project_file_path, "w") as f:
            f.write(self.to_yaml())
        if not quiet:
            logger.info("Project {} saved in {}", self.name, self.project_file_path)
        return True

    def before_start(self):
        pass

    def after_start(self):
        pass
    
    def check_dag_valid(self) -> bool:
        """
        检查当前依赖关系是否存在环
        
        Returns:
            bool: True 如果DAG有效(无环), False 如果存在环
        """
        # 复制一份入度表,避免修改原始数据
        in_degree = self.dag_degree.copy()
        
        # 使用拓扑排序检测是否存在环
        queue = []
        # 找出所有入度为0的节点
        for flow_name, degree in in_degree.items():
            if degree == 0:
                queue.append(flow_name)
        
        visited_count = 0
        while queue:
            current = queue.pop(0)
            visited_count += 1
            
            # 对于当前节点指向的所有节点,入度减1
            for flow_name, depends in self.depended_flows.items():
                for dep in depends:
                    if dep.split('.')[0] == current:  # 检查依赖的flow名称
                        in_degree[flow_name] -= 1
                        if in_degree[flow_name] == 0:
                            queue.append(flow_name)
        
        # 如果访问的节点数小于总节点数,说明存在环
        return visited_count == len(self.flows)

    def start(self):
        """start project"""
        show_jobs(quiet=True)
        self.before_start()

        if not self.check_dag_valid():
            logger.error("{}", "DAG is invalid, please check the depended flows")
            return False
        
        scheduler = ProjectScheduler(self)
        scheduler.start()

        self.after_start()

    def status(self):
        logger.warn("{}", "Project.status() not supported now.")

    @classmethod
    def list(cls):
        logger.warn("{}", "Project.list() not supported now.")
