import json
import time
from enum import Enum

from requests import delete

from tapflow.lib.request import req
from tapflow.lib.cache import system_server_conf

from tapflow.lib.help_decorator import help_decorate
from tapflow.lib.utils.log import logger
from tapflow.lib.request import TaskApi
from tapflow.lib.graph import Node, Graph
from tapflow.lib.cache import client_cache


class JobStats:
    qps = 0
    total = 0
    input_insert = 0
    input_update = 0
    input_delete = 0
    output_insert = 0
    output_update = 0
    output_Delete = 0
    snapshot_done_at = 0
    snapshot_start_at = 0
    snapshot_row_total = 0
    input_qps = 0
    output_qps = 0
    output_qps_avg = 0
    output_qps_max = 0
    replicate_lag = 0
    table_total = 0
    snapshot_table_total = 0
    last_five_minutes_qps = 0


class JobType:
    migrate = "migrate"
    sync = "sync"


@help_decorate("Enum, used to describe a job status")
class JobStatus:
    edit = "edit"
    running = "running"
    scheduled = "scheduled"
    paused = "paused"
    stop = 'stop'
    stopping = 'stopping'
    complete = "complete"
    wait_run = "wait_run"
    wait_start = "wait_start"
    error = "error"


class MilestoneStep(Enum):
    DEDUCTION = "DEDUCTION"
    DATA_NODE_INIT = "DATA_NODE_INIT"
    TABLE_INIT = "TABLE_INIT"
    SNAPSHOT = "SNAPSHOT"
    CDC = "CDC"
    NONE = None

    @staticmethod
    def value_of(str):
        for step in MilestoneStep:
            if step.value == str:
                return step
        raise ValueError(f"Milestone step {str} not found")


class Job:
    def __init__(self, name=None, id=None, dag=None, pipeline=None):
        self.id = None
        self.setting = {}
        self.job = {}
        self.dag = None
        self.validateConfig = None
        self.id = None
        self.pipeline = pipeline
        # 如果id是24位, 则认为是短id, 否则认为是长id, 短id直接获取, 长id通过op_object获取
        # 如果name不为空, 则通过name获取id, dataflow -> job
        if id is not None and len(id) == 24:
            self.id = id
        elif id is not None:
            from tapflow.lib.op_object import get_obj
            obj = get_obj("job", id)
            self.id = obj.id
        elif name is not None and client_cache["jobs"]["name_index"].get(name):
            self.id = client_cache["jobs"]["name_index"][name]["id"]
        if self.id is not None:
            self._get()
            return
        if dag is not None:
            self.dag = dag
        else:
            self.dag = pipeline.dag
        self.name = name

    @staticmethod
    def list():
        res = req.get(
            "/Task",
            params={"filter": '{"fields":{"id":true,"name":true,"status":true,"agentId":true,"stats":true}}'}
        )
        if res.status_code != 200:
            return None
        res = res.json()
        jobs = []
        for i in res["data"]['items']:
            jobs.append(Job(id=i["id"]))
        return jobs

    def reset(self, quiet=True):
        status = self.status()
        if status in ["running"]:
            if not quiet:
                logger.warn("Task status is {}, can not reset, please stop it first", status)
                return False
        res = req.patch("/Task/batchRenew", params={"taskIds": self.id}).json()
        if res["code"] == "ok":
            if not quiet:
                logger.info("{}", "Task reset success")
            return True
        logger.warn("{}", "Task reset failed")
        return False

    def _get_by_name(self):
        import urllib.parse
        param = '{"where":{"name":{"like":"%s"}}}' % (self.name)
        param = urllib.parse.quote(param, safe="().")
        param = param.replace(".", "%5C%5C.")
        param = param.replace("(", "%5C%5C(")
        param = param.replace(")", "%5C%5C)")
        res = req.get("/Task?filter=" + param)
        if res.status_code != 200:
            return None
        res = res.json()
        for j in res["data"]["items"]:
            if "name" not in j:
                continue
            if j["name"] == self.name:
                self.id = j["id"]
                self.job = j
                return

    def _get_id_by_name(self):
        param = {"filter": json.dumps({"where":{"name":{"like":self.name}}})}
        res = req.get("/Task", params=param)
        if res.status_code != 200:
            return None
        res = res.json()
        for j in res["data"]["items"]:
            if "name" not in j:
                continue
            if j["name"] == self.name:
                return j["id"]
        return None

    def _get(self):
        pipeline_id = ''
        if self.id is not None and client_cache["jobs"]["id_index"].get(self.id):
            pipeline_id = self.id
        else:
            return
        
        res = req.get(f"/Task/{pipeline_id}")
        if res.status_code != 200:
            return
        data = res.json()["data"]
        self.name = data["name"]
        self.job = data
        self.id = data["id"]
        self.dag = data["dag"]
        self.jobType = data["syncType"]
    
    def stop(self, t=60, sync=True, quiet=True, force=False):
        if self.status() != JobStatus.running:
            if not quiet:
                logger.warn("Task status is {}, not running, can not stop it", self.status())
            return False
        if self.id is None:
            return False
        res = req.put('/Task/batchStop', params={'taskIds': self.id, 'force': force})
        s = time.time()
        while True:
            if time.time() - s > t:
                if not quiet:
                    logger.warn("{}", "Task stopped failed")
                return False
            time.sleep(1)
            status = self.status()
            if status in [JobStatus.stop, JobStatus.wait_run, JobStatus.error]:
                if not quiet:
                    logger.info("{}", "Task stopped successfully")
                return True
            if status == JobStatus.stopping and not sync:
                return True
        if not quiet:
            logger.warn("{}", "Task stopped failed")
        return False

    def delete(self, quiet=True):
        if self.id is None:
            return False
        if self.status() in [JobStatus.running, JobStatus.scheduled]:
            logger.fwarn("job status is {}, please stop it first before delete it", self.status())
            if not quiet:
                logger.warn("job status is {}, please stop it first before delete it", self.status())
            return
        res = req.delete("/Task/batchDelete", params={"taskIds": self.id})
        if res.status_code != 200:
            if not quiet:
                logger.warn("{}", "Task delete failed")
            return False
        res = res.json()
        if res["code"] != "ok":
            if not quiet:
                logger.warn("{}", "Task delete failed")
            return False
        if not quiet:
            logger.info("{}", "Task deleted successfully")
        return True
    
    def copy(self, quiet=False):
        res = req.put(f"/Task/copy/{self.id}")
        if res.status_code != 200:
            logger.fwarn("{}", "Task copy failed")
            return False
        res = res.json()
        if res["code"] != "ok":
            logger.fwarn("{}", "Task copy failed")
            return False
        client_cache["jobs"]["id_index"][res["data"]["id"]] = res["data"]
        client_cache["jobs"]["name_index"][res["data"]["name"]] = res["data"]
        client_cache["jobs"]["number_index"][str(len(client_cache["jobs"]["number_index"]))] = res["data"]
        copy_id = res["data"]["id"]
        job = Job(id=copy_id)
        job.name = res["data"]["name"]
        if not quiet:
            logger.info("{}", f"Copy task '{self.name}' to '{job.name}' success")
        return job

    def relations(self):
        if self.id is None:
            return False
        res = req.post("/task-console/relations", json={"taskId": self.id})
        if res.status_code != 200:
            return []
        res = res.json()
        if res["code"] != "ok":
            return []
        return res["data"]

    def heartbeat_id(self):
        if self.id is None:
            return None
        relations = self.relations()
        for relation in relations:
            if relation["type"] == "connHeartbeat":
                return relation["id"]
        return None

    def wait_heartbeat_to_status(self, status=JobStatus.running, timeout=30, interval=1):
        heartbeat_id = self.heartbeat_id()
        if heartbeat_id is None:
            raise ValueError("Heartbeat task id is None")

        begin_time = time.time()
        heartbeat_job = Job(id=heartbeat_id)
        while True:
            last_status = heartbeat_job.status()
            if last_status == status:
                return
            if time.time() - begin_time > timeout:
                raise TimeoutError("Wait heartbeat task status timeout, current status: %s" % last_status)
            logger.finfo("Wait heartbeat {} to status {} re-check after {} seconds", last_status, status, interval)
            time.sleep(interval)

    def log_cache_id(self):
        if self.id is None:
            return None
        relations = self.relations()
        for relation in relations:
            if relation["type"] == "logCollector":
                return relation["id"]
        return None

    def save(self):
        if self.id is None:
            self.job = {
                "editVersion": int(time.time() * 1000),
                "syncType": self.dag.jobType,
                "name": self.name,
                "status": JobStatus.edit,
                "dag": self.dag.dag,
                "user_id": system_server_conf["user_id"],
                "customId": system_server_conf["user_id"],
                "createUser": system_server_conf["username"],
                "syncPoints": self.dag.setting.get("syncPoints", []),
                "dynamicAdjustMemoryUsage": True,
                "crontabExpressionFlag": False
            }

        else:
            self.job.update({
                "editVersion": int(time.time() * 1000),
                "name": self.name,
                "dag": self.dag.dag,
                "user_id": system_server_conf["user_id"],
                "customId": system_server_conf["user_id"],
                "createUser": system_server_conf["username"],
                "syncPoints": self.dag.setting.get("syncPoints", []),
                "dynamicAdjustMemoryUsage": True,
                "crontabExpressionFlag": False
            })

        if self.validateConfig is not None:
            self.job["validateConfig"] = self.validateConfig

        try:
            nodes = self.job["dag"]["nodes"]
            for node in nodes:
                if "previewQualifiedName" in node:
                    del(node["previewQualifiedName"])
                if "previewTapTable" in node:
                    del(node["previewTapTable"])
        except Exception as e:
            pass

        self.job.update(self.setting)
        if self.id is None:
            res = req.post("/Task", json=self.job)
            res = res.json()
            if res["code"] != "ok":
                if "Task.RepeatName" == res["code"]:
                    self.id = self._get_id_by_name()
                    if self.id is None:
                        logger.warn("save task failed {}", res)
                        return False
                    self.job["id"] = self.id
                    res = req.patch("/Task", json=self.job).json()
                    if res["code"] != "ok":
                        logger.warn("patch task failed {}", res)
                        return False
                else:
                    logger.warn("save failed {}", res)
                    return False
            else:
                self.id = res["data"]["id"]
        job = self.job
        job.update(self.setting)
        job.update(self.dag.to_dict())
        # load schema
        if self.pipeline.target is not None:
            res = req.get(f"/MetadataInstances/node/schema", params={"nodeId": self.pipeline.target.id}).json()
        if self.id is None:
            self._get()
        body = {
            "dag": {
                "nodes": self.dag.dag["nodes"],
                "edges": self.dag.dag["edges"],
            },
            "editVersion": int(time.time() * 1000),
            "id": self.id,
            "pageVersion": int(time.time() * 1000),
        }
        res = req.patch("/Task", json=body)
        if res.status_code != 200 or res.json().get("code") != "ok":
            logger.fwarn("start failed {}", res.json())
            logger.fdebug("res: {}", res.json())
            return False
        # 如果源有文件类型, 调用下推演
        for s in self.pipeline.sources:
            if str(s.databaseType).lower() in ["csv"]:
                for i in range(10):
                    nodeConfig = s.setting["nodeConfig"]
                    nodeConfig["nodeId"] = s.id
                    res = req.post("/proxy/call", json={
                        "className": "DiscoverSchemaService",
                        "method": "discoverSchema",
                        "nodeId": s.id,
                        "args": [
                            s.connectionId,
                            nodeConfig,
                        ]
                    })
                    new_dag = self.job["dag"]
                    for node in new_dag["nodes"]:
                        if node["id"] == s.id:
                            old_table_name = node["tableName"]

                            node["tableName"] = "tapdata"

                            res = req.patch("/Task", json={
                                "editVersion": int(time.time() * 1000),
                                "id": self.id,
                                "dag": new_dag
                            })
                            time.sleep(10)

                            node["tableName"] = old_table_name

                            res = req.patch("/Task", json={
                                "editVersion": int(time.time() * 1000),
                                "id": self.id,
                                "dag": new_dag
                            })
                            time.sleep(10)
                            break
                    schema_res = req.get("/MetadataInstances/node/schema?nodeId=" + s.id).json()
                    node_schema = []
                    if len(schema_res["data"]) > 0:
                        fields = schema_res["data"][0]["fields"]
                        for field in fields:
                            node_schema.append({
                                "indicesUnique": field["unique"],
                                "isPrimaryKey": field["primaryKey"],
                                "label": field["field_name"],
                                "tapType": field["tapType"],
                                "type": field["data_type"],
                                "value": field["field_name"],
                            })
                            dag = self.job["dag"]
                            for node in dag["nodes"]:
                                if node["id"] == s.id:
                                    node["schema"] = node_schema
                                    break
                        req.patch("/Task", json={
                            "editVersion": int(time.time() * 1000),
                            "id": self.id,
                            "dag": self.job["dag"]
                        })
                    res = req.get("/MetadataInstances/node/schemaPage?nodeId=" + s.id).json()
                    if res["data"]["total"] == 1:
                        break
                    else:
                        logger.fwarn("discover schema failed for {} times, retrying, most 10 times", i)
        res = req.patch(f"/Task/confirm/{self.id}", json=self.job)
        res = res.json()
        if res["code"] != "ok":
            logger.fwarn("save failed {}", res)
            return False
        self.job = res["data"]
        self.setting = res["data"]
        return True

    def start(self, quiet=True):
        try:
            status = self.status()
        except (KeyError, TypeError) as e:
            resp = self.save()
            if not resp:
                logger.fwarn("job {} save failed.", self.name)
                return False
            status = self.status()
        if status in [JobStatus.running, JobStatus.scheduled, JobStatus.wait_run]:
            if not quiet:
                logger.warn("Task {} status is {}, need not start", self.name, status)
            return True

        if self.id is None:
            logger.fwarn("save job fail")
            return False
        # 等推演, 10s
        time.sleep(3)
        res = req.put("/Task/batchStart", params={"taskIds": self.id}).json()
        if res["code"] != "ok":
            if not quiet:
                logger.warn("{}", "Task start failed")
            return False
        try:
            if res["data"][0]["code"] == "Task.ScheduleLimit":
                logger.warn("{}", res["data"][0]["message"])
                return False
        except Exception as e:
            pass
        if not quiet:
            logger.info("{}", "Task start succeed")
        return True

    def config(self, config):
        self.setting.update(config)

    def status(self, res=None, quiet=True):
        if res is None:
            res = req.get("/Task/" + self.id).json()
        status = res["data"]["status"]
        if not quiet:
            logger.info("job status is: {}", status)
        return status

    def get_milestone_step(self, res=None, quiet=True):
        if res is None:
            res = req.get("/Task/" + self.id).json()
        data: dict = res.get("data")
        status = data.get("status")
        # status = res["data"]["status"]
        if status not in [JobStatus.running, JobStatus.scheduled, JobStatus.wait_run]:
            raise ValueError(f"Task status error: {status}")
        sync_status = data.get("syncStatus")
        step = MilestoneStep.value_of(sync_status)
        if not quiet:
            logger.finfo("job milestone step is: {}", step)
        return step

    def wait_milestone_to_step(self, step=MilestoneStep.CDC, timeout=30, interval=2):
        begin_time = time.time()
        while True:
            current_step = self.get_milestone_step()
            if step == current_step:
                return
            if time.time() - begin_time > timeout:
                raise TimeoutError("Wait task milestone step timeout, current milestone step: %s" % current_step)
            logger.finfo("Wait milestone {} to {}, re-check after {} seconds", current_step.value, step.value, interval)
            time.sleep(interval)

    def full_qps(self):
        full_qps = 0
        for i in range(5):
            stats = self.stats()
            if stats.snapshot_done_at == 0 and stats.replicate_lag > 0:
                stats.snapshot_done_at = int(time.time()) * 1000
            full_qps = int(stats.snapshot_row_total / (stats.snapshot_done_at - stats.snapshot_start_at + 1) * 1000)
            if full_qps > 0:
                return full_qps
            time.sleep(2)
        if full_qps == 0:
            stats = self.stats()
            return stats.output_qps_avg
        return full_qps

    def cdc_qps(self):
        stats = self.stats()
        input_qps = stats.input_qps
        output_qps = stats.output_qps
        if output_qps > 0:
            return output_qps
        return input_qps

    def delay(self):
        stats = self.stats()
        return stats.replicate_lag

    def wait_delay_change(self, timeout=30, interval=1, limit=None) -> float:
        limit *= 1000
        last_delay = None
        begin_time = time.time()
        while True:
            current_delay = self.delay()
            if last_delay is None:
                last_delay = current_delay
            elif last_delay != current_delay:
                if limit is None:
                    return current_delay
                elif current_delay > limit:
                    last_delay = current_delay
                else:
                    return current_delay

            if time.time() - begin_time > timeout:
                logger.debug(f"time cost: {time.time() - begin_time}, timeout: {timeout}; "
                             f"actual delay: {current_delay}, limit: {limit}")
                raise TimeoutError("Wait delay change timeout, current delay: %sms, wait is: %sms" % (current_delay, limit))
            logger.finfo("Wait delay change {} re-check after {} seconds", current_delay, interval)
            time.sleep(interval)

    def get_sub_task_ids(self):
        sub_task_ids = []
        res = req.get("/Task/" + self.id).json()
        statuses = res["data"]["statuses"]
        jobStats = JobStats()
        for subTask in statuses:
            sub_task_ids.append(subTask["id"])
        return sub_task_ids

    def stats(self, res=None, quiet=True):
        data = TaskApi().get(self.id)["data"]
        payload = {
            "totalData": {
                "uri": "/api/measurement/query/v2",
                "param": {
                    "startAt": int(time.time() * 1000)-300000,
                    "endAt": int(time.time() * 1000),
                    "samples": {
                        "data": {
                            "endAt": int(time.time() * 1000),
                            "fields": [
                                "inputInsertTotal",
                                "inputUpdateTotal",
                                "inputDeleteTotal",
                                "inputDdlTotal",
                                "inputOthersTotal",
                                "outputInsertTotal",
                                "outputUpdateTotal",
                                "outputDeleteTotal",
                                "outputDdlTotal",
                                "outputOthersTotal",
                                "tableTotal",
                                "createTableTotal",
                                "snapshotTableTotal",
                                "initialCompleteTime",
                                "sourceConnection",
                                "targetConnection",
                                "snapshotDoneAt",
                                "snapshotRowTotal",
                                "snapshotInsertRowTotal",
                                "inputQps",
                                "outputQps",
                                "currentSnapshotTableRowTotal",
                                "currentSnapshotTableInsertRowTotal",
                                "replicateLag",
                                "snapshotStartAt",
                                "snapshotTableTotal",
                                "currentEventTimestamp",
                                "snapshotDoneCost",
                                "outputQpsMax",
                                "outputQpsAvg",
                                "lastFiveMinutesQps"
                            ],
                            "tags": {
                                "taskId": self.id,
                                "taskRecordId": data["taskRecordId"],
                                "type": "task"
                            },
                            "type": "instant",
                        }
                    }
                }
            }
        }
        for i in range(5):
            try:
                res = req.post("/measurement/batch", json=payload, timeout=3).json()
                break
            except Exception as e:
                time.sleep(1)
        job_stats = JobStats()
        try:
            if len(res["data"]["totalData"]["data"]["samples"]["data"]) > 0:
                stats = res["data"]["totalData"]["data"]["samples"]["data"][0]
                job_stats.qps = stats.get("outputQps", 0)
                job_stats.total = stats.get("tableTotal", 0)
                job_stats.input_insert = stats.get("inputInsertTotal", 0)
                job_stats.input_update = stats.get("inputUpdateTotal", 0)
                job_stats.input_delete = stats.get("inputDeleteTotal", 0)
                job_stats.output_insert = stats.get("outputInsertTotal", 0)
                job_stats.output_update = stats.get("outputUpdateTotal", 0)
                job_stats.output_Delete = stats.get("outputDeleteTotal", 0)
                job_stats.snapshot_done_at = stats.get("snapshotDoneAt", 0)
                job_stats.snapshot_start_at = stats.get("snapshotStartAt", 0)
                job_stats.input_qps = stats.get("inputQps", 0)
                job_stats.output_qps = stats.get("outputQps", 0)
                job_stats.output_qps_avg = stats.get("outputQpsAvg", 0)
                job_stats.output_qps_max = stats.get("outputQpsMax", 0)
                job_stats.snapshot_row_total = stats.get("snapshotRowTotal", 0)
                job_stats.replicate_lag = stats.get("replicateLag", 0)
                job_stats.output_qps_avg = stats.get("outputQpsAvg", 0)
                job_stats.output_qps_max = stats.get("outputQpsMax", 0)
                job_stats.snapshot_row_total = stats.get("snapshotRowTotal", 0)
                job_stats.table_total = stats.get("tableTotal", 0)
                job_stats.snapshot_table_total = stats.get("snapshotTableTotal", 0)
                job_stats.last_five_minutes_qps = stats.get("lastFiveMinutesQps", 0)
        except Exception as e:
            print(__file__, e)
            pass

        job_status = self.status(quiet=True)
        if not quiet:
            logger.info("Flow current status is: {}, qps is: {}, total rows: {}, delay is: {}ms", job_status, job_stats.qps, job_stats.snapshot_row_total, job_stats.replicate_lag)

        return job_stats

    def logs(self, res=None, limit=100, level="info", t=30, tail=False, quiet=True):
        logs = []
        data = TaskApi().get(self.id)["data"]
        res = req.post("/MonitoringLogs/query", json={
            "levels": [level],
            "order": "desc",
            "page": 1,
            "pageSize": limit,
            "taskId": self.id,
            "taskRecordId": data["taskRecordId"],
            "start": int(time.time()*1000)-3600*100000,
            "end": int(time.time()*1000)
        })
        if res.status_code != 200:
            return logs
        if res.json()["code"] != "ok":
            return logs
        if not quiet:
            for item in res.json()["data"]["items"]:
                print(item)
        return res.json()["data"]["items"]

    def find_final_target(self):
        targets = []
        try:
            dag = self.dag.dag
            if dag is None:
                return None
            edges = dag.get("edges", [])
            nodes = dag.get("nodes", [])
        except Exception as e:
            return None
        def target_is_final(target):
            for edge in edges:
                if edge["source"] == target:
                    edge_target = edge["target"]
                    for node in nodes:
                        if node["id"] == edge_target and node["type"] == "table":
                            return True
                    return False
            return True
        for edge in edges:
            target = edge.get("target")
            if target_is_final(target):
                targets.append(target)
        return targets


    def preview(self, quiet=True):
        self.save()
        final_target = self.find_final_target()
        start_time = time.time()
        res = req.post("/proxy/call", json={
            "className": "TaskPreviewService",
            "method": "preview",
            "args": [
                json.dumps(self.job),
                None,
                1
            ]
        }).json()
        if not quiet:
            logger.info("preview view took {} ms", int((time.time() - start_time)*1000))
        if "code" not in res or res["code"] != "ok":
            return

        data = res["data"]
        nodeResult = data.get("nodeResult", {})
        print(nodeResult)
        if not quiet:
            for k, v in nodeResult.items():
                if k in final_target:
                    print(json.dumps(v.get("data", [{}])[0], indent=2))

        return nodeResult

    def wait(self, print_log=False, t=600):
        start_time = time.time()
        while True:
            if time.time() - start_time > t:
                break
            time.sleep(1)
            stats = self.stats()
            status = self.status()
            print_info = [
                "job {} status: {}, qps: {}, total: {} "
                "input_stats: insert: {}, update: {}, delete: {} "
                "output_stats: insert: {}, update: {}, delete: {}",
                self.name, status, stats.qps, stats.total, stats.input_insert, stats.input_update,
                stats.input_delete, stats.output_insert, stats.output_update, stats.output_Delete,
                "info", "info", "notice", "info", "debug", "info", "info", "info", "debug", "info", "info", "info",
            ]
            if print_log:
                logger.finfo(*print_info, wrap=False, logger_header=True)
            if status in [JobStatus.running, JobStatus.edit, JobStatus.scheduled]:
                continue
            break

    def monitor(self, t=30, quiet=False):
        self.wait(print_log=True, t=t)

    def check(self):
        pass

    def desc(self):
        if self.job["syncType"] not in ["migrate", "sync"]:
            logger.fwarn("syncType {} not support in this version", self.job["syncType"])
            return

        job_info = {
            # "id": self.job["id"],
            "name": self.job["name"],
            "syncType": self.job["syncType"],
            "createTime": self.job["createTime"],
        }
        # logger.finfo("")
        logger.notice("{}", "-" * 120)
        # logger.finfo("{}", "job info")
        print(json.dumps(job_info, indent=4))

        g = Graph()
        node_map = {}  # {node.id: node config}
        attrs_get = {
            "migrate": ["tableNames", "syncObjects", "writeStrategy"],
            "sync": [
                "processorThreadNum", "script", "updateConditionFields", "expression",
                "joinType", "joinExpressions", "leftNodeId", "rightNodeId", "mergeProperties",
                "scripts", "operations", "operations", "deleteAllFields"
            ]
        }

        for n in self.job["dag"]["nodes"]:
            config = {
                "id": n.get("id"),
                "name": n.get("name"),
                "type": n.get("type"),
                "databaseType": n.get("databaseType"),
                "cdcConcurrent": True,
                "cdcConcurrentWriteNum": 8,
                "increaseReadSize": 1,
                "initialConcurrent": True,
                "initialConcurrentWriteNum": 8,
                "writeBatchSize": 100,
            }
            if self.job["syncType"] == "migrate":
                for attr in attrs_get["migrate"]:
                    if n.get(attr):
                        config.update({attr: n.get(attr)})
            elif self.job["syncType"] == "sync":
                for attr in attrs_get["sync"]:
                    if n.get(attr):
                        config.update({attr: n.get(attr)})

            node = Node(n.get("id"), n.get("name"), config=config)
            node_map.update({n.get("id"): config})
            g.addVertex(node)

        for n in self.job["dag"]["edges"]:
            g.addEdgeById(n["source"], n["target"])

        # logger.finfo("")
        logger.notice("{}", "-" * 120)
        # logger.finfo("{}", "node relationship of job")
        for s in g.to_relation():
            pass
            # logger.finfo(s)

        for node_id, config in node_map.items():
            # logger.finfo("")
            logger.notice("{}", "-" * 120)
            # logger.finfo("{} {}", "configuration of node id", node_id[-6:])
            print(json.dumps(config, indent=4))

    def rename(self, new_name=None):
        if new_name is None:
            raise ValueError("The new name cannot be empty")
        response = req.patch("/task/rename/"+self.id+"?newName=" + new_name).json()
        if response["code"] != "ok":
            logger.ferror("Task rename failed: {}", response)
            return
        logger.finfo("Task rename succeed: {}", response)
