import uuid
import time
import copy
import datetime
from typing import Iterable, Tuple, Sequence

from tapflow.lib.help_decorator import help_decorate
from tapflow.lib.request import InspectApi
from tapflow.lib.utils.log import logger
from tapflow.lib.params.job import job_config

from tapflow.lib.op_object import show_jobs
from tapflow.lib.data_pipeline.job import JobType, JobStatus, Job
from tapflow.lib.data_pipeline.dag import Dag
from tapflow.lib.op_object import QuickDataSourceMigrateJob
from tapflow.lib.data_pipeline.nodes.source import Source
from tapflow.lib.data_pipeline.nodes.sink import Sink
from tapflow.lib.data_pipeline.nodes.merge import MergeNode, Merge
from tapflow.lib.data_pipeline.base_node import FilterType, ConfigCheck
from tapflow.lib.data_pipeline.nodes.filter import Filter
from tapflow.lib.data_pipeline.nodes.row_filter import RowFilterType, RowFilter
from tapflow.lib.data_pipeline.nodes.field_rename import FieldRename
from tapflow.lib.data_pipeline.nodes.union import UnionNode
from tapflow.lib.data_pipeline.nodes.unwind import Unwind
from tapflow.lib.data_pipeline.nodes.column_filter import ColumnFilter
from tapflow.lib.data_pipeline.nodes.value_map import ValueMap
from tapflow.lib.data_pipeline.nodes.rename import Rename
from tapflow.lib.data_pipeline.nodes.rename_table import RenameTable
from tapflow.lib.data_pipeline.nodes.time_adjust import TimeAdjust
from tapflow.lib.data_pipeline.nodes.time_add import TimeAdd
from tapflow.lib.data_pipeline.nodes.js import Js
from tapflow.lib.data_pipeline.validation.data_verify import DataVerify
from tapflow.lib.connections.connection import get_table_fields
from tapflow.lib.data_pipeline.nodes.type_modification import TypeAdjust


# show all jobs
def show_pipelines(quiet=False):
    show_jobs(quiet)

migrate = "migrate"
mview = "sync"

class VerifyMode:
    count = "row_count"
    field = "field"
    pk = "jointField"
    hash = "hash"

def is_tapcli():
    try:
        get_ipython
        return True
    except NameError:
        return False

@help_decorate("use to define a stream pipeline", "p = new Pipeline($name).readFrom($source).writeTo($sink)")
class Pipeline:
    @help_decorate("__init__ method", args="p = Pipeline($name)")
    def __init__(self, name=None, mode="migrate", id=None):
        if name is None:
            name = str(uuid.uuid4())
        self.dag = Dag(name="name")
        self.dag.config({"skipErrorEvent": {
            "errorMode": "Disable",
            "limitMode": "SkipByLimit",
            "errorModeEnum": "Disable",
            "limitModeEnum": "SkipByLimit"
        }})
        self.dag.jobType = mode
        self.stage = None
        self.job = None
        self.check_job = None
        self.name = name
        self.id = id
        self.mergeNode = None
        self.sources = []
        self.lines = []
        self.sinks = []
        self.validateConfig = None
        self.get()
        self.cache_sinks = {}
        self.joinValueChange = False
        self.type_adjust = []
        self._lookup_cache = {}
        self._lookup_path_cache = {}

    def _get_lookup_parent(self, path):
        if path == "" or "." not in path:
            return self
        parent_path = path[:path.rfind(".")]
        if parent_path in self._lookup_path_cache:
            return self._lookup_path_cache[parent_path]
        return self

    def lookup(self, source, path="", type=dict, relation=None, filter=None, fields=None, rename=None, mapper=None):
        if isinstance(source, str):
            if "." in source:
                db, table = source.split(".")
                source = Source(db, table, mode=self.dag.jobType)
            else:
                source = Source(source, mode=self.dag.jobType)
        cache_key = "%s_%s_%s" % (source.table_name, path, type)
        if cache_key not in self._lookup_cache:
            child_p = Pipeline(mode=self.dag.jobType)
            child_p.read_from(source)
            self._lookup_cache[cache_key] = child_p
            self._lookup_path_cache[path] = child_p

        child_p = self._lookup_cache[cache_key]
        parent_p = self._get_lookup_parent(path)

        if filter is not None:
            child_p = child_p.filter(filter)
        if fields is not None:
            child_p = child_p.filterColumn(fields)
        if rename is not None:
            child_p = child_p.rename_fields(rename)
        if mapper is not None:
            child_p = child_p.func(script=mapper)

        if type == dict:
            parent_p.merge(child_p, association=relation, targetPath=path, mergeType="updateWrite")

        if type == list:
            parent_p.merge(child_p, association=relation, targetPath=path, mergeType="updateIntoArray", isArray=True, arrayKeys=source.primary_key)
        if is_tapcli():
            logger.info("Flow updated: new table {} added as child table", source)
        return self

    def enable_join_value_change(self):
        self.joinValueChange = True

    def mode(self, value):
        self.dag.jobType = value

    def read_from(self, source):
        return self.readFrom(source)

    @help_decorate("read data from source", args="p.readFrom($source)")
    def readFrom(self, source):
        if isinstance(source, QuickDataSourceMigrateJob):
            source = source.__db__
            source = Source(source)
        elif isinstance(source, str):
            if "." in source:
                db, table = source.split(".")
                source = Source(db, table, mode=self.dag.jobType)
            else:
                source = Source(source, mode=self.dag.jobType)
        if source.mode is not None:
            self.mode = source.mode
        source.mode = self.mode
        self.sources.append(source)
        self.lines.append(source)
        if is_tapcli():
            print("Flow updated: source added")
        return self._clone(source)

    def write_to(self, sink):
        return self.writeTo(sink)

    @help_decorate("write data to sink", args="p.writeTo($sink, $relation)")
    def writeTo(self, sink, pk=None):
        if isinstance(sink, QuickDataSourceMigrateJob):
            sink = sink.__db__
            sink = Sink(sink)
        elif isinstance(sink, str):
            if "." in sink:
                db, table = sink.split(".")
                sink = Sink(db, table, mode=self.dag.jobType)
            else:
                sink = Sink(sink, mode=self.dag.jobType)

        sink.mode = self.mode
        if self.dag.jobType == JobType.sync:
            if pk is None:
                try:
                    primary_key = self.sources[-1].primary_key
                except Exception as e:
                    primary_key = None
            else:
                primary_key = pk
            sink.config({
                "updateConditionFields": primary_key,
            })

        self.dag.edge(self, sink)
        self.sinks.append({"sink": sink})
        self.lines.append(sink)
        if is_tapcli():
            print("Flow updated: sink added")
        return self._clone(sink)

    def _common_stage(self, f):
        self.dag.edge(self, f)
        return self._clone(f)

    def _common_stage2(self, p, f):
        if isinstance(p.stage, MergeNode):
            # delete the p.stage from p.dag
            nodes = []
            for i in self.dag.dag["nodes"]:
                if i["id"] != p.stage.id:
                    nodes.append(i)
            self.dag.dag["nodes"] = nodes
            for i in self.dag.dag["edges"]:
                if i["target"] == p.stage.id:
                    i["target"] = f.id
            self.dag.edge(self, f)
            self.dag.add_extra_nodes_and_edges(self.mergeNode, p, f)
        else:
            self.dag.edge(self, f)
            self.dag.edge(p, f)
        return self._clone(f)

    @help_decorate("using simple query filter data", args='p.filter("id > 2 and sex=male")')
    def filter(self, query="", filterType=FilterType.keep, mode=None):
        if mode is not None:
            filterType = mode
        if self.dag.jobType == JobType.migrate:
            logger.fwarn("{}", "migrate job not support filter processor")
            return self
        f = Filter(query, filterType)
        self.lines.append(f)
        if is_tapcli():
            print("Flow updated: filter added")
        return self._common_stage(f)

    def rowFilter(self, expression, rowFilterType=RowFilterType.retain):
        f = RowFilter(expression, rowFilterType)
        self.lines.append(f)
        return self._common_stage(f)

    def renameField(self, config={}):
        f = FieldRename(config)
        self.lines.append(f)
        if is_tapcli():
            print("Flow updated: fields rename node added")
        return self._common_stage(f)

    def rename_fields(self, config={}):
        return self.renameField(config)

    def typeAdjust(self, converts, table):
        """
        :params converts: List[tuple, (field, field_type)]
        """
        f = TypeAdjust()
        for c in converts:
            f.convert(c[0], c[1])
        connection_ids = self._get_source_connection_id()
        if len(connection_ids) == 0:
            raise Exception("source node not found")
        f.get(connection_ids[0], table)
        self.lines.append(f)
        return self._common_stage(f)

    def union(self, unionNode=None):
        if unionNode is None:
            unionNode = UnionNode()
        self.lines.append(unionNode)
        return self._common_stage(unionNode)

    @help_decorate("filter column", args='p.filterColumn(["id", "name"], FilterType.keep)')
    def filterColumn(self, query=[], filterType=FilterType.keep):
        if self.dag.jobType == JobType.migrate:
            logger.fwarn("{}", "migrate job not support filterColumn processor")
            return self
        f = ColumnFilter(query, filterType)
        self.lines.append(f)
        if is_tapcli():
            print("Flow updated: column filter added")
        return self._common_stage(f)

    def filter_columns(self, query=[], filterType=FilterType.keep):
        return self.filterColumn(query, filterType)

    def typeMap(self, field, t):
        return self

    def verify(self, mode):
        self.verifyJob = DataVerify(self, mode=mode)
        self.verifyJob.save()
        return self.verifyJob

    def valueMap(self, field, value):
        f = ValueMap(field, value)
        return self._common_stage(f)

    @help_decorate("rename a record key", args="p.rename($old_key, $new_key)")
    def rename(self, ori, new):
        if self.dag.jobType == JobType.migrate:
            logger.fwarn("{}", "migrate job not support rename processor")
            return self
        f = Rename(ori, new)
        self.lines.append(f)
        return self._common_stage(f)

    def renameTable(self, prefix="", suffix="", config=[]):
        tables = self.lines[-1].table
        f = RenameTable(prefix, suffix, tables, config)
        self.lines.append(f)
        return self._common_stage(f)

    def adjustTime(self, addHours=0, t=["now"]):
        f = TimeAdjust(addHours, t=t)
        self.lines.append(f)
        return self._common_stage(f)

    def addTimeField(self, field="created_at"):
        f = TimeAdd(field)
        self.lines.append(f)
        return self._common_stage(f)

    def func(self, script="", declareScript="", language="js"):
        return self.js(script, declareScript, language)

    @help_decorate("use a function(js text/python function) transform data", args="p.js()")
    def js(self, script="", declareScript="", language="js"):
        if self.dag.jobType == JobType.migrate:
            logger.fwarn("{}", "migrate job not support js processor")
            return self
        import types
        if type(script) == types.FunctionType:
            from metapensiero.pj.api import translates
            import inspect
            source_code = inspect.getsource(script)
            source_code = "def process(" + source_code.split("(", 2)[1]
            js_script = translates(source_code)[0]
            f = Js(js_script, False, language=language)
        else:
            if script.endswith(".js"):
                js_script = open(script, "r").read()
                script = js_script
            f = Js(script, declareScript, language=language)
        self.lines.append(f)
        if is_tapcli():
            print("Flow updated: custom function added")
        return self._common_stage(f)

    def flat_unwind(self, path=None, index_name="_index", array_elem="BASIC", joiner="_", keep_null=True):
        array_elem = str(array_elem).upper()
        if self.dag.jobType == JobType.migrate:
            logger.fwarn("{}", "migrate job not support js processor")
            return self
        f = Unwind(name="flat_unwind", mode="FLATTEN", path=path, index_name=index_name, array_elem=array_elem, joiner=joiner, keep_null=keep_null)
        self.lines.append(f)
        return self._common_stage(f)

    def embedded_unwind(self, path=None, index_name="_index", keep_null=True):
        if self.dag.jobType == JobType.migrate:
            logger.fwarn("{}", "migrate job not support js processor")
            return self
        f = Unwind(name="embedded_unwind", mode="EMBEDDED", path=path, index_name=index_name, keep_null=keep_null)
        self.lines.append(f)
        return self._common_stage(f)

    @help_decorate("merge another pipeline", args="p.merge($pipeline)")
    def merge(self, pipeline, association: Iterable[Sequence[Tuple[str, str]]] = None, mergeType="updateWrite",
              targetPath="", isArray=False, arrayKeys=[]):
        if not isinstance(pipeline, Pipeline):
            logger.fwarn("{}", "pipeline must be the instance of class Pipeline")
            return
        if not isinstance(association, Iterable) and association is not None:
            logger.fwarn("{}", "association error, it can be like this: [('id', 'id')]")
            return
        if self.dag.jobType == JobType.migrate:
            logger.fwarn("{}", "migrate job not support merge")
            return
        parent_id = self.lines[-1].id
        parent_table_name = self.sources[-1].tableName
        if self.mergeNode is None:
            self.mergeNode = Merge(
                parent_id, parent_table_name, association=[], mergeType=mergeType, targetPath=targetPath, join_value_change=self.joinValueChange
            )
        child_id = pipeline.lines[-1].id
        child_table_name = pipeline.sources[len(pipeline.sources) - 1].tableName
        mergeNode = Merge(
            child_id,
            child_table_name,
            association=[] if association is None else association,
            mergeType=mergeType,
            targetPath=targetPath,
            isArray=isArray,
            arrayKeys=arrayKeys,
            join_value_change=self.joinValueChange
        )
        if pipeline.mergeNode is None:
            pipeline.mergeNode = mergeNode
        else:
            pipeline.mergeNode.update(mergeNode)
        self.mergeNode.add(pipeline.mergeNode)
        return self._common_stage2(pipeline, self.mergeNode)

    @help_decorate("use a function(js text/python function) transform data", args="p.processor()")
    def processor(self, script=""):
        return self.js(script)

    def get(self):
        job = Job(name=self.name, id=self.id)
        if job.id is not None:
            self.job = job

    def _get_source_connection_id(self):
        ids = []
        for s in self.sources:
            ids.append(s.connectionId)
        return ids

    def enableLatencyMeasure(self):
        return self.accurateDelay()

    def enableShareCdc(self):
        self.config({"shareCdcEnable": True})

    def skip_error_event_by_limit(self, size: int):
        self.config({
            "skipErrorEvent": {
                "errorMode": "SkipData",
                "limitMode": "SkipByLimit",
                "limit": size,
                "errorModeEnum": "SkipData",
                "limitModeEnum": "SkipByLimit"
            }
        })

    def accurateDelay(self):
        source = self.sources[0]
        sink = self.sinks[0]
        fields = get_table_fields(source.tableName, whole=True, source=source.connectionId)
        self.validateConfig = {
            "flowId": "",
            "name": "",
            "mode": "cron",
            "inspectMethod": "",
            "enabled": True,
            "status": "",
            "limit": {"keep": 100},
            "platformInfo": {"agentType": "private"},
            "timing": {
                "start": int(time.time()) * 1000,
                "end": int(time.time()) * 1000 + 86400000 * 365 * 10,
                "intervals": 1440,
                "intervalsUnit": "minute"
            },
            "tasks": [{
                "fullMatch": True,
                "jsEngineName": "graal.js",
                "script": "",
                "showAdvancedVerification": False,
                "source": {
                    "connectionId": source.connectionId,
                    "databaseType": source.databaseType,
                    "fields": fields,
                    "sortColumn": sink["relation"].association[0][0],
                    "table": source.tableName
                },
                "target": {
                    "connectionId": sink["sink"].connectionId,
                    "databaseType": sink["sink"].databaseType,
                    "fields": fields,
                    "sortColumn": sink["relation"].association[0][0],
                    "table": sink["sink"].tableName
                }
            }]
        }
        return self.config({"accurate_delay": True})

    @help_decorate("config pipeline", args="config map, please h pipeline_config get all config key and it's meaning")
    def config(self, config: dict = None, keep_extra=True):
        if not isinstance(config, dict):
            logger.fwarn("type {} must be {}", config, "dict", "notice", "notice")
            return
        mode = self.dag.jobType
        self.dag.config(config)
        resp = ConfigCheck(self.dag.setting, job_config[mode], keep_extra=keep_extra).checked_config
        self.dag.config(resp)
        return self

    def full_sync(self):
        self.config({"type": "initial_sync"})
        return self

    def sync_type(self):
        return self.dag.setting.get("type", "initial_sync")

    def include_cdc(self):
        self.config({"type": "initial_sync+cdc"})
        return self

    def only_cdc(self, start_time=None):
        self.config({"type": "cdc"})
        if start_time is not None:
            self.config_cdc_start_time(start_time)
        return self

    def circle_sync(self):
        self.config({"doubleActive": True})
        return self

    def readLogFrom(self, logMiner):
        return self

    def _clone(self, stage):
        p = Pipeline()
        p.dag = self.dag
        self.stage = stage
        p.stage = self.stage
        p.job = self.job
        p.check_job = self.check_job
        p.sources = copy.copy(self.sources)
        p.sinks = copy.copy(self.sinks)
        p.lines =  copy.copy(self.lines)
        p.name = self.name
        p.cache_sinks = self.cache_sinks
        p.mergeNode = self.mergeNode
        p.joinValueChange = self.joinValueChange
        return p

    def cache(self, ttl):
        return self

    @help_decorate("config cdc time", args='p.config_cdc_start_time()')
    def config_cdc_start_time(self, start_time, tz="+8"):
        if type(start_time) == datetime.datetime:
            # 转时间戳
            start_time = int(time.mktime(start_time.timetuple()) * 1000)
        source_connections = self.sources
        config = self.dag.config()
        syncPoints = []
        tz = "+08:00"
        t = "localTZ"
        if start_time is None or start_time == "":
            t = "current"
        for i in range(len(source_connections)):
            syncPoints.append({
                "dateTime": start_time,
                "timezone": tz,
                "pointType": t,
                "connectionId": source_connections[i].connectionId,
                "connectionName": source_connections[i].name,
                "nodeId": source_connections[i].id,
                "nodeName": source_connections[i].name
            })
        config["syncPoints"] = syncPoints
        config["sync_type"] = "cdc"
        self.config(config)

    def save(self):
        if self.job is not None:
            self.job.config(self.dag.setting)
            self.job.save()
            return self

        job = Job(name=self.name, pipeline=self)
        job.validateConfig = self.validateConfig
        self.job = job
        self.config({})
        job.config(self.dag.setting)
        job.save()
        return self

    @help_decorate("start this pipeline as a running job", args="p.start()")
    def start(self):
        if self.job is not None:
            self.job.config(self.dag.setting)
            self.job.start()
            return self
        job = Job(name=self.name, pipeline=self)
        job.validateConfig = self.validateConfig
        self.job = job
        self.config({})
        job.config(self.dag.setting)
        if job.start():
            pass
        else:
            logger.fwarn("job {} start failed!", self.name)
            print(job.logs(level=["debug", "error"]))
        if not self.wait_status("running", t=60):
            logger.fwarn("job {} start timeout!", self.name)
            print(job.logs(level=["debug", "error"]))
            return False
        return self

    @help_decorate("stop this pipeline job", args="p.stop()")
    def stop(self):
        if self.job is None:
            # logger.fwarn("pipeline {} not start, can not stop", self.name)
            return self
        self.job.stop()
        time.sleep(0.1)
        self.job.stop()
        time.sleep(0.1)
        self.job.stop()
        return self

    def full_qps(self):
        if self.job is None:
            #logger.fwarn("pipeline {} not start", self.name)
            return 0
        return int(self.job.full_qps())

    def reset(self):
        if self.job is None:
            #logger.fwarn("pipeline {} not start", self.name)
            return 0
        return self.job.reset()

    def cdc_qps(self):
        if self.job is None:
            #logger.fwarn("pipeline {} not start", self.name)
            return 0
        return int(self.job.cdc_qps())

    def wait_cdc_0(self, t=100, threshold=20):
        time.sleep(10)
        zero_times = 0
        start_time = time.time()
        while True:
            if time.time() - start_time > t:
                return False
            time.sleep(5)
            if zero_times >= threshold:
                return True
            if self.cdc_qps() == 0:
                zero_times += 1
                continue
            else:
                zero_times = 0

    def replicate_lag(self):
        if self.job is None:
            logger.fwarn("pipeline {} not start", self.name)
            return -1
        stats = self.job.stats()
        return stats.replicate_lag

    def wait_delay(self, delay=10000, t=120):
        start_time = time.time()
        while True:
            if time.time() - start_time > t:
                return False
            if self.replicate_lag() < delay and self.replicate_lag() != 0:
                return True
            time.sleep(5)
        return False

    @help_decorate("delete this pipeline job", args="p.delete()")
    def delete(self):
        if self.job is None:
            logger.fwarn("pipeline {} not exists, can not delete", self.name)
            return self
        self.job.delete()
        return self

    @help_decorate("get pipeline job status", args="p.status()")
    def status(self):
        if self.job is None:
            #logger.fwarn("pipeline not start, no status can show")
            return self
        status = self.job.status()
        ##logger.finfo("job {} status is: {}", self.name, status)
        return status

    def has_retry(self):
        logs = self.job.logs(level="WARN")
        if logs is not None and len(logs) > 0:
            for log in logs:
                if "[Auto Retry]" in log["message"]:
                    return True
        return False

    def delay(self):
        return self.replicate_lag()

    def wait_oracle_cdc_started(self, t=1800, quiet=True):
        if self.job is None:
            #logger.fwarn("pipeline not start, no status can show")
            return False
        s = time.time()
        while True:
            time.sleep(5)
            if time.time() - s > t:
                return False
            if self.status() != "running":
                return False
            logs = self.job.logs(level="INFO", limit=1000)
            if logs is not None and len(logs) > 0:
                for log in logs:
                    if "incremental start succeed" in log["message"]:
                        return True
                    if "add log miner sql" in log["message"]:
                        time.sleep(10)
                        return True


    def wait_status(self, status, t=30, quiet=True):
        if self.job is None:
            #logger.fwarn("pipeline not start, no status can show")
            return self
        s = time.time()
        if type(status) == type(""):
            status == [status]
        while True:
            if self.job.status() in status:
                return True
            if self.job.status() == JobStatus.error and JobStatus.error not in status:
                return False
            time.sleep(1)
            if time.time() - s > t:
                break
        return False

    def wait_stats(self, stats, t=30, quiet=True):
        if self.job is None:
            #logger.fwarn("pipeline not start, no status can show")
            return self
        s = time.time()
        while True:
            job_stats = self.job.stats().__dict__
            ok = True
            for k, v in stats.items():
                if k not in job_stats:
                    ok = False
                    continue
                if job_stats[k] != v:
                    ok = False
            if ok:
                return True
            time.sleep(1)
            if time.time() - s > t:
                break
        return False

    def wait_initial_complete(self, t=300, quiet=True):
        return self.wait_initial_sync(t, quiet)

    def wait_initial_sync(self, t=300, quiet=True):
        if self.job is None:
            return self
        s = time.time()
        while True:
            status = self.job.status()
            stats = self.job.stats()
            if (stats.snapshot_table_total > 0 and stats.snapshot_table_total == stats.table_total) or status in ["complete", "error"]:
                if not quiet:
                    pass
                    #logger.finfo("job {} initial sync finish, wait time is: {} seconds", self.job.name,
                    #            int(time.time() - s))
                if status == "running":
                    # 等几秒, 避免状态不一致
                    time.sleep(5)
                return True
            time.sleep(5)
            if time.time() - s > t:
                break
        time.sleep(10)
        return False

    # BUG:
    # TODO:
    def wait_cdc_delay(self, t=30, quiet=True):
        if self.job is None:
            #logger.fwarn("pipeline not start, no status can show")
            return self
        s = time.time()
        last_stats = self.job.stats()
        while True:
            time.sleep(6)
            now_stats = self.job.stats()
            if last_stats.input_insert == now_stats.input_insert and last_stats.input_update == now_stats.input_update and last_stats.input_delete == now_stats.input_delete:
                return self
            last_stats = now_stats
            if time.time() - s > t:
                break
        return False

    def metrics(self):
        return self.job.stats()

    @help_decorate("get pipeline job stats", args="p.stats()")
    def stats(self, quiet=True):
        self.monitor(t=2)
        return self

    @help_decorate("monitor pipeline job until it stoppped or timeout", args="p.monitor(10)")
    def monitor(self, t=30):
        if self.job is None:
            #logger.fwarn("pipeline not start, no monitor can show")
            return
        self.job.monitor(t)
        return self

    def check(self, count=10):
        if self.status() not in [JobStatus.running, JobStatus.stop, JobStatus.complete]:
            logger.fwarn(
                "{}", "The status of this task is not in [running, stop, complete], unable to check data."
            )
            return
        if not self.dag.setting.get("isAutoInspect"):
            logger.fwarn("please set {} to enable auto inspect", "$pipeline.config({'isAutoInspect': True})", "info")
            return
        for _ in range(count):
            time.sleep(1)

            data = InspectApi().post({"id": self.job.id})
            if not data:
                pass
            data = data["data"]
            diff_record = data.get("diffRecords", 0)
            diff_tables = data.get("diffTables", 0)
            totals = data.get("totals", 0)
            ignore = data.get("ignore", 0)

            logger.log(
                "data check start, total is {}, ignore row number is {}, diff row number is {}, diff table number is {}",
                totals, ignore, diff_record, diff_tables,
                "info", "info", "warn", "warn",
            )
            if self.status() in [JobStatus.stop, JobStatus.complete, JobStatus.error]:
                break


class MView(Pipeline):
    def __init__(self, name=None, id=None):
        super().__init__(name=name, mode=mview, id=id)

class Flow(Pipeline):
    def __init__(self, name=None, id=None):
        super().__init__(name=name, mode=mview, id=id)
