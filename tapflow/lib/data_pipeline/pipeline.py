import inspect
import uuid
import time
import copy
import datetime
from typing import Iterable, Tuple, Sequence, List, Dict, Union

from tapflow.lib.data_pipeline.nodes import get_node_instance
from tapflow.lib.data_pipeline.nodes.field_add_del import FieldAddDel
from tapflow.lib.data_pipeline.nodes.field_calculate import FieldCalculate
from tapflow.lib.data_pipeline.nodes.python import Python
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
from tapflow.lib.data_pipeline.nodes.type_filter import TypeFilterNode
from tapflow.lib.data_pipeline.nodes.merge import MergeNode, Merge
from tapflow.lib.data_pipeline.base_node import FilterType, ConfigCheck, WriteMode
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
from tapflow.lib.data_pipeline.nodes.py import Py

from tapflow.lib.data_pipeline.validation.data_verify import DataVerify
from tapflow.lib.connections.connection import get_table_fields
from tapflow.lib.data_pipeline.nodes.type_modification import TypeAdjust
from tapflow.lib.cache import client_cache

_flows = {}


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
        get_ipython # type: ignore
        return True
    except NameError:
        return False
    

class SourceNotExistError(Exception):
    pass


class SinkTableNumberError(Exception):
    pass


@help_decorate("use to define a stream pipeline", "p = new Pipeline($name).readFrom($source).writeTo($sink)")
class Pipeline:
    @help_decorate("__init__ method", args="p = Pipeline($name)")
    def __init__(self, name=None, mode="migrate", id=None):
        if name is None:
            name = str(uuid.uuid4())
        self._dag = {}
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
        self.target = None
        self.lines = []
        self.sinks = []
        self.validateConfig = None
        self.cache_sinks = {}
        self.joinValueChange = False
        self._lookup_cache = {}
        self._lookup_path_cache = {}
        self.merge_node_childs = []
        self.command = []
        self._parent_cache = {}
        self._read_from_ed = False
        self._write_to_ed = False
        self._lookup_ed = False
        self._union_node = None
        self.depends_on = []
        self.if_condition = None
        self.get()

    def if_condition(self, condition):
        self.if_condition = condition
        return self

    def depend(self, depends_on: Union[str, List[str]]):
        if isinstance(depends_on, str):
            self.depends_on.append(depends_on)
        else:
            self.depends_on.extend(depends_on)
        return self

    def _get_lookup_parent(self, path):
        if path == "" or "." not in path:
            return self
        parent_path = path[:path.rfind(".")]
        if parent_path in self._lookup_path_cache:
            return self._lookup_path_cache[parent_path]
        return self

    def lookup(self, source, path="", type="object", arrayKeys=[], relation=None, query=None, **kwargs):

        if isinstance(source, str):
            if "." in source:
                db, table = source.split(".")
                source = Source(db, table)
            else:
                source = Source(source)
        cache_key = "%s_%s_%s" % (source.table_name, path, type)
        self.merge_node_childs.append(source)
        if cache_key not in self._lookup_cache:
            child_p = Pipeline(mode=self.dag.jobType)
            child_p.read_from(source, query=query)
            self._lookup_cache[cache_key] = child_p
            self._lookup_path_cache[path] = child_p

        child_p = self._lookup_cache[cache_key]
        child_p._pre_process_node(kwargs)

        if relation is not None:
            relation = [ r[::-1] for r in relation ]

        if type == "object":
            self.merge(child_p, association=relation, targetPath=path, mergeType="updateWrite")

        if type == "array":
            if len(arrayKeys) == 0:
                arrayKeys = source.primary_key
            self.merge(child_p, association=relation, targetPath=path, mergeType="updateIntoArray", isArray=True, arrayKeys=arrayKeys)
        if is_tapcli():
            logger.info("Flow updated: new table {} added as child table", source.table_name)
        self.command.append(["lookup", f"{source.name}.{source.table}", f"path={path}", f"type={type}", f"relation={relation}", kwargs])
        self._lookup_ed = True
        return self

    def enable_join_value_change(self):
        self.joinValueChange = True

    def mode(self, value):
        self.dag.jobType = value

    def read_from(self, *args, **kwargs):
        return self.readFrom(*args, **kwargs)
    
    def _conditions_to_filter(self, conditions):
        m = {
            1: ">",
            2: ">=",
            3: "<",
            4: "<=",
            5: "="
        }
        operators = []
        for c in conditions:
            operator = m.get(c["operator"], "=")
            left = c["key"]
            value = c["value"]
            operators.append(f"\"{left} {operator} {value}\"")
        return " and ".join(operators)

    def _filter_to_conditions(self, filter=None):
        if filter is None:
            return None

        def parse_expression(expression):
            import re
            # 定义所有支持的操作符
            operators = [">=", "<=", ">", "<", "==", "="]

            # 转义操作符并构建正则表达式模式
            pattern = "|".join([re.escape(op) for op in operators])

            # 使用正则表达式分割输入的表达式
            match = re.split(f"({pattern})", expression)

            if len(match) == 3:
                left = match[0].strip()  # 左边部分
                operator = match[1].strip()  # 操作符
                right = match[2].strip()  # 右边部分
                return left, operator, right
            else:
                return None, None, None  # 返回 None 表示解析失败
        conditions = []
        m = {
            ">": 1,
            ">=": 2,
            "<": 3,
            "<=": 4,
            "=": 5
        }
        filters = str(filter).split("and")
        for f in filters:
            k, op, v = parse_expression(f)
            if k is None:
                continue
            conditions.append({
                "fastQuery": False,
                "form": "BEFORE",
                "key": k,
                "number": 1,
                "operator": m.get(op, 5),
                "value": v,
                "unit": "DAY"
            })
        return conditions
    
    def _check_source_exists(self, source: Source) -> bool:
        """
        检查source是否存在
        """
        return source.exists()

    @help_decorate("read data from source", args="p.readFrom($source)")
    def readFrom(self, source, setting={}, query=None, filter=None, quiet=False):
        if self._read_from_ed:
            logger.warn("Read data from DB is already setted, please create a new Flow before reading data")
            return self
        if isinstance(source, QuickDataSourceMigrateJob):
            source = source.__db__
            source = Source(source)
        elif isinstance(source, str):
            if "." in source:
                db, table = source.split(".")
                source = Source(db, table)
            else:
                source = Source(source)
            source.mode = self.dag.jobType
        table_or_db = "table" if source.mode == JobType.sync else "database"
        source_name = f"{source.connection.c.get('name', '')}.{source.table_name}" if source.mode == JobType.sync else source.connection.c.get("name", "")
        # check if table exists
        if not self._check_source_exists(source):
            raise SourceNotExistError(f"Cannot read from the non-existent table {source_name}")
        # check if the table is a target table
        if source.connection_type() == "target":
            raise SourceNotExistError(f"Cannot read from {source_name}, because it is a target {table_or_db}")
        if source.mode is not None:
            self.dag.jobType = source.mode
        else:
            source.mode = self.dag.jobType
        source.setting.update(setting)
        if query is not None:
            source.setting.update({"customCommand":{
                "command": "executeQuery",
                "params": {
                    "sql": query
                }
            }, "enableCustomCommand": True})
        if filter is not None:
            conditions = self._filter_to_conditions(filter)
            if conditions is not None:
                source.setting.update({"conditions": conditions, "isFilter": True})
        self.sources.append(source)
        self.lines.append(source)
        self.dag.add_node(source)
        if is_tapcli() and not quiet:
            print("Flow updated: source added")
        if self.dag.jobType == JobType.sync:
            self.command.append(["read_from", source.connection.c.get("name", "")+"."+source.table_name])
        else:
            self.command.append(["read_from", source.connection.c.get("name", "")])
        self._read_from_ed = True
        self.merge_node_childs.append(source)
        obj = self._clone(source)
        self.__dict__ = obj.__dict__
        return self

    def write_to(self, *args, **kwargs):
        return self.writeTo(*args, **kwargs)
    
    def materialize(self, view_name):
        DEFAULT_SINK = client_cache["default_sink"]
        return self.writeTo(f"{DEFAULT_SINK.name}.{view_name}")

    @help_decorate("write data to sink", args="p.writeTo($sink, $relation)")
    def writeTo(self, sink, pk=None):
        if self._write_to_ed:
            logger.warn("Write data to DB is already setted, please create a new Flow before writing data")
            return self

        if isinstance(sink, QuickDataSourceMigrateJob):
            sink = sink.__db__
            sink = Sink(sink)
        elif isinstance(sink, str):
            if "." in sink:
                db, table = sink.split(".")
                sink = Sink(db, table)
            else:
                sink = Sink(sink)
        if sink.mode != self.dag.jobType and self.dag.jobType == JobType.migrate:
            # 提示Sink表数量不正确
            raise SinkTableNumberError("Sink table number is not correct, please use sink node like this: sink = Sink($Datasource) or write_to(Datasource)")
        elif sink.mode != self.dag.jobType and self.dag.jobType == JobType.sync:
            # 提示Sink表数量不正确
            raise SinkTableNumberError("Sink table number is not correct, please use sink node like this: sink = Sink($Datasource, $table) or write_to(Datasource.table)")
        sink.mode = self.dag.jobType
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
        if self.dag.jobType == JobType.sync:
            self.command.append(["write_to", sink.connection.c.get("name", "")+"."+sink.table_name])
        else:
            self.command.append(["write_to", sink.connection.c.get("name", "")])
        self._write_to_ed = True
        obj = self._clone(sink)
        self.__dict__ = obj.__dict__
        self.target = sink
        return self

    def _common_stage(self, f):
        self.dag.edge(self, f)
        obj = self._clone(f)
        self.__dict__.update(obj.__dict__)
        return self

    def _common_stage2(self, p, f):
        if isinstance(p.stage, MergeNode):
            # replace p.stage with f in self.dag
            self.dag.replace_node(p.stage, f)
            self.dag.edge(self, f)
            self.dag.add_extra_nodes_and_edges(self.mergeNode, p, f)
        else:
            self.dag.edge(self, f)
            self.dag.edge(p, f)
        return self._clone(f)

    @help_decorate("using simple query filter data", args='p.filter("id > 2 and sex=male")')
    def filter(self, query="", filterType=FilterType.keep, mode=None, name="Row Filter"):
        if mode is not None:
            filterType = mode
        if self.dag.jobType == JobType.migrate:
            logger.fwarn("{}", "migrate job not support filter processor")
            return self
        f = Filter(query, filterType, name=name)
        self.lines.append(f)
        if is_tapcli():
            print("Flow updated: filter added")
        self.command.append(["filter", query])
        return self._common_stage(f)

    def exclude_type(self, type_name):
        f = TypeFilterNode(type_name)
        self.lines.append(f)
        if is_tapcli():
            print("Flow updated: type filter added")
        self.command.append(["exclude_type", type_name])
        return self._common_stage(f)

    def rowFilter(self, expression, rowFilterType=RowFilterType.retain):
        f = RowFilter(expression, rowFilterType)
        self.lines.append(f)
        return self._common_stage(f)

    def renameField(self, config={}, name="Fields Rename"):
        f = FieldRename(config, name=name)
        self.lines.append(f)
        if is_tapcli():
            print("Flow updated: fields rename node added")
        self.command.append(["rename_fields", config])
        return self._common_stage(f)

    def rename_fields(self, config={}, name="Fields Rename"):
        return self.renameField(config, name)

    def type_adjust(self, converts, table, name="Type Adjust"):
        """
        :params converts: List[tuple, (field, field_type)]
        """
        f = TypeAdjust(name=name)
        for c in converts:
            f.convert(c[0], c[1])
        connection_ids = self._get_source_connection_id()
        if len(connection_ids) == 0:
            raise Exception("source node not found")
        f.get(connection_ids[0], table)
        self.lines.append(f)
        return self._common_stage(f)
    
    def _fn_map(self):
        fn_map = {
            "filter": self.filter,
            "fields": self.filterColumn,
            "rename": self.rename_fields,
            "js": self.js,
            "py": self.py,
            "mapper": self.func,
            "adjust_time": self.adjust_time,
            "type_adjust": self.type_adjust,
            "include": self.include,
            "exclude": self.exclude,
        }
        return fn_map
    
    def _pre_process_node(self, kwargs):
        """
        前置的计算节点
        :param kwargs:
        """
        fn_map = self._fn_map()
        for k, v in kwargs.items():
            if k in fn_map:
                if isinstance(v, dict):
                    fn_map[k](**v)
                elif isinstance(v, list):
                    fn_map[k](*v)
                else:
                    fn_map[k](v)

    def union(self, unionNode=None, name="Union", **kwargs):
        self._pre_process_node(kwargs)
        source = unionNode
        if unionNode is None and self._union_node is None:
            unionNode = UnionNode(name=name)
            self._union_node = unionNode

        if isinstance(unionNode, UnionNode):
            self._union_node = unionNode

        if isinstance(source, QuickDataSourceMigrateJob) or isinstance(source, str) or isinstance(source, Source):
            if self._union_node is None:
                self._union_node = UnionNode(name=name)
            if isinstance(source, QuickDataSourceMigrateJob):
                source = source.__db__
                source = Source(source)
            elif isinstance(source, str):
                if "." in source:
                    db, table = source.split(".")
                    source = Source(db, table)
                else:
                    source = Source(source)
            elif isinstance(source, Source):
                source = source
            self.union(self._union_node)
            self.read_from(source)
            self.union(self._union_node)
        self.lines.append(self._union_node)
        return self._common_stage(self._union_node)

    @help_decorate("filter column", args='p.filterColumn(["id", "name"], FilterType.keep)')
    def filterColumn(self, query=[], filterType=FilterType.keep, name="Column Filter"):
        if self.dag.jobType == JobType.migrate:
            logger.fwarn("{}", "migrate job not support filterColumn processor")
            return self
        f = ColumnFilter(query, filterType, name=name)
        self.lines.append(f)
        if is_tapcli():
            print("Flow updated: column filter added")
        self.command.append(["filter_columns", query])
        return self._common_stage(f)

    def include(self, *args, name="Include"):
        return self.filter_columns(query=list(args), filterType=FilterType.keep, name=name)

    def exclude(self, *args, name="Exclude"):
        return self.filter_columns(query=list(args), filterType=FilterType.delete, name=name)

    def filter_columns(self, query=[], filterType=FilterType.keep, name="Column Filter"):
        return self.filterColumn(query, filterType, name)

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

    def adjust_time(self, addHours=0, t=["now"], name="Time Adjust"):
        f = TimeAdjust(addHours, t=t, name=name)
        self.lines.append(f)
        return self._common_stage(f)

    def addTimeField(self, field="created_at"):
        f = TimeAdd(field)
        self.lines.append(f)
        return self._common_stage(f)
    
    def copy(self):
        job = self.job.copy(quiet=True)
        return Pipeline(name=job.name)

    def func(self, script="", declareScript="", language="js", pk=None, name="Function"):
        return self.js(script, declareScript, language, pk, name)

    def py(self, script="", declareScript="", pk=None, name="Python"):
        return self.func(script=script, declareScript=declareScript, pk=pk, name=name)

    @help_decorate("use a function(js text/python function) transform data", args="p.js()")
    def js(self, script="", declareScript="", language="js", name="JS", pk=None):
        if pk is not None:
            if declareScript != "" and not declareScript.endswith(";"):
                declareScript += ";\n"
            if type(pk) is str:
                pks = [pk]
            else:
                pks = pk
            for pkk in pks:
                declareScript += "TapModelDeclare.setPk(tapTable, '{}');\n".format(pkk)
        if self.dag.jobType == JobType.migrate:
            logger.fwarn("{}", "migrate job not support js processor")
            return self
        import types
        if type(script) == types.FunctionType:
            import inspect
            source_code = inspect.getsource(script)
            codes = source_code.split("\n")[1:-1]
            codes = "\n".join([i[4:] for i in codes])
            print(codes)
            f = Py(codes, declareScript)
        else:
            if script.endswith(".js"):
                js_script = open(script, "r").read()
                script = js_script
            f = Js(script, declareScript, language=language, name=name)
        self.lines.append(f)
        if is_tapcli():
            print("Flow updated: custom function added")
        self.command.append(["js", script])
        return self._common_stage(f)

    def add_date_field(self, k):
        return self.addTimeField(k)

    def add_field(self, k, v=None, js=None):
        return self.add_fields(k, v, js)

    def add_fields(self, k, v=None, js=None):
        fields = []
        if type(k) == list:
            fields = k
        else:
            fields = [[k, v, js]]

        m = {
            "String": "TapString",
            "Date": "TapDate",
            "DateTime": "TapDateTime",
            "Double": "TapNumber",
            "Float": "TapNumber",
            "BigDecimal": "TapNumber",
            "Long": "TapNumber",
            "Map": "TapMap",
            "Array": "TapArray"
        }
        declareScript = ""
        js_script = ""

        for f in fields:
            f_key = f[0]
            f_t = f[1]
            f_js = None
            if len(f) > 2:
                f_js = f[2]

            declareScript += "TapModelDeclare.addField(tapTable, '{}', '{}');\n".format(f_key, m.get(f_t, "TapString"))
            if f_js is not None:
                js_script += "record['{}'] = {};\n".format(f_key, f_js)
        js_script += "return record;"
        return self.js(script=js_script, declareScript=declareScript)

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
        if self.mergeNode is None:
            parent_id = self.lines[-1].id
            parent_table_name = self.sources[-1].tableName
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
        parent_p = self._get_lookup_parent(targetPath)

        # 1. targetPath 优先级大于 association
        # 2. 当不存在 targetPath 时，使用 association 关联，关联顺序为从merge_node_childs(父节点)开始
        # 3. 当都不存在，则将pipeline.mergeNode添加到self.mergeNode的子节点 

        if targetPath != "":
            parent_p.mergeNode.add(pipeline.mergeNode)
            self._parent_cache[pipeline] = parent_p
        elif association is not None:
            # 递归寻找pipeline.mergeNode的父mergeNode节点
            def _find_parent(target_fields):
                for node in self.merge_node_childs:
                    display_fields = get_table_fields(node.table_name, source=node.connectionId)
                    if display_fields.get(target_fields) is not None:
                        return self.mergeNode.find_by_node_id(node.id)
                return None

            confirmed = False
            for asso in association:
                if isinstance(asso, Iterable) and not isinstance(asso, str):
                    target_fields = asso[1]
                else:
                    raise Exception("association error, it can be like this: [('id', 'id')]")
                result_mergeNode = _find_parent(target_fields)
                # 如果找到父节点，则将pipeline.mergeNode添加到父节点, 否则添加到self.mergeNode的子节点
                if result_mergeNode is not None:
                    result_mergeNode.add(pipeline.mergeNode)
                    confirmed = True
                    break
            if not confirmed:
                self.mergeNode.add(pipeline.mergeNode)
        else:
            parent_p.mergeNode.add(pipeline.mergeNode)
            self._parent_cache[pipeline] = parent_p

        return self._common_stage2(pipeline, self.mergeNode)

    # 递归更新主从合并节点
    def recursive_update_parent(self, pipeline):
        if pipeline not in self._parent_cache:
            return
        parent = self._parent_cache[pipeline]
        parent._common_stage2(pipeline, parent.mergeNode)
        return self.recursive_update_parent(parent)

    @help_decorate("use a function(js text/python function) transform data", args="p.processor()")
    def processor(self, script=""):
        return self.js(script)
    
    def _make_node(self, node_dict):
        return get_node_instance(node_dict)
    
    def _find_node_by_id(self,node_id):
        return self._node_map.get(node_id, None)
    
    def _get_source_node(self, target_node_id):
        return self.dag.get_source_node(target_node_id)
    
    def _set_default_stage(self):

        # 查找没有子节点的目标节点
        targets_with_no_children = set(edge['target'] for edge in self.dag.dag['edges'])
        sources = set(edge['source'] for edge in self.dag.dag['edges'])
        leaf_targets = targets_with_no_children - sources
        if leaf_targets:
            # 如果 last_edge['target'] 是 Merge 节点，则将 self.stage 设置为该节点，否则设置为 last_edge['source']
            last_edge = next(edge for edge in self.dag.dag['edges'] if edge['target'] in leaf_targets)
            target_node = next((node for node in self.dag.dag['nodes'] if node['id'] == last_edge['target']), None)
            if target_node and target_node.get("type") == "merge_table_processor":
                self.stage = self._make_node(target_node)
            else:
                source_node = next((node for node in self.dag.dag['nodes'] if node['id'] == last_edge['source']), None)
                self.stage = self._make_node(source_node)
            return

        # if not found, set stage to first node
        if self.stage is None and len(self.dag.dag["nodes"]) > 0:
            self.stage = self._make_node(self.dag.dag["nodes"][0])
                
    def set_stage(self, stage):
        self.stage = stage

    def _set_lines(self):
        for node in self.dag.dag["nodes"]:
            self.lines.append(self._make_node(node))

    def _set_sources(self):
        for node in self.dag.dag["nodes"]:
            if node["type"] == "table":
                self.sources.append(Sink(node["attrs"]["connectionName"], node["tableName"]))

    def _set_lookup_cache(self, children: dict, parent_merge_node: Merge, node = None):
        node = self._find_node_by_id(children["id"]) if node is None else node
        if not node["type"] in ["table", "database"]:
            node = self._get_source_node(children["id"])
            return self._set_lookup_cache(children, parent_merge_node, node.to_dict())
        if node is None:
            return
        table_name = children["tableName"]
        path = "" if not children.get("targetPath", "") else children["targetPath"]
        type = dict if not children.get("isArray", False) else list
        cache_key = "%s_%s_%s" % (table_name, path, type)
        if cache_key not in self._lookup_cache:
            child_p = Pipeline(mode=self.dag.jobType)
            conn = f"{node['attrs']['connectionName']}.{node['tableName']}"
            source = Source(conn)
            child_p.read_from(source, quiet=True)
            self._lookup_cache[cache_key] = child_p
            self._lookup_path_cache[path] = child_p
            child_p.mergeNode = Merge(
                node["id"],
                child_p.lines[-1].table,
                association=[],
                mergeType=children.get("mergeType", "updateWrite"),
                targetPath=children.get("targetPath", ""),
                isArray=children.get("isArray", False),
                arrayKeys=children.get("arrayKeys", []),
                join_value_change=self.joinValueChange,
                id=node["id"]
            )

        for child in children["children"]:
            self._set_lookup_cache(child, parent_merge_node)

    def _set_merge_node(self):
        if self.dag.jobType == JobType.migrate:
            return None
        for node in self._dag["nodes"]:
            if node["type"] == "merge_table_processor":
                self.mergeNode = self._make_node(node)
                if node.get("mergeProperties") is None:
                    continue
                for merge_property in node["mergeProperties"]:
                    for child in merge_property["children"]:
                        self._set_lookup_cache(child, self.mergeNode)
        if self.mergeNode is not None:
            self.dag.update_node(self.mergeNode) 

    def _set_command(self):

        _process_kwargs_map = {
            Filter: lambda node: { "query": node.f.values()[0], "filterType": node.f.keys()[0], "name": node.name },
            FieldRename: lambda node: { "config": node.config, "name": node.name },
            Js: lambda node: { "script": node.script, "declareScript": node.declareScript, "language": node.language, "name": node.name },
            Python: lambda node: { "script": node.script, "declareScript": node.declareScript, "name": node.name },
            TimeAdjust: lambda node: { "addHours": node.addHours, "t": node.t, "name": node.name },
            TypeAdjust: lambda node: { "converts": node._convert_field, "table": node.pre_table_name, "name": node.name },
            Source: lambda node: { "source": f"{node.connection.c.get('name', '')}.{node.table_name}" if node.mode == JobType.sync else node.connection.c.get("name", "")},
            Sink: lambda node: { "sink": f"{node.connection.c.get('name', '')}.{node.table_name}" if node.mode == JobType.sync else node.connection.c.get("name", "")},
            UnionNode: lambda node: { "name": node.name },
            RowFilter: lambda node: { "expression": node.expression, "rowFilterType": node.rowFilterType }
        }

        _process_func_map = {
            Filter: self.filter,
            FieldRename: self.rename_fields,
            RowFilter: self.rowFilter,
            Js: self.js,
            Python: self.py,
            TimeAdjust: self.adjust_time,
            TypeAdjust: self.type_adjust,
            Source: self.readFrom,
            Sink: self.writeTo,
            UnionNode: self.union,
        }

        def command_generator(node, lookup_params=False):
            method = _process_func_map[type(node)]
            method_name = method.__name__.replace("readFrom", "read_from").replace("writeTo", "write_to")
            kwargs = _process_kwargs_map[type(node)](node)
            params = []
            if lookup_params:
                return [f"{method_name}={str(kwargs)}"]
            for k, v in kwargs.items():
                # 如果k是位置参数，则直接添加到params_str中
                if inspect.signature(method).parameters[k].default is None or inspect.signature(method).parameters[k].default == inspect.Parameter.empty:
                    params.append(v)
                # 如果k是默认参数，则添加到params中
                elif inspect.signature(method).parameters[k].default is not None:
                    params.append(f"{k}={v}")
                if isinstance(node, Source) and len(node.setting.get("conditions", [])) > 0:
                    params.append(f"filter={self._conditions_to_filter(node.setting.get('conditions'))}")
                if isinstance(node, Source) and node.setting.get("customCommand") is not None:
                    try:
                        params.append(f"query={node.setting.get('customCommand')['params']['sql']}")
                    except Exception as e:
                        pass
            param_str = ", ".join(params)
            return [method_name, param_str]
        
        def look_from_source_node_to_merge_node(node, merge_node, child_table=False):
            """
            从顶层节点遍历到merge节点

            1. 当node为子(表)节点, 则使用lookup语法:
                子:  .lookup(node, js=kwargs)
                父:  .read_from(node).js()....
            2. 当存在多个子节点, 
                如: 
                        -> js 
                    node          -> merge
                        -> filter 
                则: 不进行实现，只遍历第一个链条即 node -> js -> merge
            3. 当存在单个子节点, 则使用lookup语法: 
                如: node -> js -> merge, 则: .lookup(node, js=kwargs)

            :param node: 当前节点
            :param child_table: 是否是lookup中的子表, 当node为子(表)节点, 则使用lookup语法
            """
            # 退出条件
            if isinstance(node, MergeNode) or len(self.dag.graph[node.id]) == 0:
                return []
            
            child_node = self.dag.get_node(self.dag.graph[node.id][0])

            if child_table:
                if isinstance(node, Source):
                    command = ["lookup", node.connection.c.get("name", "")+"."+node.table_name]
                    if merge_node.targetPath != "":
                        command.append(f"path={merge_node.targetPath}")
                    if merge_node.association:
                        command.append(f"relation={merge_node.association}")
                    if merge_node.arrayKeys:
                        if len(merge_node.arrayKeys) > 0:
                            command.append("type=array")
                        command.append(f"arrayKeys={merge_node.arrayKeys}")
                    command += look_from_source_node_to_merge_node(child_node, merge_node, child_table=True)
                    return [command]
                else:
                    command = command_generator(node, lookup_params=True)
                    command += look_from_source_node_to_merge_node(child_node, merge_node, child_table=True)
                    return command
            
            else:
                command = [command_generator(node)]
                command += look_from_source_node_to_merge_node(child_node, merge_node, child_table=False)
                return command

        def find_head_node_by_merge_node(merge_node):
            head_node = merge_node.node_id
            if isinstance(self._find_node_by_id(head_node), Source):
                return self._find_node_by_id(head_node)
            else:
                return self.dag.get_source_node(head_node)
            
        def look_by_merge_node(merge_node, is_head_node=False):
            """
            根据merge节点中描述的children顺序, 依次遍历
            """
            node = find_head_node_by_merge_node(merge_node)
            if node is None:
                return
            self.command += look_from_source_node_to_merge_node(node, merge_node, child_table=not is_head_node)

            # 退出条件: 当不存在子Merge节点
            if len(merge_node.child) == 0:
                return
            
            for child in merge_node.child:
                look_by_merge_node(child, is_head_node=False)

        def look_end_to_write_node(node):
            # 退出条件
            if isinstance(node, Sink):
                return
            self.command.append(command_generator(node))
            for child in self.dag.graph[node.id]:
                look_end_to_write_node(self.dag.get_node(child))

        # 如果存在merge节点
        merge_nodes = [node for node in self.dag.node_map.values() if isinstance(node, Merge)]
        merge_node, father_node_id = None, None
        # 多个merge节点不考虑，只考虑第一个merge节点
        if len(merge_nodes) > 0:
            merge_node = merge_nodes[0]
            father_node_id = merge_node.node_id
        # 如果merge_node存在，找到最顶层的源节点
        if merge_node is not None and father_node_id is not None:
            look_by_merge_node(merge_node, is_head_node=True)
        else:
            head_nodes = self.dag.get_read_from_nodes()
            for head_node in head_nodes:
                look_end_to_write_node(head_node)
        # 如果存在目标节点，则添加目标节点
        target_node = self.dag.get_target_node()
        if target_node is not None:
            self.command.append(command_generator(target_node))

    def get(self):
        job = Job(name=self.name, id=self.id, pipeline=self)
        if job.id is not None:
            self.job = job
            self._dag = job.dag
            self.dag = Dag.to_instance(job.dag, self.name)
            self.id = job.id
            self.job.dag = self.dag
            self.dag.jobType = self.job.jobType
            self._node_map = {node["id"]: node for node in self.dag.dag["nodes"]}
            self._set_default_stage()
            self._set_lines()
            self._set_sources()
            self._set_merge_node()
            self._set_command()

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
        p.command = self.command
        p._union_node = self._union_node
        p.depends_on = self.depends_on
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
                "dateTime": int(start_time),
                "timezone": tz,
                "pointType": t,
                "connectionId": source_connections[i].connectionId,
                "connectionName": source_connections[i].name,
                "nodeId": source_connections[i].id,
                "nodeName": source_connections[i].name
            })
        config["syncPoints"] = syncPoints
        config["syncType"] = "sync"
        config["type"] = "cdc"
        self.config(config)
        return self

    def save(self):
        if self.job is not None:
            self.job.pipeline = self
            self.job.config(self.dag.setting)
            self.job.dag = self.dag
            self.job.save()
            return self

        job = Job(name=self.name, pipeline=self)
        job.validateConfig = self.validateConfig
        self.job = job
        self.job.pipeline = self
        self.job.config(self.dag.setting)
        self.job.dag = self.dag
        self.config({})
        job.save()
        self.id = job.id
        return self
    
    def start_at(self, start_time, tz="+8"):
        """
        设置时，进入cdc模式，并设置cdc开始时间
        :param start_time: 开始时间戳
        :param tz: 时区
        """
        return self.config_cdc_start_time(start_time, tz)

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
        self.id = job.id
        return self

    def show(self):
        command = ""
        for i in self.command:
            command = command + "." + i[0] + "(" + ", ".join([str(j) for j in i[1:]]) + ")"
        # 获取当前调用的类名
        class_name = self.__class__.__name__
        command = f"{class_name}({self.name}){command}"
        return command

    def preview(self, quiet=False):
        self.save()
        if self.job is None:
            return
        self.job.preview(quiet=False)
        return self


    @help_decorate("stop this pipeline job", args="p.stop()")
    def stop(self, force=False):
        if self.job is None:
            # logger.fwarn("pipeline {} not start, can not stop", self.name)
            return self
        self.job.stop(force=force)
        time.sleep(0.1)
        self.job.stop(force=force)
        time.sleep(0.1)
        self.job.stop(force=force)
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
    def __init__(self, name=None, mode=None, id=None):
        super().__init__(name=name, mode=mode, id=id)

class Flow(Pipeline):
    def __init__(self, name=None, mode=mview, id=None):
        super().__init__(name=name, mode=mode, id=id)
        global _flows
        _flows[name] = self
