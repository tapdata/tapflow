from tapflow.lib.data_pipeline.job import JobType, JobStatus
from tapflow.lib.data_pipeline.nodes.js import Js
from tapflow.lib.data_pipeline.nodes.source import Source
from tapflow.lib.data_pipeline.nodes.sink import Sink
from tapflow.lib.data_pipeline.nodes.filter import Filter
from tapflow.lib.data_pipeline.nodes.merge import Merge
from tapflow.lib.data_pipeline.nodes.rename_table import RenameTable
from tapflow.lib.data_pipeline.nodes.row_filter import RowFilter
from tapflow.lib.data_pipeline.nodes.field_rename import FieldRename
from tapflow.lib.data_pipeline.nodes.type_filter import TypeFilterNode
from tapflow.lib.data_pipeline.nodes.union import UnionNode
from tapflow.lib.data_pipeline.nodes.unwind import Unwind
from tapflow.lib.data_pipeline.nodes.time_add import TimeAdd
from tapflow.lib.data_pipeline.nodes.time_adjust import TimeAdjust
from tapflow.lib.data_pipeline.nodes.type_modification import TypeAdjust
from tapflow.lib.data_pipeline.nodes.column_filter import ColumnFilter


# used to describe a pipeline job


class Dag:
    def __init__(self, name=""):
        self.name = name
        self.status = JobStatus.edit
        self.dag = {
            "edges": [],
            "nodes": []
        }
        self.jobType = JobType.migrate
        self.setting = {
            "distinctWriteType": "intellect"
        }

    def config(self, config=None):
        if config is None:
            return self.setting
        self.setting.update(config)

    def replace_node(self, node):
        for i in range(len(self.dag["nodes"])):
            if self.dag["nodes"][i]["id"] == node["id"]:
                self.dag["nodes"][i] = node
                return
        self.dag["nodes"].append(node)

    def edge(self, s, sink):
        source = s.stage
        _source = None
        _sink = None

        for node in self.dag["nodes"]:
            if node["id"] == source.id:
                _source = node
            if node["id"] == sink.id:
                _sink = node

        if source.source is not None:
            sink.source = source.source
        if type(source) == Source:
            sink.source = source

        if source.id == sink.id:
            return

        if _source is None or _source.get("type") == 'merge_table_processor':
            _source = gen_dag_stage(source)
            if _source.get("type") == 'merge_table_processor':
                self.replace_node(_source)
            else:
                self.dag["nodes"].append(_source)

        if _sink is None or _source.get("type") == 'merge_table_processor':
            _sink = gen_dag_stage(sink)
            if _sink.get("type") == 'merge_table_processor':
                self.replace_node(_sink)
            else:
                self.dag["nodes"].append(_sink)

        if isinstance(source, Filter) or isinstance(sink, Filter):
            nodes = []
            for node in self.dag["nodes"]:
                if node["type"] == "table":
                    node["isFilter"] = True
                nodes.append(node)

        self.dag["edges"].append({
            "source": source.id,
            "target": sink.id
        })

        self.add_extra_nodes_and_edges(None, s, sink)

    def add_extra_nodes_and_edges(self, mergeNode, s, sink):
        extra_merge_node_id = []
        for k in (s, sink):
            try:
                for n in k.dag.dag["nodes"]:
                    if n.get("type") == 'merge_table_processor':
                        extra_merge_node_id.append(n["id"])
                        continue
                    n_exists = False
                    for n2 in self.dag["nodes"]:
                        if n2["id"] == n["id"]:
                            n_exists = True
                    if n_exists:
                        continue
                    self.dag["nodes"].append(n)
            except Exception as e:
                pass

        for k in (s, sink):
            try:
                for n in k.dag.dag["edges"]:
                    n_exists = False
                    if n["source"] in extra_merge_node_id and mergeNode is not None:
                        n["source"] = mergeNode.id
                    if n["target"] in extra_merge_node_id and mergeNode is not None:
                        n["target"] = mergeNode.id
                    for n2 in self.dag["edges"]:
                        if n["source"] == n2["source"] and n["target"] == n2["target"]:
                            n_exists = True
                    if n_exists:
                        continue
                    if n["source"] == n["target"]:
                        continue
                    self.dag["edges"].append(n)
            except Exception as e:
                pass


# generate dag stage, used by dag object, stage is used to describe a dag in server
def gen_dag_stage(obj):
    return obj.to_dict()