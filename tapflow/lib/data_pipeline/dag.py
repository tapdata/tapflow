from typing import Any
from tapflow.lib.data_pipeline.base_node import BaseNode
from tapflow.lib.data_pipeline.job import JobType, JobStatus
from tapflow.lib.data_pipeline.nodes.merge import MergeNode
from tapflow.lib.data_pipeline.nodes.sink import Sink
from tapflow.lib.data_pipeline.nodes.source import Source
from tapflow.lib.data_pipeline.nodes.filter import Filter
from tapflow.lib.data_pipeline.nodes import get_node_instance


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
        }

        self.graph = {}
        self.node_map = {}

    def add_node(self, node):
        """
        add node to graph, and if node in node_map, update it
        """
        self.node_map[node.id] = node
        if node.id in self.graph:
            return
        self.graph[node.id] = []

    def add_edge(self, source, target):
        """
        add edge by source and target
        """
        if source is None or target is None:
            return
        
        if source.id == target.id:
            return
        
        # node not in graph, add it
        if source.id not in self.graph:
            self.add_node(source)
        if target.id not in self.graph:
            self.add_node(target)
        
        # avoid duplicate edge
        if target.id not in self.graph[source.id]:
            self.graph[source.id].append(target.id)

    def delete_node(self, node_id):
        """
        delete node by node_id
        """
        if node_id not in self.graph:
            return
        del self.graph[node_id]
        for source in self.graph:
            if node_id in self.graph[source]:
                self.graph[source].remove(node_id)
        del self.node_map[node_id]

    def delete_edge(self, source_id, target_id):
        """
        delete edge by source_id and target_id
        """
        if source_id == target_id:
            return
        if source_id not in self.graph or target_id not in self.graph:
            return
        if target_id in self.graph[source_id]:
            self.graph[source_id].remove(target_id)

    def get_source_node(self, target_node_id):
        """
        获取target_node_id的父节点, 如果没有父节点, 则返回target_node_id
        """
        for g in self.graph:
            if target_node_id in self.graph[g]:
                return self.get_node(g)
        return self.get_node(target_node_id)
    
    def get_read_from_nodes(self):
        """
        获取所有源节点
        1. 源节点必须有子节点
        2. 源节点必须是Source节点
        3. 源节点不能有父节点
        """
        child_nodes = set()
        for childs in self.graph.values():
            for child in childs:
                child_nodes.add(child)
        source_nodes = []
        for node_id in self.graph:
            if isinstance(self.get_node(node_id), Source) and len(self.graph[node_id]) > 0 and node_id not in child_nodes:
                source_nodes.append(self.get_node(node_id))
        return source_nodes
    
    def get_target_node(self):
        """
        获取目标节点, Dag只有一个目标节点
        1. 目标节点必须有父节点
        2. 目标节点不能有子节点
        3. 目标节点必须是Sink节点
        """
        for node_id in self.graph:
            if len(self.graph[node_id]) == 0:
                return self.get_node(node_id) if isinstance(self.get_node(node_id), Sink) else None
        return None

    def to_dict(self):
        """
        return dag in dict format
        """
        set = {
            "edges": [{"source": source, "target": target} for source in self.graph for target in self.graph[source]],
            "nodes": [self.node_map[node_id].to_dict() for node_id in self.node_map],
        }
        set.update(self.setting)
        return set
    
    def get_node(self, node_id):
        """
        get node by node_id, return None if node_id not in graph
        """
        return self.node_map.get(node_id)
    
    def __getattribute__(self, name: str) -> Any:
        """
        override __getattribute__ to return dag object instead of dict
        """
        if name == "dag":
            return self.to_dict()
        return super().__getattribute__(name)
    
    def update_node(self, node):
        """
        update node by node_id
        """
        self.node_map[node.id] = node

    def config(self, config=None):
        if config is None:
            return self.setting
        self.setting.update(config)

    def replace_node(self, old_node, new_node):
        self.add_node(new_node)
        for source_id in self.graph:
            # if old_node is source, replace it with new_node
            if source_id == old_node.id:
                self.graph[new_node.id] = self.graph[source_id]
                del self.graph[source_id]
                break
            # if old_node is target, replace it with new_node
            for index, target_id in enumerate(self.graph[source_id]):
                if target_id == old_node.id:
                    self.graph[source_id][index] = new_node.id
        self.delete_node(old_node.id)

    def edge(self, s, sink):
        """
        add edge by source and sink
        """

        self.add_edge(s.stage, sink)
        self.add_extra_nodes_and_edges(None, s, sink)

    def add_extra_nodes_and_edges(self, mergeNode, s, sink):
        # 1. add extra merge nodes
        # 2. add (s, sink).dag["nodes"] to self.dag["nodes"] if not exist
        extra_merge_node_id = []
        for k in (s, sink):
            try:
                for node in k.dag.node_map.values():
                    if isinstance(node, MergeNode):
                        extra_merge_node_id.append(node.id)
                        continue
                    if self.node_map.get(node.id) is None:
                        self.add_node(node)
            except Exception as e:
                pass
        
        # 3. add (s, sink).dag["edges"] to self.dag["edges"] if not exist
        for k in (s, sink):
            try:
                for source, targets in k.dag.graph.items():
                    if source in extra_merge_node_id and mergeNode is not None:
                        s_id = mergeNode.id
                    else:
                        s_id = source
                    for target in targets:
                        if target in extra_merge_node_id and mergeNode is not None:
                            t_id = mergeNode.id
                        else:
                            t_id = target
                        # not exist edge and source != target
                        if not (self.graph.get(s_id) and t_id in self.graph[s_id]) and s_id != t_id:
                            self.add_edge(self.get_node(s_id), self.get_node(t_id))
            except Exception as e:
                pass

    @classmethod
    def to_instance(cls, dag_dict, name):
        dag = Dag(name=name)
        for node in dag_dict["nodes"]:
            dag.add_node(get_node_instance(node))
        for edge in dag_dict["edges"]:
            dag.add_edge(dag.get_node(edge["source"]), dag.get_node(edge["target"]))
        return dag

# generate dag stage, used by dag object, stage is used to describe a dag in server
def gen_dag_stage(obj):
    return obj.to_dict()
