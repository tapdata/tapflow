from typing import Any
from tapflow.lib.data_pipeline.base_node import BaseNode
from tapflow.lib.data_pipeline.job import JobType, JobStatus
from tapflow.lib.data_pipeline.nodes.source import Source
from tapflow.lib.data_pipeline.nodes.filter import Filter


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

    def to_dict(self):
        """
        return dag in dict format
        """
        return {
            "edges": [{"source": source, "target": target} for source in self.graph for target in self.graph[source]],
            "nodes": [self.node_map[node_id].to_dict() for node_id in self.node_map]
        }
    
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

    def replace_node(self, node):
        for i in range(len(self.dag["nodes"])):
            if self.dag["nodes"][i]["id"] == node["id"]:
                self.dag["nodes"][i] = node
                return
        self.dag["nodes"].append(node)

    def edge(self, s, sink):
        """
        add edge by source and sink
        """
        if hasattr(s, "stage"):
            s = s.stage

        self.add_edge(s, sink)
        self.add_extra_nodes_and_edges(None, s, sink)

    def add_extra_nodes_and_edges(self, mergeNode, s, sink):

        # 1. add extra merge nodes
        # 2. add (s, sink).dag["nodes"] to self.dag["nodes"] if not exist
        extra_merge_node_id = []
        for k in (s, sink):
            try:
                for node in k.dag.node_map.values():
                    if node.get("type") == 'merge_table_processor':
                        extra_merge_node_id.append(node["id"])
                    if self.node_map.get(node.id) is None:
                        self.add_node(node)
            except Exception as e:
                pass

        # 3. add (s, sink).dag["edges"] to self.dag["edges"] if not exist
        for k in (s, sink):
            try:
                for source_id in k.dag.graph.keys():
                    if source_id in extra_merge_node_id and mergeNode is not None:
                        source_id = mergeNode.id
                for target_id in k.dag.graph[source_id]:
                    if target_id in extra_merge_node_id and mergeNode is not None:
                        target_id = mergeNode.id
                    # edge not exists
                    if self.get_node(source_id) and target_id not in self.node_map[source_id] and source_id != target_id:
                        self.add_edge(self.get_node(source_id), self.get_node(target_id))
            except Exception as e:
                pass


# generate dag stage, used by dag object, stage is used to describe a dag in server
def gen_dag_stage(obj):
    return obj.to_dict()
