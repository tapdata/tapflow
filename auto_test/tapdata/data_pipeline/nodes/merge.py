from typing import Iterable, Tuple, Sequence

from tapshell.tapdata_cli.log import logger
from auto_test.tapdata.data_pipeline.base_node import WriteMode

from auto_test.tapdata.data_pipeline.base_obj import BaseObj


class MergeNode(BaseObj):

    def __init__(self,
                 node_id: str,
                 table_name: str,
                 association: Iterable[Sequence[Tuple[str, str]]],
                 mergeType=WriteMode.updateOrInsert,
                 targetPath="",
                 isArray=False,
                 arrayKeys=[],
                 join_value_change=False,
                 ):
        self.node_id = node_id
        self.table_name = table_name
        self.mergeType = mergeType
        self.targetPath = targetPath
        self.association = association
        self.father = None
        self.child = []
        self.isArray = isArray
        self.arrayKeys=arrayKeys
        self.join_value_change = join_value_change
        super(MergeNode, self).__init__()

    def to_dict(self):
        return {
            "id": self.node_id,
            "isArray": self.isArray,
            "arrayKeys": self.arrayKeys,
            "joinKeys": [{"source": i[0], "target": i[1]} for i in self.association],
            "mergeType": self.mergeType,
            "targetPath": self.targetPath,
            "enableUpdateJoinKeyValue": self.join_value_change,
            "children": [i.to_dict() for i in self.child],
            "tableName": self.table_name
        }

    def update(self, node):
        self.mergeType = node.mergeType
        self.targetPath = node.targetPath
        self.association = node.association
        self.isArray = node.isArray
        self.arrayKeys = node.arrayKeys
        for nc in node.child:
            b = False
            for c in self.child:
                if c.table_name == nc.table_name:
                    c.update(nc)
                    b = True
                    break
            if not b:
                self.child.append(nc)

    def add(self, node):
        if not hasattr(node, 'father'):
            logger.fwarn("{}", "the node must be the instance of class MergeNode")
            return
        node.father = self
        self.child.append(node)


class Merge(MergeNode):

    def __init__(self,
                 node_id: str,
                 table_name: str,
                 association: Iterable[Sequence[Tuple[str, str]]],
                 mergeType=WriteMode.updateOrInsert,
                 targetPath="",
                 isArray=False,
                 arrayKeys=[],
                 join_value_change=False,
                 ):
        super(Merge, self).__init__(
            node_id,
            table_name,
            association,
            mergeType,
            targetPath,
            isArray,
            arrayKeys,
            join_value_change
        )

    def to_dict(self, is_head=False):
        # if the node is head node
        if self.father is None or is_head:
            d = {
                "type": "merge_table_processor",
                "processorThreadNum": 1,
                "name": "主从合并",
                "mergeProperties": [{
                    "children": [i.to_dict() for i in self.child],
                    "id": self.node_id,
                    "isArray": self.isArray,
                    "arrayKeys": self.arrayKeys,
                    "tableName": self.table_name,
                    "mergeType": "updateOrInsert",
                    "enableUpdateJoinKeyValue": self.join_value_change
                }],
                "id": self.id,
                "elementType": "Node",
                "catalog": "processor",
                "attrs": {
                    "position": [0, 0]
                }
            }
        else:
            d = {
                "id": self.node_id,
                "isArray": self.isArray,
                "joinKeys": [{"source": i[0], "target": i[1]} for i in self.association],
                "mergeType": self.mergeType,
                "targetPath": self.targetPath,
                "arrayKeys": self.arrayKeys,
                "children": [i.to_dict() for i in self.child],
                "tableName": self.table_name,
                "enableUpdateJoinKeyValue": self.join_value_change
            }
        return d
