from typing import Iterable, List, Tuple, Sequence

from tapflow.lib.utils.log import logger
from tapflow.lib.data_pipeline.base_node import WriteMode

from tapflow.lib.data_pipeline.base_obj import BaseObj


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
                 id=None,
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
        if id is not None:
            self.id = id

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
        self.join_value_change = node.join_value_change
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
        if node not in self.child:
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
                 id=None,
                 ):
        super(Merge, self).__init__(
            node_id,
            table_name,
            association,
            mergeType,
            targetPath,
            isArray,
            arrayKeys,
            join_value_change,
            id=id
        )

    @classmethod
    def to_instance(cls, node_dict: dict, is_head=True) -> "Merge":
        """
        to_dict方法的逆向操作
        :param node_dict: API 返回的节点dict
        :param table_name: 源表名
        :return: 节点实例
        """
        # make head node
        if is_head:
            if node_dict.get("mergeProperties") is None:
                return None
            mergePropertie = node_dict["mergeProperties"][0]
            node = cls(mergePropertie["id"],
                    mergePropertie["tableName"],
                    [[i["target"], i["source"]] for i in mergePropertie.get("joinKeys", [])],
                    mergeType=mergePropertie['mergeType'],
                    targetPath=mergePropertie.get('targetPath', ''),
                    isArray=mergePropertie['isArray'],
                    arrayKeys=mergePropertie.get('arrayKeys', []),
                    join_value_change=mergePropertie.get('enableUpdateJoinKeyValue', False),
                id=node_dict["id"]
            )
            children = mergePropertie.get("children", [])
        else:
            node = MergeNode(node_dict["id"],
                    node_dict["tableName"],
                    [[i["target"], i["source"]] for i in node_dict.get("joinKeys", [])],
                    mergeType=node_dict['mergeType'],
                    targetPath=node_dict.get('targetPath', ''),
                    isArray=node_dict['isArray'],
                    arrayKeys=node_dict.get('arrayKeys', []),
                    join_value_change=node_dict.get('enableUpdateJoinKeyValue', False),
                )
            children = node_dict.get("children", [])
        for child in children:
            child_node = cls.to_instance(child, is_head=False)
            if child_node is not None:
                node.add(child_node)
        return node
    
    def find_by_node_id(self, node_id):
        if self.node_id == node_id:
            return self
        for child in self.child:
            result = child.find_by_node_id(node_id)
            if result is not None:
                return result
        return None

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
                    # "arrayKeys": self.arrayKeys,
                    "tableName": self.table_name,
                    "mergeType": "updateOrInsert",
                    "enableUpdateJoinKeyValue": self.join_value_change
                }],
                "id": self.id,
                "elementType": "Node",
                "mergeMode": "main_table_first",
                "disable": False,
                "isTransformed": False,
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
