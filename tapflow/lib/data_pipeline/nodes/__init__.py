from tapflow.lib.data_pipeline.base_node import BaseNode
from tapflow.lib.data_pipeline.nodes.field_add_del import FieldAddDel
from tapflow.lib.data_pipeline.nodes.field_calculate import FieldCalculate
from tapflow.lib.data_pipeline.nodes.field_rename import FieldRename
from tapflow.lib.data_pipeline.nodes.sink import Sink
from tapflow.lib.data_pipeline.nodes.merge import Merge
from tapflow.lib.data_pipeline.nodes.time_adjust import TimeAdjust
from tapflow.lib.data_pipeline.nodes.type_modification import TypeAdjust
from tapflow.lib.data_pipeline.nodes.union import UnionNode
from tapflow.lib.data_pipeline.nodes.filter import Filter
from tapflow.lib.data_pipeline.nodes.rename import Rename
from tapflow.lib.data_pipeline.nodes.js import Js
from tapflow.lib.data_pipeline.nodes.python import Python
from tapflow.lib.data_pipeline.nodes.row_filter import RowFilter
from tapflow.lib.data_pipeline.nodes.unwind import Unwind


NODE_MAP = {
    "table": Sink,
    "merge_table_processor": Merge,
    "union_processor": UnionNode,
    "js_processor": Js,
    "python_processor": Python,
    "row_filter_processor": RowFilter,
    "field_calc_processor": FieldCalculate,
    "field_rename_processor": FieldRename,
    "field_mod_type_processor": TypeAdjust,
    "field_add_del_processor": FieldAddDel,
    "filter_processor": Filter,
    "unwind_processor": Unwind,
    "date_processor": TimeAdjust,
}


def get_node_instance(node_dict: dict) -> BaseNode:
    """
    根据节点字典获取节点实例
    :param node_dict: 节点字典
    :return: 节点实例
    """
    node_type = node_dict.get("type", None)
    if node_type is None:
        raise ValueError("Invalid node_dict")
    node_class = NODE_MAP.get(node_type, None)
    if node_class is None:
        raise ValueError(f"Node type {node_type} not supported")
    return node_class.to_instance(node_dict)
