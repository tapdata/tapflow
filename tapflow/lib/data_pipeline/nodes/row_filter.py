
from tapflow.lib.data_pipeline.base_obj import BaseObj


class RowFilterType:
    retain = "retain"
    discard = "discard"


class RowFilter(BaseObj):
    def __init__(self, expression, row_filter_type=RowFilterType.retain, id=None, name="Row Filter"):
        super().__init__()
        self.expression = expression
        self.rowFilterType = row_filter_type
        if id is not None:
            self.id = id
        self.name = name

    def to_dict(self):
        return {
            "id": self.id,
            "action": self.rowFilterType,
            "catalog": "processor",
            "elementType": "Node",
            "expression": self.expression,
            "name": "Row Filter",
            "type": "row_filter_processor"
        }
    
    @classmethod
    def to_instance(cls, node_dict):
        return cls(node_dict["expression"], 
                   node_dict.get("action", RowFilterType.retain),
                   id=node_dict["id"],
                   name=node_dict["name"])

