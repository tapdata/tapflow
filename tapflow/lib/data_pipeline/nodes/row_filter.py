
from tapflow.lib.data_pipeline.base_obj import BaseObj


class RowFilterType:
    retain = "retain"
    discard = "discard"


class RowFilter(BaseObj):
    def __init__(self, expression, row_filter_type=RowFilterType.retain):
        super().__init__()
        self.expression = expression
        self.rowFilterType = row_filter_type

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

