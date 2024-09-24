from tapflow.lib.data_pipeline.base_obj import BaseObj


class UnionNode(BaseObj):
    def __init__(self):
        super().__init__()

    def to_dict(self):
        return {
            "id": self.id,
            "catalog": "processor",
            "elementType": "Node",
            "name": "Union",
            "type": "union_processor"
        }

