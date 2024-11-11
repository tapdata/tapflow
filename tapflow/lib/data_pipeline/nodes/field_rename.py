from tapflow.lib.data_pipeline.base_obj import BaseObj


class FieldRename(BaseObj):
    def __init__(self, config, id=None, name="Field Rename"):
        super().__init__()
        self.config = config
        if id is not None:
            self.id = id
        self.name = name

    def to_dict(self):
        operations = []
        for b, a in self.config.items():
            operations.append({
                "field": b,
                "op": "RENAME",
                "operand": a
            })
        return {
            "id": self.id,
            "operations": operations,
            "catalog": "processor",
            "elementType": "Node",
            "name": self.name,
            "type": "field_rename_processor"
        }
    
    @classmethod
    def to_instance(cls, node_dict):
        config = {op["field"]: op["operand"] for op in node_dict["operations"]}
        return cls(config,
                   id=node_dict["id"],
                   name=node_dict["name"])
