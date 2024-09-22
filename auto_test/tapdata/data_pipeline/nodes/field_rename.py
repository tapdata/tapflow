from auto_test.tapdata.data_pipeline.base_obj import BaseObj


class FieldRename(BaseObj):
    def __init__(self, config):
        super().__init__()
        self.config = config
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
            "name": "Field Rename",
            "type": "field_rename_processor"
        }
