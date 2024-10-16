from tapflow.lib.data_pipeline.base_obj import BaseObj

class FieldCalculate(BaseObj):

    node_type = "field_calc_processor"

    def __init__(self, id=None, name="Field Calculate", table_name=None):
        super().__init__()
        if id is not None:
            self.id = id
        self.name = name
        self.script = {}
        self.table_name = table_name

    def update(self, field, script):
        if self.table_name is None:
            raise ValueError("table_name is required")
        self.script.update({field: {
            "field": field,
            "id": f"{self.id}_{self.table_name}_{field}",
            "lebel": field,
            "script": script,
            "scriptType": "js",
            "tableName": self.table_name,
        }})

    def delete(self, field):
        self.script.pop(field)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "type": "field_calc_processor",
            "catalog": "processor",
            "concurrentNum": 2,
            "disabled": False,
            "elementType": "Node",
            "enableConcurrentProcess": False,
            "isTransformed": False,
            "scripts": self.script.values(),
        }
    
    @classmethod
    def to_instance(cls, node_dict):
        if len(node_dict["scripts"]) == 0:
            table_name = ""
        else:
            table_name = node_dict["scripts"][0]["tableName"]
        f_node = cls(id=node_dict["id"],
                   name=node_dict["name"],
                   table_name=table_name)
        for field in node_dict["scripts"]:
            f_node.update(field["field"], field["script"])
        return f_node

