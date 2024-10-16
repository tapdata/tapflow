from tapflow.lib.data_pipeline.base_obj import BaseObj


class FieldAddDel(BaseObj):
    
    node_name = "字段增删"
    node_type = "field_add_del_processor"

    def __init__(self, id=None, name="字段增删", operations=[]):
        super().__init__()
        if id is not None:
            self.id = id
        self.name = name
        self.operations = []

    def delete_field(self, field):
        self.operations.append({
            "field": field,
            "op": "REMOVE",
            "operand": True,
        })

    def add_field(self, field, type, java_type=None):
        self.operations.append({
            "field": field,
            "op": "CREATE",
            "type": type,
            "javaType": java_type if java_type else type,
        })

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "operations": self.operations,
            "catalog": "processor",
            "elementType": "Node",
            "type": self.node_type,
            "disabled": False,
        }
    
    @classmethod
    def to_instance(cls, node_dict):
        return cls(id=node_dict["id"],
                   name=node_dict["name"],
                   operations=node_dict["operations"])
