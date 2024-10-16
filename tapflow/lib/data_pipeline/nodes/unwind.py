from tapflow.lib.data_pipeline.base_obj import BaseObj


class Unwind(BaseObj):
    def __init__(self, name="unwind", path=None, index_name=None, mode="FLATTEN", array_elem="BASIC", joiner="_", keep_null=True, id=None):
        super().__init__()
        self.name = name
        self.path = path
        self.index_name = index_name
        self.mode = mode
        self.array_elem = array_elem
        self.joiner = joiner
        self.keep_null = keep_null
        if id is not None:
            self.id = id

    def to_dict(self):
        return {
            "arrayModel": self.array_elem,
            "catalog": "processor",
            "elementType": "Node",
            "id": self.id,
            "includeArrayIndex": self.index_name,
            "joiner": self.joiner,
            "name": self.name,
            "path": self.path,
            "preserveNullAndEmptyArrays": self.keep_null,
            "processorThreadNum": 1,
            "type": "unwind_processor",
            "unwindModel": self.mode
        }
    
    @classmethod
    def to_instance(cls, node_dict):
        return cls(id=node_dict["id"],
                   name=node_dict["name"],
                   path=node_dict["path"],
                   index_name=node_dict["includeArrayIndex"],
                   mode=node_dict["unwindModel"],
                   array_elem=node_dict.get("arrayModel", "BASIC"),
                   joiner=node_dict["joiner"],
                   keep_null=node_dict["preserveNullAndEmptyArrays"])