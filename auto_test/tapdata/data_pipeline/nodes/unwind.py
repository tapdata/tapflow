from auto_test.tapdata.data_pipeline.base_obj import BaseObj


class Unwind(BaseObj):
    def __init__(self, name="unwind", path=None, index_name=None, mode="FLATTEN", array_elem="BASIC", joiner="_", keep_null=True):
        super().__init__()
        self.name = name
        self.path = path
        self.index_name = index_name
        self.mode = mode
        self.array_elem = array_elem
        self.joiner = joiner
        self.keep_null = keep_null
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
