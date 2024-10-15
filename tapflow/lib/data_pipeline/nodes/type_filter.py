from tapflow.lib.data_pipeline.base_obj import BaseObj


class TypeFilterNode(BaseObj):
    def __init__(self, ts):
        self._filter_types = ts
        if type(ts) != list:
            self._filter_types = [ts]
        filter_types = []
        for i in range(len(self._filter_types)):
            filter_types.append(str(self._filter_types[i]))
        self._filter_types = filter_types
        super().__init__()

    def to_dict(self):
        return {
            "id": self.id,
            "filterTypes": self._filter_types,
            "concurrentNum": 1,
            "type": "field_mod_type_filter_processor",
            "catalog": "processor",
            "isTransformed": False,
            "name": "Type Filter",
            "elementType": "Node",
            "disabled": False
        }

