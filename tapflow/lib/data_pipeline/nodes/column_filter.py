import uuid

from tapflow.lib.data_pipeline.nodes.filter import Filter
from tapflow.lib.data_pipeline.base_node import FilterType


class ColumnFilter(Filter):
    node_type = "js_processor"
    def __init__(self, f, filter_type=FilterType.keep, id=None, name="Column Filter"):
        super().__init__(f, filter_type)
        self.id = id or str(uuid.uuid4())
        self.name = name
        self.f = {filter_type: f}

    def _to_js(self):
        keep = self.f.get(FilterType.keep)
        delete = self.f.get(FilterType.delete)
        if keep:
            return '''
        keepFields = %s;
        newRecord = {};
        for (i in keepFields) {
            newRecord[keepFields[i]] = record[keepFields[i]];
        }
        return newRecord;
        ''' % (str(keep))

        return '''
    deleteFields = %s;
    newRecord = record;
    for (i in deleteFields) {
        delete(newRecord[deleteFields[i]]);
    }
    return newRecord;
    ''' % (str(delete))

    def to_js(self):
        return "function process(record){\n\n\t// Enter you code at here\n%s}" % self._to_js()

    def to_declareScript(self):
        return ""
    
    def to_dict(self):
        return {
            "attrs": {
                "accessNodeProcessId": "",
                "connectionType": "source_and_target",
                "position": [0, 0]
            },
            "id": self.id,
            "name": self.name,
            "type": self.node_type,
            "script": self.to_js(),
            "declareScript": self.to_declareScript(),
        }
        
