import uuid

from tapflow.lib.data_pipeline.nodes.filter import Filter
from tapflow.lib.data_pipeline.base_node import FilterType


class ColumnFilter(Filter):
    def __init__(self, f, filter_type=FilterType.keep):
        super().__init__(f, filter_type)
        self.id = str(uuid.uuid4())
        self.f = {filter_type: f}

    def to_js(self):
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
}
    ''' % (str(delete))

    def to_declareScript(self):
        return ""
