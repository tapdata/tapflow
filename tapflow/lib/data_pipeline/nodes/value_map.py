from tapflow.lib.data_pipeline.base_obj import BaseObj


class ValueMap(BaseObj):
    def __init__(self, field, value):
        super().__init__()
        self.field = field
        self.value = value

    def to_js(self):
        return '''
    record["%s"] = %s;
        ''' % (self.field, self.value)
