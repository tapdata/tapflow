from tapflow.lib.data_pipeline.base_obj import BaseObj


class Rename(BaseObj):
    def __init__(self, ori, new):
        super().__init__()
        self.ori = ori
        self.new = new

    def to_js(self):
        return '''
    record["%s"] = record["%s"];
    delete(record["%s"]);
    return record;
        ''' % (self.new, self.ori, self.ori)
