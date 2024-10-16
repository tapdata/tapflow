from tapflow.lib.data_pipeline.base_obj import BaseObj
from tapflow.lib.data_pipeline.nodes.js import Js


class Rename(Js):
    def __init__(self, ori, new):
        self.ori = ori
        self.new = new
        super().__init__(self._to_js(), "", name="Rename")

    def _to_js(self):
        return '''
    record["%s"] = record["%s"];
    delete(record["%s"]);
    return record;
        ''' % (self.new, self.ori, self.ori)
