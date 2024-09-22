from auto_test.tapdata.data_pipeline.base_obj import BaseObj
from auto_test.tapdata.data_pipeline.base_node import FilterType


class Filter(BaseObj):
    def __init__(self, f, filter_type=FilterType.keep):
        super().__init__()
        self.f = {filter_type: f}

    def _add_record_for_str(self, s):
        import re
        s1 = ""
        m1 = re.finditer('\"([^\"]*)\"', s)
        f = 0
        for i in m1:
            s1 = s1 + s[f:i.start()] + '""'
            f = i.end()
        s1 = s1 + s[f:]

        s2 = ""
        m2 = re.finditer(r'[a-zA-Z_][\.a-zA-Z0-9_]*', s1)
        f = 0
        for i in m2:
            s2 = s2 + s1[f:i.start()] + "record." + i.group()
            f = i.end()
        s2 = s2 + s1[f:]

        s3 = ""
        m3l = list(re.finditer('\"([^\"]*)\"', s2))
        f = 0
        m1l = list(re.finditer('\"([^\"]*)\"', s))
        for i in range(len(m3l)):
            s3 = s3 + s2[f:m3l[i].start()] + m1l[i].group()
            f = m3l[i].end()
        s3 = s3 + s2[f:]

        return s3

    def to_js(self):
        keep = self.f.get("keep")
        delete = self.f.get("delete")
        if keep:
            return '''
    if (%s) {
        return record;
    }
    return null;
        ''' % (self._add_record_for_str(keep))
        return '''
    if (%s) {
        return null;
    }
    return record;
        ''' % (self._add_record_for_str(delete))

    def to_declareScript(self):
        return ""

