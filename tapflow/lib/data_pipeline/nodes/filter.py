from tapflow.lib.data_pipeline.base_obj import BaseObj
from tapflow.lib.data_pipeline.base_node import FilterType
import re


class Filter(BaseObj):
    def __init__(self, f, filter_type=FilterType.keep, id=None, name=None):
        super().__init__()
        self.f = {filter_type: f}
        if id is not None:
            self.id = id
        self.name = name

    def f_to_expression(self):
        # 使用正则表达式匹配变量名，并为其添加前缀
        # 先不匹配在引号里的内容
        def replace_variable(match):
            var = match.group(1)
            # 不对操作符两侧的字符串做处理
            if var not in ['==', '>', '<', '&&', '||', '>=', '<=', '!='] and not var.startswith("record."):
                return f'record.{var}'
            return var

        # 匹配非引号内的变量名，排除操作符，避免替换掉字面量
        expression = list(self.f.values())[0]
        return re.sub(r"(?<!['\"])(\b[a-zA-Z_][a-zA-Z0-9_]*\b)(?!['\"])", replace_variable, expression)

    def to_dict(self):
        action = "retain"
        if list(self.f.keys())[0] != "keep":
            action = "discard"

        exp = self.f_to_expression()
        return {
            "action": action,
            "expression": exp,
            "concurrentNum": 1,
            "type": "row_filter_processor",
            "catalog": "processor",
            "isTransformed": False,
            "id": self.id,
            "name": self.name,
            "elementType": "Node",
            "disabled": False
        }

    def _add_record_for_str(self, s):
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
    
    @classmethod
    def to_instance(cls, node_dict):
        return cls(node_dict["expression"],
                   id=node_dict["id"],
                   name=node_dict["name"])
