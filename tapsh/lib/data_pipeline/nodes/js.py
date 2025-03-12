from tapsh.lib.data_pipeline.base_obj import BaseObj


class Js(BaseObj):
    node_type = "js_processor"
    def __init__(self, script, declareScript, func_header=True, language="js", id=None, name="JS"):
        super().__init__()
        self.language = language
        self.origin_script = script
        self.script = script
        self.declareScript = declareScript
        self.func_header = func_header
        if id is not None:
            self.id = id
        self.name = name

    def update_script(self, script):
        self.script = script

    def to_js(self):
        if self.func_header and "function process(record)" not in self.script:
            return "function process(record){\n\n\t// Enter you code at here\n%s}" % self.script
        else:
            return self.script

    def to_declareScript(self):
        return self.declareScript
    
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
            "script": self.script,
            "declareScript": self.to_declareScript(),
            "script": self.to_js()
        }
    
    @classmethod
    def to_instance(cls, node_dict):
        return cls(node_dict.get("script", ""), 
                   node_dict.get("declareScript", ""), 
                   func_header=node_dict.get("func_header", True), 
                   language=node_dict.get("language", "js"),
                   id=node_dict.get("id", None),
                   name=node_dict.get("name", "JS"))
