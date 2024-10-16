from tapflow.lib.data_pipeline.base_obj import BaseObj

class Python(BaseObj):
    node_type = "python_processor"
    def __init__(self, script, declareScript, language="py", id=None, name="Python"):
        super().__init__()
        self.language = language
        self.script = script
        self.declareScript = declareScript
        if id is not None:
            self.id = id
        self.name = name

    def to_python(self):
        if self.func_header:
            return "import json, random, time, datetime, uuid, types, yaml\nimport urllib, urllib2, requests\nimport math, hashlib, base64\ndef process(record, context):%s" % self.script
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
            "script": self.to_python(),
            "declareScript": self.to_declareScript(),
        }
    
    @classmethod
    def to_instance(cls, node_dict):
        return cls(node_dict["script"], 
                   node_dict.get("declareScript", ""), 
                   language=node_dict.get("language", "py"),
                   id=node_dict.get("id", None),
                   name=node_dict.get("name", "Python"))
