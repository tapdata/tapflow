from tapflow.lib.data_pipeline.base_obj import BaseObj


class Py(BaseObj):
    def __init__(self, script, declareScript):
        self.origin_script = script
        super().__init__()
        self.python_header = "import json, random, time, datetime, uuid, types, yaml\nimport urllib, urllib2, requests\nimport math, hashlib, base64\ndef process(record, context):\"\"\"\nDetailed context description can be found in the API documentation\ncontext = {\n  \"event\": {},  #Data source event type, table name, and other information\n  \"before\": {}, #Content before data changes\n  \"info\": {},   #Data source event information\n  \"global\": {}  #A state storage container on the node dimension within the task cycle\n}\n\"\"\"\n# Enter you code at here\n"
        self.declareScript = declareScript

    def update_script(self, script):
        self.script = script


    def to_dict(self):
        final_script = self.script
        if "def process(record, context)" not in self.script:
            final_script = self.python_header + self.script

        return {
            "script": final_script,
            "declareScript": self.declareScript,
            "concurrentNum": 1,
            "type": "python_processor",
            "catalog": "processor",
            "isTransformed": False,
            "id": self.id,
            "name": "Python",
            "elementType": "Node",
            "disabled": False
        }
