from tapflow.lib.data_pipeline.base_obj import BaseObj


class Py(BaseObj):
    def __init__(self, script, declareScript, context={}):
        self.origin_script = script
        super().__init__()
        self.python_header = "import json, random, time, datetime, uuid, types, yaml\nimport urllib, urllib2, requests\nimport math, hashlib, base64\ndef process(record, context):\"\"\"\nDetailed context description can be found in the API documentation\ncontext = {\n  \"event\": {},  #Data source event type, table name, and other information\n  \"before\": {}, #Content before data changes\n  \"info\": {},   #Data source event information\n  \"global\": {}  #A state storage container on the node dimension within the task cycle\n}\n\"\"\"\n# Enter you code at here\n"
        context_str = ""
        if (context != None and len(context) > 0):
            for k, v in self.context.items():
                if type(v) in [type(1), type(0.1)]:
                    context_str += "context[\""+str(k)+"\"]="+str(v)+"\n"
                else:
                    context_str += "context[\""+str(k)+"\"]=\""+str(v)+"\"\n"
        self.script = context_str + script
        self.declareScript = declareScript

    def update_script(self, script):
        self.script = script


    def to_dict(self):
        return {
            "script": self.python_header + self.script,
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
