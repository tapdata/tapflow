from lib.data_pipeline.base_obj import BaseObj


class Js(BaseObj):
    def __init__(self, script, declareScript, func_header=True, language="js"):
        super().__init__()
        self.language = language
        self.script = script
        self.declareScript = declareScript
        self.func_header = func_header

    def to_js(self):
        return self.script

    def to_declareScript(self):
        return self.declareScript

