import uuid

class BaseObj:
    def __init__(self):
        self.source = None
        self.id = str(uuid.uuid4())
        self.func_header = True
