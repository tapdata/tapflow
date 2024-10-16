from tapflow.lib.data_pipeline.base_obj import BaseObj


class UnionNode(BaseObj):
    def __init__(self, id=None, name="Union", concurrentNum=2, enableConcurrentProcess=False):
        super().__init__()
        if id is not None:
            self.id = id
        self.name = name
        self.concurrentNum = concurrentNum
        self.enableConcurrentProcess = enableConcurrentProcess

    def to_dict(self):
        return {
            "id": self.id,
            "catalog": "processor",
            "elementType": "Node",
            "name": self.name,
            "type": "union_processor",
            "concurrentNum": self.concurrentNum,
            "enableConcurrentProcess": self.enableConcurrentProcess
        }
    
    @classmethod
    def to_instance(cls, node_dict):
        return cls(id=node_dict["id"], 
                   name=node_dict["name"], 
                   concurrentNum=node_dict["concurrentNum"], 
                   enableConcurrentProcess=node_dict.get("enableConcurrentProcess", False))
