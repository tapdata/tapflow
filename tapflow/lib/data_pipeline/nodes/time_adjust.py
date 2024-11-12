from tapflow.lib.data_pipeline.base_obj import BaseObj


# 时间调整节点
class TimeAdjust(BaseObj):
    def __init__(self, addHours=0, t=None, name="Time Adjust"):
        super().__init__()
        if t is None:
            t = ["now"]
        self.addHours = addHours
        self.t = t
        self.name = name

    def to_dict(self):
        return {
            "add": True if self.addHours > 0 else False,
            "dataTypes": self.t,
            "disabled": False,
            "hours": self.addHours if self.addHours > 0 else -self.addHours,
            "id": self.id,
            "catalog": "processor",
            "elementType": "Node",
            "name": self.name,
            "type": "date_processor",
        }
    
    @classmethod
    def to_instance(cls, node_dict: dict):
        if node_dict.get("add"):
            addHours = node_dict.get("hours")
        else:
            addHours = -node_dict.get("hours")
        t_inst = cls(addHours=addHours, t=node_dict.get("t", ["now"]))
        t_inst.id = node_dict.get("id")
        return t_inst