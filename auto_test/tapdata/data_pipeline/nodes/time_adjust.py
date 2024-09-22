from auto_test.tapdata.data_pipeline.base_obj import BaseObj


# 时间调整节点
class TimeAdjust(BaseObj):
    def __init__(self, addHours=0, t=None):
        super().__init__()
        if t is None:
            t = ["now"]
        self.addHours = addHours
        self.t = t

    def to_dict(self):
        return {
            "add": True if self.addHours > 0 else False,
            "dataTypes": self.t,
            "disabled": False,
            "hours": self.addHours if self.addHours > 0 else -self.addHours,
            "id": self.id,
            "catalog": "processor",
            "elementType": "Node",
            "name": "Time Adjust",
            "type": "date_processor",
        }
