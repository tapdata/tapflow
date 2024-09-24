from tapflow.lib.data_pipeline.base_obj import BaseObj


class TimeAdd(BaseObj):
    def __init__(self, field="created_at"):
        super().__init__()
        self.field = field

    def to_dict(self):
        return {
            "dateFieldName": self.field,
            "disabled": False,
            "id": self.id,
            "catalog": "processor",
            "elementType": "Node",
            "name": "Add a time field",
            "type": "add_date_field_processor",
        }
