import unittest
from tapflow.lib.data_pipeline.nodes.time_add import TimeAdd

class TestTimeAdd(unittest.TestCase):
    def test_to_dict_default_field(self):
        # 测试默认字段
        time_add = TimeAdd()
        result = time_add.to_dict()
        self.assertEqual(result["dateFieldName"], "created_at")
        self.assertFalse(result["disabled"])
        self.assertEqual(result["catalog"], "processor")
        self.assertEqual(result["elementType"], "Node")
        self.assertEqual(result["name"], "Add a time field")
        self.assertEqual(result["type"], "add_date_field_processor")

    def test_to_dict_custom_field(self):
        # 测试自定义字段
        custom_field = "updated_at"
        time_add = TimeAdd(field=custom_field)
        result = time_add.to_dict()
        self.assertEqual(result["dateFieldName"], custom_field)

if __name__ == '__main__':
    unittest.main() 