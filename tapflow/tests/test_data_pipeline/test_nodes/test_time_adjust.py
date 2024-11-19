import unittest
from tapflow.lib.data_pipeline.nodes.time_adjust import TimeAdjust

class TestTimeAdjust(unittest.TestCase):
    def test_to_dict_add_hours(self):
        # 测试增加小时
        time_adjust = TimeAdjust(addHours=5)
        result = time_adjust.to_dict()
        self.assertTrue(result["add"])
        self.assertEqual(result["hours"], 5)
        self.assertEqual(result["dataTypes"], ["now"])
        self.assertEqual(result["name"], "Time Adjust")
        self.assertEqual(result["type"], "date_processor")

    def test_to_dict_subtract_hours(self):
        # 测试减少小时
        time_adjust = TimeAdjust(addHours=-3)
        result = time_adjust.to_dict()
        self.assertFalse(result["add"])
        self.assertEqual(result["hours"], 3)

    def test_to_instance(self):
        # 测试从字典创建实例
        node_dict = {
            "add": True,
            "hours": 4,
            "t": ["now"],
            "id": "test_id"
        }
        time_adjust = TimeAdjust.to_instance(node_dict)
        self.assertEqual(time_adjust.addHours, 4)
        self.assertEqual(time_adjust.t, ["now"])
        self.assertEqual(time_adjust.id, "test_id")

if __name__ == '__main__':
    unittest.main() 