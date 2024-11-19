import unittest
from tapflow.lib.data_pipeline.nodes.type_filter import TypeFilterNode

class TestTypeFilterNode(unittest.TestCase):
    def test_to_dict_single_type(self):
        # 测试单一类型过滤
        type_filter = TypeFilterNode(ts="int")
        result = type_filter.to_dict()
        self.assertEqual(result["filterTypes"], ["int"])
        self.assertEqual(result["concurrentNum"], 1)
        self.assertEqual(result["type"], "field_mod_type_filter_processor")
        self.assertEqual(result["name"], "Type Filter")
        self.assertFalse(result["isTransformed"])
        self.assertFalse(result["disabled"])

    def test_to_dict_multiple_types(self):
        # 测试多类型过滤
        type_filter = TypeFilterNode(ts=["int", "string"])
        result = type_filter.to_dict()
        self.assertEqual(result["filterTypes"], ["int", "string"])

if __name__ == '__main__':
    unittest.main() 