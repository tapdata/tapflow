import unittest
from tapflow.lib.data_pipeline.nodes.unwind import Unwind

class TestUnwind(unittest.TestCase):
    def test_to_dict_default(self):
        # 测试默认参数
        unwind_node = Unwind(id="test_id")
        result = unwind_node.to_dict()
        self.assertEqual(result["id"], "test_id")
        self.assertEqual(result["name"], "unwind")
        self.assertEqual(result["type"], "unwind_processor")
        self.assertEqual(result["arrayModel"], "BASIC")
        self.assertEqual(result["unwindModel"], "FLATTEN")
        self.assertTrue(result["preserveNullAndEmptyArrays"])

    def test_to_dict_custom(self):
        # 测试自定义参数
        unwind_node = Unwind(
            id="test_id",
            name="Custom Unwind",
            path="$.items",
            index_name="index",
            mode="EXPAND",
            array_elem="FULL",
            joiner="-",
            keep_null=False
        )
        result = unwind_node.to_dict()
        self.assertEqual(result["name"], "Custom Unwind")
        self.assertEqual(result["path"], "$.items")
        self.assertEqual(result["includeArrayIndex"], "index")
        self.assertEqual(result["unwindModel"], "EXPAND")
        self.assertEqual(result["arrayModel"], "FULL")
        self.assertEqual(result["joiner"], "-")
        self.assertFalse(result["preserveNullAndEmptyArrays"])

    def test_to_instance(self):
        # 测试从字典创建实例
        node_dict = {
            "id": "test_id",
            "name": "Custom Unwind",
            "path": "$.items",
            "includeArrayIndex": "index",
            "unwindModel": "EXPAND",
            "arrayModel": "FULL",
            "joiner": "-",
            "preserveNullAndEmptyArrays": False
        }
        unwind_node = Unwind.to_instance(node_dict)
        self.assertEqual(unwind_node.id, "test_id")
        self.assertEqual(unwind_node.name, "Custom Unwind")
        self.assertEqual(unwind_node.path, "$.items")
        self.assertEqual(unwind_node.index_name, "index")
        self.assertEqual(unwind_node.mode, "EXPAND")
        self.assertEqual(unwind_node.array_elem, "FULL")
        self.assertEqual(unwind_node.joiner, "-")
        self.assertFalse(unwind_node.keep_null)

if __name__ == '__main__':
    unittest.main() 