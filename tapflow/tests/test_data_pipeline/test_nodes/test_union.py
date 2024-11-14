import unittest
from tapflow.lib.data_pipeline.nodes.union import UnionNode

class TestUnionNode(unittest.TestCase):
    def test_to_dict_default(self):
        # 测试默认参数
        union_node = UnionNode(id="test_id")
        result = union_node.to_dict()
        self.assertEqual(result["id"], "test_id")
        self.assertEqual(result["name"], "Union")
        self.assertEqual(result["type"], "union_processor")
        self.assertEqual(result["concurrentNum"], 2)
        self.assertFalse(result["enableConcurrentProcess"])

    def test_to_dict_custom(self):
        # 测试自定义参数
        union_node = UnionNode(id="test_id", name="Custom Union", concurrentNum=4, enableConcurrentProcess=True)
        result = union_node.to_dict()
        self.assertEqual(result["name"], "Custom Union")
        self.assertEqual(result["concurrentNum"], 4)
        self.assertTrue(result["enableConcurrentProcess"])

    def test_to_instance(self):
        # 测试从字典创建实例
        node_dict = {
            "id": "test_id",
            "name": "Custom Union",
            "concurrentNum": 4,
            "enableConcurrentProcess": True
        }
        union_node = UnionNode.to_instance(node_dict)
        self.assertEqual(union_node.id, "test_id")
        self.assertEqual(union_node.name, "Custom Union")
        self.assertEqual(union_node.concurrentNum, 4)
        self.assertTrue(union_node.enableConcurrentProcess)

if __name__ == '__main__':
    unittest.main() 