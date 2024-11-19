import unittest
from tapflow.lib.data_pipeline.nodes.filter import Filter
from tapflow.lib.data_pipeline.base_node import FilterType

class TestFilter(unittest.TestCase):
    def test_initialization(self):
        filter_node = Filter(f="field1 > 10", filter_type=FilterType.keep, id="123", name="Test Filter")
        self.assertEqual(filter_node.id, "123")
        self.assertEqual(filter_node.name, "Test Filter")
        self.assertEqual(filter_node.f, {FilterType.keep: "field1 > 10"})

    def test_f_to_expression(self):
        filter_node = Filter(f="field1 > 10")
        expression = filter_node.f_to_expression()
        self.assertIn("record.field1", expression)

    def test_to_dict(self):
        filter_node = Filter(f="field1 > 10", filter_type=FilterType.keep, id="123", name="Test Filter")
        result = filter_node.to_dict()
        self.assertEqual(result["id"], "123")
        self.assertEqual(result["name"], "Test Filter")
        self.assertEqual(result["type"], "row_filter_processor")
        self.assertEqual(result["action"], "retain")
        self.assertIn("expression", result)

    def test_to_js_keep(self):
        filter_node = Filter(f="field1 > 10", filter_type=FilterType.keep)
        expected_js = '''
    if (record.field1 > 10) {
        return record;
    }
    return null;
        '''
        self.assertEqual(filter_node.to_js().strip(), expected_js.strip())

    def test_to_js_delete(self):
        filter_node = Filter(f="field1 > 10", filter_type=FilterType.delete)
        expected_js = '''
    if (record.field1 > 10) {
        return null;
    }
    return record;
        '''
        self.assertEqual(filter_node.to_js().strip(), expected_js.strip())

    def test_to_instance(self):
        node_dict = {
            "id": "123",
            "name": "Test Filter",
            "expression": "field1 > 10",
            "action": "retain"
        }
        filter_node = Filter.to_instance(node_dict)
        self.assertEqual(filter_node.id, "123")
        self.assertEqual(filter_node.name, "Test Filter")
        self.assertEqual(filter_node.f, {"keep": "field1 > 10"})

if __name__ == '__main__':
    unittest.main() 