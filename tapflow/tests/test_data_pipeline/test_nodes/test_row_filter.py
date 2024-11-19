import unittest
from tapflow.lib.data_pipeline.nodes.row_filter import RowFilter, RowFilterType

class TestRowFilter(unittest.TestCase):
    def test_initialization(self):
        row_filter = RowFilter(expression="field1 > 10", row_filter_type=RowFilterType.retain, id="123", name="Test Row Filter")
        self.assertEqual(row_filter.expression, "field1 > 10")
        self.assertEqual(row_filter.rowFilterType, RowFilterType.retain)
        self.assertEqual(row_filter.id, "123")
        self.assertEqual(row_filter.name, "Test Row Filter")

    def test_to_dict(self):
        row_filter = RowFilter(expression="field1 > 10", row_filter_type=RowFilterType.retain, id="123", name="Test Row Filter")
        result = row_filter.to_dict()
        self.assertEqual(result["id"], "123")
        self.assertEqual(result["name"], "Row Filter")
        self.assertEqual(result["type"], "row_filter_processor")
        self.assertEqual(result["action"], "retain")
        self.assertEqual(result["expression"], "field1 > 10")

    def test_to_instance(self):
        node_dict = {
            "id": "123",
            "name": "Test Row Filter",
            "expression": "field1 > 10",
            "action": "retain"
        }
        row_filter = RowFilter.to_instance(node_dict)
        self.assertEqual(row_filter.id, "123")
        self.assertEqual(row_filter.name, "Test Row Filter")
        self.assertEqual(row_filter.expression, "field1 > 10")
        self.assertEqual(row_filter.rowFilterType, RowFilterType.retain)

if __name__ == '__main__':
    unittest.main() 