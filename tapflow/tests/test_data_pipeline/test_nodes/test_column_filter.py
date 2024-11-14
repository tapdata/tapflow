import unittest
from tapflow.lib.data_pipeline.nodes.column_filter import ColumnFilter
from tapflow.lib.data_pipeline.base_node import FilterType

class TestColumnFilter(unittest.TestCase):
    def test_initialization(self):
        column_filter = ColumnFilter(f=["field1", "field2"], filter_type=FilterType.keep, id="123", name="Test Filter")
        self.assertEqual(column_filter.id, "123")
        self.assertEqual(column_filter.name, "Test Filter")
        self.assertEqual(column_filter.f, {FilterType.keep: ["field1", "field2"]})

    def test_to_js_keep(self):
        column_filter = ColumnFilter(f=["field1", "field2"], filter_type=FilterType.keep)
        expected_js = '''
        keepFields = ['field1', 'field2'];
        newRecord = {};
        for (i in keepFields) {
            newRecord[keepFields[i]] = record[keepFields[i]];
        }
        return newRecord;
        '''
        self.assertEqual(column_filter._to_js().strip(), expected_js.strip())

    def test_to_js_delete(self):
        column_filter = ColumnFilter(f=["field1", "field2"], filter_type=FilterType.delete)
        expected_js = '''
    deleteFields = ['field1', 'field2'];
    newRecord = record;
    for (i in deleteFields) {
        delete(newRecord[deleteFields[i]]);
    }
    return newRecord;
    '''
        self.assertEqual(column_filter._to_js().strip(), expected_js.strip())

    def test_to_dict(self):
        column_filter = ColumnFilter(f=["field1", "field2"], filter_type=FilterType.keep, id="123", name="Test Filter")
        result = column_filter.to_dict()
        self.assertEqual(result["id"], "123")
        self.assertEqual(result["name"], "Test Filter")
        self.assertEqual(result["type"], "js_processor")
        self.assertIn("script", result)
        self.assertIn("declareScript", result)

if __name__ == '__main__':
    unittest.main() 