import unittest
from tapflow.lib.data_pipeline.nodes.field_calculate import FieldCalculate

class TestFieldCalculate(unittest.TestCase):
    def test_initialization(self):
        field_calculate = FieldCalculate(id="123", name="Test Field Calculate", table_name="test_table")
        self.assertEqual(field_calculate.id, "123")
        self.assertEqual(field_calculate.name, "Test Field Calculate")
        self.assertEqual(field_calculate.table_name, "test_table")
        self.assertEqual(field_calculate.script, {})

    def test_update(self):
        field_calculate = FieldCalculate(id="123", name="Test Field Calculate", table_name="test_table")
        field_calculate.update("field1", "return record['field1'] * 2;")
        self.assertIn("field1", field_calculate.script)
        self.assertEqual(field_calculate.script["field1"]["script"], "return record['field1'] * 2;")

    def test_update_without_table_name(self):
        field_calculate = FieldCalculate(id="123", name="Test Field Calculate")
        with self.assertRaises(ValueError):
            field_calculate.update("field1", "return record['field1'] * 2;")

    def test_delete(self):
        field_calculate = FieldCalculate(id="123", name="Test Field Calculate", table_name="test_table")
        field_calculate.update("field1", "return record['field1'] * 2;")
        field_calculate.delete("field1")
        self.assertNotIn("field1", field_calculate.script)

    def test_to_dict(self):
        field_calculate = FieldCalculate(id="123", name="Test Field Calculate", table_name="test_table")
        field_calculate.update("field1", "return record['field1'] * 2;")
        result = field_calculate.to_dict()
        self.assertEqual(result["id"], "123")
        self.assertEqual(result["name"], "Test Field Calculate")
        self.assertEqual(result["type"], "field_calc_processor")
        self.assertEqual(len(result["scripts"]), 1)

    def test_to_instance(self):
        node_dict = {
            "id": "123",
            "name": "Test Field Calculate",
            "scripts": [
                {
                    "field": "field1",
                    "script": "return record['field1'] * 2;",
                    "tableName": "test_table"
                }
            ]
        }
        field_calculate = FieldCalculate.to_instance(node_dict)
        self.assertEqual(field_calculate.id, "123")
        self.assertEqual(field_calculate.name, "Test Field Calculate")
        self.assertEqual(field_calculate.table_name, "test_table")
        self.assertIn("field1", field_calculate.script)

if __name__ == '__main__':
    unittest.main() 