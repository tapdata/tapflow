import unittest
from unittest.mock import patch
from tapflow.lib.data_pipeline.nodes.type_modification import TypeAdjust
from tapflow.tests.test_data_pipeline.test_nodes.mock_utils import mock_get_responses, mock_get_responses_for_operations

class TestTypeAdjust(unittest.TestCase):
    @patch('tapflow.lib.data_pipeline.nodes.type_modification.req.get')
    def test_get_method(self, mock_get):
        mock_get.side_effect = mock_get_responses()

        type_adjust = TypeAdjust()
        result = type_adjust.get(pre_connection_id="some_id", pre_table_name="ReplicationTableEditor")
        
        self.assertTrue(result)
        self.assertIn("_id", type_adjust.fields)
        self.assertIn("SETTLED_DATE", type_adjust.fields)
        self.assertTrue(type_adjust.fields["_id"]["primaryKey"])
        self.assertFalse(type_adjust.fields["SETTLED_DATE"]["primaryKey"])

    @patch('tapflow.lib.data_pipeline.nodes.type_modification.req.get')
    def test_to_dict_with_operations(self, mock_get):
        mock_get.side_effect = mock_get_responses_for_operations()

        type_adjust = TypeAdjust(id="test_id", name="Test Type Adjust")
        type_adjust.get(pre_connection_id="some_id", pre_table_name="ReplicationTableEditor")
        type_adjust.convert("field1", "string")
        type_adjust.convert("field2", "int")
        result = type_adjust.to_dict()
        
        self.assertEqual(result["id"], "test_id")
        self.assertEqual(result["name"], "Test Type Adjust")
        self.assertEqual(result["type"], "field_mod_type_processor")
        self.assertIn("operations", result)
        self.assertEqual(len(result["operations"]), 2)

        expected_operations = [
            {
                "field": "field1",
                "field_name": "field1",
                "id": "some_id_ReplicationTableEditor_field1",
                "label": "field1",
                "op": "CONVERT",
                "operand": "string",
                "originalDataType": "bigint(20) unsigned",
                "primary_key_position": 0,
                "table_name": "table",
                "type": "string"
            },
            {
                "field": "field2",
                "field_name": "field2",
                "id": "some_id_ReplicationTableEditor_field2",
                "label": "field2",
                "op": "CONVERT",
                "operand": "int",
                "originalDataType": "bigint(20) unsigned",
                "primary_key_position": 1,
                "table_name": "table",
                "type": "int"
            }
        ]

        for op, expected_op in zip(result["operations"], expected_operations):
            self.assertDictEqual(op, expected_op)

    def test_to_instance(self):
        node_dict = {
            "id": "test_id",
            "name": "Test Type Adjust",
            "operations": [
                {"field": "field1", "operand": "string", "id": "conn_table_field1"},
                {"field": "field2", "operand": "int", "id": "conn_table_field2"}
            ]
        }
        type_adjust = TypeAdjust.to_instance(node_dict)
        self.assertEqual(type_adjust.id, "test_id")
        self.assertEqual(type_adjust.name, "Test Type Adjust")
        self.assertEqual(len(type_adjust._convert_field), 2)

if __name__ == '__main__':
    unittest.main() 