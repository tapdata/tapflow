import unittest
from tapflow.lib.data_pipeline.nodes.field_add_del import FieldAddDel

class TestFieldAddDel(unittest.TestCase):
    def test_initialization(self):
        field_add_del = FieldAddDel(id="123", name="Test Field Add/Del")
        self.assertEqual(field_add_del.id, "123")
        self.assertEqual(field_add_del.name, "Test Field Add/Del")
        self.assertEqual(field_add_del.operations, [])

    def test_add_field(self):
        field_add_del = FieldAddDel()
        field_add_del.add_field("new_field", "string")
        self.assertEqual(len(field_add_del.operations), 1)
        self.assertEqual(field_add_del.operations[0]["field"], "new_field")
        self.assertEqual(field_add_del.operations[0]["op"], "CREATE")
        self.assertEqual(field_add_del.operations[0]["type"], "string")

    def test_delete_field(self):
        field_add_del = FieldAddDel()
        field_add_del.delete_field("old_field")
        self.assertEqual(len(field_add_del.operations), 1)
        self.assertEqual(field_add_del.operations[0]["field"], "old_field")
        self.assertEqual(field_add_del.operations[0]["op"], "REMOVE")

    def test_to_dict(self):
        field_add_del = FieldAddDel(id="123", name="Test Field Add/Del")
        field_add_del.add_field("new_field", "string")
        field_add_del.delete_field("old_field")
        result = field_add_del.to_dict()
        self.assertEqual(result["id"], "123")
        self.assertEqual(result["name"], "Test Field Add/Del")
        self.assertEqual(result["type"], "field_add_del_processor")
        self.assertEqual(len(result["operations"]), 2)

if __name__ == '__main__':
    unittest.main() 