import unittest
from tapflow.lib.data_pipeline.nodes.field_rename import FieldRename

class TestFieldRename(unittest.TestCase):
    def test_initialization(self):
        config = {"old_name": "new_name"}
        field_rename = FieldRename(config=config, id="123", name="Test Field Rename")
        self.assertEqual(field_rename.id, "123")
        self.assertEqual(field_rename.name, "Test Field Rename")
        self.assertEqual(field_rename.config, config)

    def test_to_dict(self):
        config = {"old_name": "new_name"}
        field_rename = FieldRename(config=config, id="123", name="Test Field Rename")
        result = field_rename.to_dict()
        self.assertEqual(result["id"], "123")
        self.assertEqual(result["name"], "Test Field Rename")
        self.assertEqual(result["type"], "field_rename_processor")
        self.assertEqual(len(result["operations"]), 1)
        self.assertEqual(result["operations"][0]["field"], "old_name")
        self.assertEqual(result["operations"][0]["operand"], "new_name")

    def test_to_instance(self):
        node_dict = {
            "id": "123",
            "name": "Test Field Rename",
            "operations": [
                {
                    "field": "old_name",
                    "op": "RENAME",
                    "operand": "new_name"
                }
            ]
        }
        field_rename = FieldRename.to_instance(node_dict)
        self.assertEqual(field_rename.id, "123")
        self.assertEqual(field_rename.name, "Test Field Rename")
        self.assertEqual(field_rename.config, {"old_name": "new_name"})

if __name__ == '__main__':
    unittest.main() 