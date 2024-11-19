import unittest
from tapflow.lib.data_pipeline.nodes.rename import Rename

class TestRenameNode(unittest.TestCase):
    def test_initialization(self):
        rename_node = Rename(ori="old_field", new="new_field")
        self.assertEqual(rename_node.ori, "old_field")
        self.assertEqual(rename_node.new, "new_field")

    def test_to_js(self):
        rename_node = Rename(ori="old_field", new="new_field")
        expected_js = '''
    record["new_field"] = record["old_field"];
    delete(record["old_field"]);
    return record;
        '''
        self.assertEqual(rename_node._to_js().strip(), expected_js.strip())

if __name__ == '__main__':
    unittest.main() 