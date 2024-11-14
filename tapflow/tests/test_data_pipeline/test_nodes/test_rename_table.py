import unittest
from tapflow.lib.data_pipeline.nodes.rename_table import RenameTable

class TestRenameTable(unittest.TestCase):
    def test_initialization(self):
        rename_table = RenameTable(prefix="pre_", suffix="_suf", tables=["table1", "table2"], config=[("table1", "new_table1")])
        self.assertEqual(rename_table.prefix, "pre_")
        self.assertEqual(rename_table.suffix, "_suf")
        self.assertEqual(rename_table.tables, ["table1", "table2"])
        self.assertEqual(rename_table.config, [("table1", "new_table1")])

    def test_to_dict(self):
        rename_table = RenameTable(prefix="pre_", suffix="_suf", tables=["table1", "table2"], config=[("table1", "new_table1")])
        result = rename_table.to_dict()
        self.assertEqual(result["name"], "TableRename")
        self.assertEqual(result["type"], "table_rename_processor")
        self.assertEqual(result["prefix"], "pre_")
        self.assertEqual(result["suffix"], "_suf")
        self.assertEqual(len(result["tableNames"]), 2)
        self.assertEqual(result["tableNames"][0]["originTableName"], "table1")
        self.assertEqual(result["tableNames"][0]["currentTableName"], "new_table1")
        self.assertEqual(result["tableNames"][1]["originTableName"], "table2")
        self.assertEqual(result["tableNames"][1]["currentTableName"], "pre_table2_suf")

if __name__ == '__main__':
    unittest.main() 