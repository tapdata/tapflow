import unittest
from tapflow.lib.data_pipeline.nodes.merge import Merge

class TestMergeNode(unittest.TestCase):
    def test_initialization(self):
        merge_node = Merge(
            node_id="1",
            table_name="table",
            association=[("source_field", "target_field")],
            mergeType="updateOrInsert",
            targetPath="",
            isArray=False,
            arrayKeys=[],
            join_value_change=False,
            id="123"
        )
        self.assertEqual(merge_node.node_id, "1")
        self.assertEqual(merge_node.table_name, "table")
        self.assertEqual(merge_node.association, [("source_field", "target_field")])
        self.assertEqual(merge_node.mergeType, "updateOrInsert")
        self.assertEqual(merge_node.targetPath, "")
        self.assertFalse(merge_node.isArray)
        self.assertEqual(merge_node.arrayKeys, [])
        self.assertFalse(merge_node.join_value_change)
        self.assertEqual(merge_node.id, "123")

    def test_to_dict_as_head(self):
        merge_node = Merge(
            node_id="1",
            table_name="table",
            association=[("source_field", "target_field")],
            mergeType="updateOrInsert",
            targetPath="",
            isArray=False,
            arrayKeys=[],
            join_value_change=False,
            id="123"
        )
        merge_node.father = None
        result = merge_node.to_dict(is_head=True)
        self.assertEqual(result["id"], "123")
        self.assertEqual(result["mergeProperties"][0]["tableName"], "table")
        self.assertEqual(result["mergeProperties"][0]["id"], "1")
        self.assertEqual(result["mergeProperties"][0]["mergeType"], "updateOrInsert")
        self.assertFalse(result["mergeProperties"][0]["isArray"])
        self.assertEqual(result["mergeProperties"][0]["children"], [])

    def test_to_dict_as_child(self):
        merge_node = Merge(
            node_id="1",
            table_name="table",
            association=[("source_field", "target_field")],
            mergeType="updateOrInsert",
            targetPath="",
            isArray=False,
            arrayKeys=[],
            join_value_change=False,
            id="123"
        )
        merge_node.father = Merge(node_id="0", table_name="parent_table", association=[])
        result = merge_node.to_dict(is_head=False)
        self.assertEqual(result["id"], "1")
        self.assertEqual(result["tableName"], "table")
        self.assertEqual(result["joinKeys"], [{"source": "source_field", "target": "target_field"}])
        self.assertEqual(result["mergeType"], "updateOrInsert")
        self.assertEqual(result["targetPath"], "")
        self.assertFalse(result["isArray"])
        self.assertEqual(result["arrayKeys"], [])
        self.assertFalse(result["enableUpdateJoinKeyValue"])
        self.assertEqual(result["children"], [])

    def test_to_instance(self):
        node_dict = {
            "id": "123",
            "mergeProperties": [{
                "id": "1",
                "tableName": "table",
                "mergeType": "updateOrInsert",
                "isArray": False,
                "arrayKeys": [],
                "enableUpdateJoinKeyValue": False
            }]
        }
        merge_node = Merge.to_instance(node_dict)
        self.assertEqual(merge_node.node_id, "1")
        self.assertEqual(merge_node.table_name, "table")
        self.assertEqual(merge_node.mergeType, "updateOrInsert")
        self.assertFalse(merge_node.isArray)
        self.assertEqual(merge_node.arrayKeys, [])
        self.assertFalse(merge_node.join_value_change)

    def test_add_child_node(self):
        parent_node = Merge(
            node_id="1",
            table_name="parent_table",
            association=[],
            mergeType="updateOrInsert",
            targetPath="",
            isArray=False,
            arrayKeys=[],
            join_value_change=False,
            id="123"
        )
        child_node = Merge(
            node_id="2",
            table_name="child_table",
            association=[],
            mergeType="updateOrInsert",
            targetPath="",
            isArray=False,
            arrayKeys=[],
            join_value_change=False,
            id="456"
        )
        parent_node.add(child_node)
        self.assertIn(child_node, parent_node.child)
        self.assertEqual(child_node.father, parent_node)

    def test_update_node(self):
        original_node = Merge(
            node_id="1",
            table_name="table",
            association=[("source_field", "target_field")],
            mergeType="updateOrInsert",
            targetPath="",
            isArray=False,
            arrayKeys=[],
            join_value_change=False,
            id="123"
        )
        updated_node = Merge(
            node_id="1",
            table_name="table",
            association=[("new_source_field", "new_target_field")],
            mergeType="updateWrite",
            targetPath="new_path",
            isArray=True,
            arrayKeys=["key1"],
            join_value_change=True,
            id="123"
        )
        original_node.update(updated_node)
        self.assertEqual(original_node.mergeType, "updateWrite")
        self.assertEqual(original_node.targetPath, "new_path")
        self.assertEqual(original_node.association, [("new_source_field", "new_target_field")])
        self.assertTrue(original_node.isArray)
        self.assertEqual(original_node.arrayKeys, ["key1"])
        self.assertTrue(original_node.join_value_change)

    def test_find_by_node_id(self):
        parent_node = Merge(
            node_id="1",
            table_name="parent_table",
            association=[],
            mergeType="updateOrInsert",
            targetPath="",
            isArray=False,
            arrayKeys=[],
            join_value_change=False,
            id="123"
        )
        child_node = Merge(
            node_id="2",
            table_name="child_table",
            association=[],
            mergeType="updateOrInsert",
            targetPath="",
            isArray=False,
            arrayKeys=[],
            join_value_change=False,
            id="456"
        )
        parent_node.add(child_node)
        found_node = parent_node.find_by_node_id("2")
        self.assertIsNotNone(found_node)
        self.assertEqual(found_node.node_id, "2")

        not_found_node = parent_node.find_by_node_id("3")
        self.assertIsNone(not_found_node)

if __name__ == '__main__':
    unittest.main() 