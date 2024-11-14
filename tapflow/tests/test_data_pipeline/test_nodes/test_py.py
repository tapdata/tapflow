import unittest
from tapflow.lib.data_pipeline.nodes.py import Py

class TestPyNode(unittest.TestCase):
    def test_initialization(self):
        py_node = Py(script="print('Hello, World!')", declareScript="import os")
        self.assertEqual(py_node.script, "print('Hello, World!')")
        self.assertEqual(py_node.declareScript, "import os")

    def test_to_dict(self):
        py_node = Py(script="print('Hello, World!')", declareScript="import os")
        result = py_node.to_dict()
        self.assertIn("script", result)
        self.assertIn("declareScript", result)
        self.assertEqual(result["type"], "python_processor")
        self.assertEqual(result["script"], py_node.python_header + "print('Hello, World!')")
        self.assertEqual(result["declareScript"], "import os")

if __name__ == '__main__':
    unittest.main() 