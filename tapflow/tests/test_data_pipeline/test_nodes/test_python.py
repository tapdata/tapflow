import unittest
from tapflow.lib.data_pipeline.nodes.python import Python

class TestPythonNode(unittest.TestCase):
    def test_initialization(self):
        python_node = Python(script="print('Hello, World!')", declareScript="import os", language="py", id="123", name="Python Node")
        self.assertEqual(python_node.script, "print('Hello, World!')")
        self.assertEqual(python_node.declareScript, "import os")
        self.assertEqual(python_node.language, "py")
        self.assertEqual(python_node.id, "123")
        self.assertEqual(python_node.name, "Python Node")

    def test_to_python_with_func_header(self):
        python_node = Python(script="print('Hello, World!')", declareScript="", language="py")
        python_node.func_header = True
        expected_script = "import json, random, time, datetime, uuid, types, yaml\nimport urllib, urllib2, requests\nimport math, hashlib, base64\ndef process(record, context):print('Hello, World!')"
        self.assertEqual(python_node.to_python(), expected_script)

    def test_to_python_without_func_header(self):
        python_node = Python(script="print('Hello, World!')", declareScript="", language="py")
        python_node.func_header = False
        self.assertEqual(python_node.to_python(), "print('Hello, World!')")

    def test_to_dict(self):
        python_node = Python(script="print('Hello, World!')", declareScript="import os", id="123")
        result = python_node.to_dict()
        self.assertEqual(result["id"], "123")
        self.assertEqual(result["name"], "Python")
        self.assertEqual(result["type"], "python_processor")
        self.assertIn("script", result)
        self.assertIn("declareScript", result)

    def test_to_instance(self):
        node_dict = {
            "script": "print('Hello, World!')",
            "declareScript": "import os",
            "language": "py",
            "id": "123",
            "name": "Python Node"
        }
        python_node = Python.to_instance(node_dict)
        self.assertEqual(python_node.script, "print('Hello, World!')")
        self.assertEqual(python_node.declareScript, "import os")
        self.assertEqual(python_node.language, "py")
        self.assertEqual(python_node.id, "123")
        self.assertEqual(python_node.name, "Python Node")

if __name__ == '__main__':
    unittest.main() 