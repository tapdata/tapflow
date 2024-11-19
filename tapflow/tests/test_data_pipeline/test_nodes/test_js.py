import unittest
from tapflow.lib.data_pipeline.nodes.js import Js

class TestJsNode(unittest.TestCase):
    def test_initialization(self):
        js_node = Js(script="console.log('Hello, World!');", declareScript="var x = 10;", func_header=True)
        self.assertEqual(js_node.script, "console.log('Hello, World!');")
        self.assertEqual(js_node.declareScript, "var x = 10;")
        self.assertTrue(js_node.func_header)
        self.assertEqual(js_node.language, "js")
        self.assertEqual(js_node.name, "JS")

    def test_to_js_with_func_header(self):
        js_node = Js(script="console.log('Hello, World!');", declareScript="", func_header=True)
        expected_script = "function process(record){\n\n\t// Enter you code at here\nconsole.log('Hello, World!');}"
        self.assertEqual(js_node.to_js(), expected_script)

    def test_to_js_without_func_header(self):
        js_node = Js(script="console.log('Hello, World!');", declareScript="", func_header=False)
        self.assertEqual(js_node.to_js(), "console.log('Hello, World!');")

    def test_to_dict(self):
        js_node = Js(script="console.log('Hello, World!');", declareScript="var x = 10;", id="123")
        result = js_node.to_dict()
        self.assertEqual(result["id"], "123")
        self.assertEqual(result["name"], "JS")
        self.assertEqual(result["type"], "js_processor")
        self.assertIn("script", result)
        self.assertIn("declareScript", result)

    def test_to_instance(self):
        node_dict = {
            "script": "console.log('Hello, World!');",
            "declareScript": "var x = 10;",
            "func_header": False,
            "language": "js",
            "id": "123",
            "name": "JS Node"
        }
        js_node = Js.to_instance(node_dict)
        self.assertEqual(js_node.script, "console.log('Hello, World!');")
        self.assertEqual(js_node.declareScript, "var x = 10;")
        self.assertFalse(js_node.func_header)
        self.assertEqual(js_node.language, "js")
        self.assertEqual(js_node.id, "123")
        self.assertEqual(js_node.name, "JS Node")

if __name__ == '__main__':
    unittest.main()
