import unittest
from tapflow.lib.data_pipeline.nodes.value_map import ValueMap

class TestValueMap(unittest.TestCase):
    def test_to_js(self):
        # 测试生成的JavaScript代码
        value_map = ValueMap(field="age", value=30)
        js_code = value_map.to_js()
        expected_js_code = '''
    record["age"] = 30;
        '''
        self.assertEqual(js_code.strip(), expected_js_code.strip())

    def test_to_js_with_string_value(self):
        # 测试生成的JavaScript代码，值为字符串
        value_map = ValueMap(field="name", value='"John"')
        js_code = value_map.to_js()
        expected_js_code = '''
    record["name"] = "John";
        '''
        self.assertEqual(js_code.strip(), expected_js_code.strip())

if __name__ == '__main__':
    unittest.main() 