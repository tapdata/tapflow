import unittest
from tapflow.lib.utils.boolean_parser import BooleanParser

class TestBooleanParser(unittest.TestCase):
    def test_simple_variable(self):
        """测试单个变量的解析"""
        parser = BooleanParser("variable1")
        ast = parser.parse()
        
        self.assertEqual(ast, "variable1")
        self.assertEqual(parser.variables, {"variable1"})
        
    def test_not_operation(self):
        """测试NOT操作符"""
        parser = BooleanParser("!variable1")
        ast = parser.parse()
        
        self.assertEqual(ast, ("NOT", "variable1"))
        self.assertEqual(parser.variables, {"variable1"})
        
    def test_logical_operations(self):
        """测试逻辑操作符 AND 和 OR"""
        test_cases = [
            ("var1 && var2", ("AND", "var1", "var2")),
            ("var1 || var2", ("OR", "var1", "var2")),
            ("var1 && var2 && var3", ("AND", ("AND", "var1", "var2"), "var3")),
            ("var1 || var2 || var3", ("OR", ("OR", "var1", "var2"), "var3")),
        ]
        
        for expression, expected_ast in test_cases:
            with self.subTest(expression=expression):
                parser = BooleanParser(expression)
                ast = parser.parse()
                self.assertEqual(ast, expected_ast)
        
    def test_complex_expression(self):
        """测试复杂表达式，包含括号和多个操作符"""
        expression = "(var1 && var2) || (!var3)"
        parser = BooleanParser(expression)
        ast = parser.parse()
        
        expected_ast = ("OR", 
                       ("AND", "var1", "var2"),
                       ("NOT", "var3"))
        self.assertEqual(ast, expected_ast)
        self.assertEqual(parser.variables, {"var1", "var2", "var3"})
        
    def test_evaluate(self):
        """测试表达式求值"""
        test_cases = [
            ("var1", {"var1": True}, True),
            ("var1", {"var1": False}, False),
            ("!var1", {"var1": True}, False),
            ("var1 && var2", {"var1": True, "var2": True}, True),
            ("var1 && var2", {"var1": True, "var2": False}, False),
            ("var1 || var2", {"var1": False, "var2": True}, True),
            ("(var1 && var2) || var3", 
             {"var1": True, "var2": True, "var3": False}, True),
        ]
        
        for expression, values, expected in test_cases:
            with self.subTest(expression=expression):
                parser = BooleanParser(expression)
                ast = parser.parse()
                result = parser.evaluate(ast, values)
                self.assertEqual(result, expected)
        
    def test_complex_variable_names(self):
        """测试包含特殊字符的变量名"""
        expression = "flow 1.end && flow 2.cdc.start"
        parser = BooleanParser(expression)
        ast = parser.parse()
        
        self.assertEqual(
            parser.variables, 
            {"flow 1.end", "flow 2.cdc.start"}
        )
        
    def test_missing_variable_value(self):
        """测试缺失变量值的情况"""
        parser = BooleanParser("var1 && var2")
        ast = parser.parse()
        result = parser.evaluate(ast, {"var1": True})
        
        self.assertFalse(result)  # 缺失的变量默认为 False
        
    def test_whitespace_handling(self):
        """测试空白字符处理"""
        parser1 = BooleanParser("var1&&var2")
        parser2 = BooleanParser("var1 && var2")
        
        ast1 = parser1.parse()
        ast2 = parser2.parse()
        
        self.assertEqual(ast1, ast2)
        
    def test_invalid_expressions(self):
        """测试无效表达式"""
        invalid_expressions = [
            "var1 && ",  # 不完整的表达式
            "|| var1",   # 开始就是操作符
            "(var1",     # 未闭合的括号
        ]
        
        for expression in invalid_expressions:
            with self.subTest(expression=expression):
                with self.assertRaises(Exception):
                    parser = BooleanParser(expression)
                    parser.parse()


if __name__ == '__main__':
    unittest.main()