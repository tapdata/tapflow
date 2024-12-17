"""
布尔表达式解析器
@author: Jerry
@date: 2024-12-16
"""

import re

class BooleanParserError(Exception):
    """布尔解析器异常"""
    message = "Boolean expression parsing error"

class BooleanParser:
    def __init__(self, expression):
        self.expression = expression
        self.tokens = []
        self.pos = 0
        self.variables = set()  # 用来存储表达式中的变量
        self.tokenize()

    def tokenize(self):
        # 修改正则表达式以支持!运算符，并优化空格处理
        pattern = r'\(|\)|&&|\|\||!|[a-zA-Z0-9][a-zA-Z0-9\s\.]*[a-zA-Z0-9]|[a-zA-Z0-9]'
        tokens = []
        for token in re.findall(pattern, self.expression):
            cleaned_token = token.strip()
            if cleaned_token:  # 只添加非空token
                tokens.append(cleaned_token)
        
        # 验证表达式的有效性
        if not tokens:
            raise BooleanParserError("Empty expression")
            
        # 验证第一个token不是操作符
        if tokens[0] in {'&&', '||'}:
            raise BooleanParserError("Expression cannot start with && or ||")
            
        # 验证最后一个token不是操作符
        if tokens[-1] in {'&&', '||'}:
            raise BooleanParserError("Expression cannot end with && or ||")
            
        # 验证括号匹配
        bracket_count = 0
        for token in tokens:
            if token == '(':
                bracket_count += 1
            elif token == ')':
                bracket_count -= 1
            if bracket_count < 0:
                raise BooleanParserError("Unmatched closing bracket")
        if bracket_count > 0:
            raise BooleanParserError("Unclosed bracket")
            
        self.tokens = tokens
        
        # 提取变量时，排除运算符
        operators = {'(', ')', '&&', '||', '!'}
        self.variables = {token for token in self.tokens if token not in operators}

    def parse(self):
        if not self.tokens:
            raise BooleanParserError("No tokens to parse")
        try:
            result = self.parse_or()
            if self.pos < len(self.tokens):
                raise BooleanParserError("Unexpected tokens after parsing")
            return result
        except IndexError:
            raise BooleanParserError("Unexpected end of expression")

    def parse_or(self):
        # 处理 OR 操作 (||)
        left = self.parse_and()
        while self.pos < len(self.tokens) and self.tokens[self.pos] == '||':
            self.pos += 1  # consume the '||'
            right = self.parse_and()
            left = ('OR', left, right)
        return left
    
    def parse_and(self):
        # 处理 AND 操作 (&&)
        left = self.parse_factor()
        while self.pos < len(self.tokens) and self.tokens[self.pos] == '&&':
            self.pos += 1  # consume the '&&'
            right = self.parse_factor()
            left = ('AND', left, right)
        return left

    def parse_factor(self):
        # 处理括号、NOT运算符和基本变量
        token = self.tokens[self.pos]
        if token == '!':
            self.pos += 1  # 消费!符号
            expr = self.parse_factor()  # 递归解析!后面的表达式
            return ('NOT', expr)
        elif token == '(':
            self.pos += 1
            expr = self.parse_or()
            if self.tokens[self.pos] == ')':
                self.pos += 1  # consume the ')'
            return expr
        else:
            self.pos += 1  # consume the variable
            return token

    def evaluate(self, ast, variable_values):
        """ 根据给定的变量值求解AST的布尔值 """
        if isinstance(ast, str):
            # 如果是变量名，则返回它的布尔值
            return variable_values.get(ast, False)
        
        if isinstance(ast, tuple):
            if ast[0] == 'NOT':
                return not self.evaluate(ast[1], variable_values)
            elif ast[0] == 'AND':
                return self.evaluate(ast[1], variable_values) and self.evaluate(ast[2], variable_values)
            elif ast[0] == 'OR':
                return self.evaluate(ast[1], variable_values) or self.evaluate(ast[2], variable_values)


if __name__ == "__main__":
    # 示例
    expression = "flow 1.end && flow 2.cdc.start"
    parser = BooleanParser(expression)
    ast = parser.parse()

    # 获取表达式中所有的变量
    variables = parser.variables
    print("表达式中的变量:", variables)

    # 定义变量值
    variable_values = {
        'flow 1.end': True,
        'flow 2.cdc.start': False,
    }

    # 计算AST的布尔值
    result = parser.evaluate(ast, variable_values)
    print("表达式的结果是:", result)
