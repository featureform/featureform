#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import ast
import inspect
import re
import textwrap
from typing import Callable

import astor


def add_variant_to_name(text, replacement):
    return re.sub(r"\{\{\s*(\w+)\s*\}\}", r"{{ \1." + replacement + r" }}", text)


def canonicalize_function_definition(fn: Callable) -> str:
    """
    Takes a function definition and returns a string with comments and extra whitespace removed,
    making it easier to compare the function definitions for equality.

    The function parses the input function using the `astor` module, removes comments and docstrings,
    and then converts the Abstract Syntax Tree (AST) back into source code.

    Example:
    canonicalize_function_definition('''
    def foo(bar: int) -> int:
        # This is a comment
        return bar
    ''')
    returns:
    'def foo(bar: int) -> int: return bar'

    Args:
        fn (Callable): The function whose definition will be canonicalized.

    Raises:
        TypeError: If `fn` is None or not a valid function.
        ValueError: If the function's source code can't be parsed.

    Returns:
        str: A string representing the cleaned function definition without comments or docstrings.
    """

    if fn is None:
        raise TypeError("Function is required")

    # get sources from the function
    text = inspect.getsource(fn)

    dedented_code = textwrap.dedent(text)

    function_without_decorator = _remove_decorator(dedented_code)

    try:
        tree = ast.parse(function_without_decorator)
    except (SyntaxError, ValueError) as e:
        raise ValueError(f"Error parsing source code: {e}") from e

    class NormalizeAst(ast.NodeTransformer):
        def remove_docstring(self, node):
            """Removes comments if present as the first element of a node body."""
            while (
                node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, (ast.Str, ast.Constant))
            ):
                node.body.pop(0)
            return node

        def visit_FunctionDef(self, node):
            # Remove function docstring
            self.remove_docstring(node)
            return self.generic_visit(node)

        def visit_ClassDef(self, node):
            # Remove class docstring
            self.remove_docstring(node)
            return self.generic_visit(node)

        def visit_Module(self, node):
            # Remove module docstring
            self.remove_docstring(node)
            return self.generic_visit(node)

    normalizer = NormalizeAst()
    normalized_tree = normalizer.visit(tree)
    ast.fix_missing_locations(normalized_tree)

    normalized_code = astor.to_source(normalized_tree).strip()

    return normalized_code


def _remove_decorator(fn_text: str):
    module_node = ast.parse(fn_text)
    func_def_node = module_node.body[0]
    func_def_node.decorator_list = []
    function_without_decorator = astor.to_source(func_def_node)
    return function_without_decorator
