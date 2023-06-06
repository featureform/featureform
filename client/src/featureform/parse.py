import re


def add_variant_to_name(text, replacement):
    return re.sub(r"\{\{\s*(\w+)\s*\}\}", r"{{ \1." + replacement + r" }}", text)
