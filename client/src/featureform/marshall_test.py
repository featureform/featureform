import marshal, types


def my_function(x):
    print(x, " there")

print("NORMAL")
print(marshal.dumps(my_function.__code__))

print()
print("ENCODE")
encoded = str(marshal.dumps(my_function.__code__), 'latin-1')
print(encoded)

decoded = bytearray(encoded, 'latin-1')
print(decoded)
code = marshal.loads(decoded)
func = types.FunctionType(code, globals(), "some_func_name")
func("hi")