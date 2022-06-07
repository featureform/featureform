import sqlite3

conn = sqlite3.connect('test.db')

class SQLiteTest:

    def __init__(self):
        pass

    def getType(self, type):
        query = "SELECT * FROM " + type + "FOR JSON"
        type_data = conn.execute(query)
        return type_data

    def getResource(self, type, resource):
        type_table_query = "SELECT * FROM " + type + "WHERE name=" + resource + " FOR JSON"
        type_data = conn.execute(type_table_query)

        # Assuming that owner is the secondary key in the variant table, aka the owner corresponds to the "name" in the type (feature) table
        variant_table_query = "SELECT * FROM " + type + "_variant AS tv, " + type + " as t WHERE t.name=" + resource + ", t.name=tv.owner, FOR JSON"
        variant_data = conn.execute(variant_table_query)
        return type_data, variant_data
