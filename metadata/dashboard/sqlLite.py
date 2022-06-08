from ast import Pass
import sqlite3
from sqlite3 import Error

conn = sqlite3.connect('test.db')

class SQLiteTest:

    def __init__(self):
        self.createTables()

    def createTables(self):
        # USE KSSHIRAJA'S VERSION
        # Features table
        Pass

# All 3 functions return a cursor, USE THIS
    def getTypeTable(self, type):
        query = "SELECT * FROM " + type
        type_data = conn.execute(query)
        return type_data

    def getTypeForResource(self, type, resource):
        type_table_query = "SELECT * FROM " + type + "WHERE name=" + resource
        type_data = conn.execute(type_table_query)
        return type_data

    def getResource(self, type, resource):
        variant_table_query = "SELECT * FROM "+type+"_variant WHERE name="+resource
        variant_data = conn.execute(variant_table_query)
        return variant_data
