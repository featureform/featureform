import sqlite3

conn = sqlite3.connect('test.db')

class SQLiteTest:

    def __init__(self):
        self.createTables()

    def createTables(self):

        # Features table
        conn.execute('''CREATE TABLE features
         (NAME VARCHAR(20) PRIMARY KEY     NOT NULL,
         DEFAULT_VARIANT           VARCHAR(20),
         TYPE            VARCHAR(20)     NOT NULL,
         VARIANTS        VARCHAR(20),
         FOREIGNKEY(NAME) REFERENCES features_variant(NAME),
         ALLVARIANTS VARCHAR(20) []);''')

         # Features variant table
        conn.execute('''CREATE TABLE features_variant
         (NAME VARCHAR(20) PRIMARY KEY     NOT NULL,
         VARIANT           VARCHAR(20),
         STATUS           VARCHAR(20),
         DATATYPE           VARCHAR(20),
         PROVIDER           VARCHAR(20),
         OWNER           VARCHAR(20),
         ENTITY           VARCHAR(20),
         DESCRIPTION           VARCHAR(20),
         CREATED            TIMESTAMP
         LOCATION            VARCHAR(20) [] [],
         SOURCE        VARCHAR(20),
         TRAININGSETS VARCHAR(20) [][]
         FOREIGNKEY(NAME) REFERENCES training-set_variant(NAME));''')

         # Training-set table
        conn.execute('''CREATE TABLE training-set
         (NAME VARCHAR(20) PRIMARY KEY     NOT NULL,
         DEFAULT_VARIANT           VARCHAR(20),
         TYPE            VARCHAR(20)     NOT NULL,
         VARIANTS        VARCHAR(20),
         FOREIGNKEY(NAME) REFERENCES training-set_variant(NAME),
         ALLVARIANTS VARCHAR(20) []);''')

        # training set variant
        conn.execute('''CREATE TABLE features_variant
         (NAME VARCHAR(20) PRIMARY KEY     NOT NULL,
         STATUS           VARCHAR(20),
         PROVIDER           VARCHAR(20),
         OWNER           VARCHAR(20),
         LABEL            VARCHAR(20),
         DESCRIPTION           VARCHAR(20),
         CREATED            TIMESTAMP
         SOURCE        VARCHAR(20),
         VARIANTS VARCHAR(20) [][]
         FOREIGNKEY(NAME) REFERENCES features_variant(NAME));''')

          # sources table
        conn.execute('''CREATE TABLE sources
         (NAME VARCHAR(20) PRIMARY KEY     NOT NULL,
         DEFAULT_VARIANT           VARCHAR(20),
         TYPE            VARCHAR(20)     NOT NULL,
         VARIANTS        VARCHAR(20),
         FOREIGNKEY(NAME) REFERENCES sources_variant(NAME),
         ALLVARIANTS VARCHAR(20) []);''')

        # source variant
        conn.execute('''CREATE TABLE sources_variant
         (NAME VARCHAR(20) PRIMARY KEY     NOT NULL,
         STATUS           VARCHAR(20),
         PROVIDER           VARCHAR(20),
         OWNER           VARCHAR(20),
         LABEL            VARCHAR(20),
         DESCRIPTION           TEXT,
         CREATED            TIMESTAMP
         SOURCETYPE        VARCHAR(20),
         VARIANT        VARCHAR(20),
         DEFINITION     VARCHAR(20),
         TRAININGSETS VARCHAR(20) [][],
         FOREIGNKEY(NAME) REFERENCES training-set_variant(NAME),
         LABELS VARCHAR(20) [][],
         FOREIGNKEY(NAME) REFERENCES labels-set_variant(NAME),
         FEATURES VARCHAR(20) [][],
         FOREIGNKEY(NAME) REFERENCES features_variant(NAME));''')


           # labels table
        conn.execute('''CREATE TABLE labels
         (NAME VARCHAR(20) PRIMARY KEY     NOT NULL,
         DEFAULT_VARIANT           VARCHAR(20),
         TYPE            VARCHAR(20)     NOT NULL,
         VARIANTS        VARCHAR(20),
         FOREIGNKEY(NAME) REFERENCES labels_variant(NAME),
         ALLVARIANTS VARCHAR(20) []);''')



    def getType(self, type):
        query = "SELECT * FROM " + type + "FOR JSON"
        type_data = conn.execute(query)
        return type_data

    def getResource(self, type, resource):
        type_table_query = "SELECT * FROM " + type + "WHERE name=" + resource + " FOR JSON"
        type_data = conn.execute(type_table_query)

        # Assuming that owner is the secondary key in the variant table, aka the owner corresponds to the "name" in the type (feature) table
        variant_table_query = "SELECT * FROM " + type + "_variant AS tv, " + type + " as t WHERE t.name=" + resource + ", t.name=tv.owner FOR JSON"
        variant_data = conn.execute(variant_table_query)
        return type_data, variant_data
