from ast import Pass
import sqlite3
from sqlite3 import Error
from threading import Lock


class SyncSQLExecutor:
  def __init__(self, conn):
    self.__conn = conn
    self.__lock = Lock()

  def execute(self, cmd):
    with self.__lock:
      print(cmd)
      return self.__conn.execute(cmd)
  
  def executemany(self, cmd, param):
    with self.__lock:
      print(cmd)
      return self.__conn.executemany(cmd,param)

raw_conn = sqlite3.connect('test.db', check_same_thread=False)
conn = SyncSQLExecutor(raw_conn)
raw_cur = raw_conn.cursor()
cur = SyncSQLExecutor(raw_cur)

class SQLiteTest:
    def __init__(self):
        self.createTables()

    def createTables(self):
       conn.execute('''DROP TABLE IF EXISTS feature_variant''')
       conn.execute(''' DROP TABLE IF EXISTS features''')
       conn.execute('''DROP TABLE IF EXISTS training_set_variant''')
       conn.execute('''DROP TABLE IF EXISTS training_sets''')
       conn.execute('''DROP TABLE IF EXISTS source_variant''')
       conn.execute('''DROP TABLE IF EXISTS sources''')
       conn.execute('''DROP TABLE IF EXISTS label_variant''')
       conn.execute('''DROP TABLE IF EXISTS labels''')
       conn.execute('''DROP TABLE IF EXISTS entities''')
       conn.execute('''DROP TABLE IF EXISTS users''')
       conn.execute('''DROP TABLE IF EXISTS models''')
       conn.execute('''DROP TABLE IF EXISTS providers''')

         # Features variant table
       conn.execute('''CREATE TABLE feature_variant(
            created text,
            description text,
            entity text NOT NULL,
            featureName text NOT NULL,
            owner text,
            provider text NOT NULL,
            dataType text NOT NULL,
            variantName text NOT NULL,
            status text,
            sourceEntity text,
            sourceTimestamp text,
            sourceValue text,
            source text NOT NULL,
            
            PRIMARY KEY(featureName, variantName),
            
            FOREIGN KEY(featureName) REFERENCES features(name),
            FOREIGN KEY(entity) REFERENCES entities(name),
            FOREIGN KEY(provider) REFERENCES providers(name),
            FOREIGN KEY(source) REFERENCES sources(name))''') 

         #insert feature variant wine data
       conn.execute("""INSERT INTO feature_variant VALUES 
            ("2020-08-10T13:49:51.141Z", "Sulfur Dioxide that is trapped", "wine_id", "Non_free_Sulfur_Dioxide", "Simba Khadder", "cassandra", "float", "first-variant", "private", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "54", "Wine Data"),
            ("2020-08-10T13:49:51.141Z", "Sulfur Dioxide that is trapped, streaming derived", "wine_id", "Non_free_Sulfur_Dioxide", "Simba Khadder", "cassandra", "float", "streaming-variant", "private", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "52", "Wine Data"),
            ("2020-08-10T13:49:51.141Z", "acidity that is fixed", "wine_id", "fixed_acidity", "Simba Khadder", "cassandra", "float", "first-variant", "private", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "57", "Wine Data"),
            ("2020-08-11", "acidity that is fixed, normalized", "wine_id", "fixed_acidity", "Simba Khadder", "cassandra", "float", "normalized-variant", "private", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "59", "Wine Data"),
            ("2020-08-10T13:49:51.141Z", "clean part of density", "wine_id", "clean_density", "Simba Khadder", "cassandra", "float", "default variant", "private", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "67", "Wine Data"),
            ("2020-08-10T13:49:51.141Z", "null-lost clean part of density", "wine_id", "clean_density", "Simba Khadder", "cassandra", "float", "null-lost variant", "private", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "67", "Wine Data"),
            ("2020-08-10T13:49:51.141Z", "average purchase price", "wine_id", "LogAvgPurchasePrice", "Simba Khadder", "cassandra", "float", "first-variant", "private", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "67", "Wine Data"),
            ("2020-08-10T13:49:51.141Z", "average purchase price, streaming derived", "wine_id", "LogAvgPurchasePrice", "Simba Khadder", "cassandra", "float", "streaming-variant", "private", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "67", "Wine Data")"""
       )
         # Features table
       conn.execute('''CREATE TABLE features(
            name text NOT NULL,
            defaultVariant text NOT NULL,
            type text,
            PRIMARY KEY (name));''')

         #insert feture wine data
       conn.execute("""INSERT INTO features VALUES
            ("Non_free_Sulfur_Dioxide", "first-variant", "float"),
            ("fixed_acidity", "first-variant", "float"),
            ("clean_density", "default variant", "float"),
            ("LogAvgPurchasePrice", "first-variant", "float")
            """)
         # training set variant
       conn.execute('''CREATE TABLE training_set_variant(
            created text,
            description text,            
            trainingSetName text NOT NULL,
            owner text,
            provider text NOT NULL,
            variantName text,
            entity text,
            label text,
            status text,
            PRIMARY KEY(trainingSetName, variantName),
            FOREIGN KEY(provider) REFERENCES providers(name),
            FOREIGN KEY(trainingSetName) REFERENCES training_sets(name));''')
 
         # Training-set table
       conn.execute('''CREATE TABLE training_sets(
            type text NOT NULL,
            defaultVariant text,
            name text PRIMARY KEY NOT NULL);''')

         # source variant
       conn.execute('''CREATE TABLE source_variant(
            created     text,
            description text,
            sourceName  text NOT NULL,
            sourceType  text,
            owner       text,
            provider    text NOT NULL,
            variant     text,
            status      text,
            definition  text,
            PRIMARY KEY(sourceName, variant),
            FOREIGN KEY(provider) REFERENCES providers(name),
            FOREIGN KEY(sourceName) REFERENCES sources(name));''')

         # sources table
       conn.execute('''CREATE TABLE sources(
         type           text NOT NULL,
         defaultVariant text,
         name           text PRIMARY KEY NOT NULL);''')

         # labels variant
       conn.execute('''CREATE TABLE label_variant(
            created         text,
            description     text,
            entity          text,
            labelName       text NOT NULL,
            owner           text,
            provider        text NOT NULL,
            dataType        text,
            variantName     text,
            sourceEntity    text,
            sourceTimestamp text,
            sourceValue     text,
            status          text,
            PRIMARY KEY(labelName, variantName),
            FOREIGN KEY(provider) REFERENCES providers(name),
            FOREIGN KEY(labelName) REFERENCES labels(name));''')

         # labels table
       conn.execute('''CREATE TABLE labels(
            type           text,
            defaultVariant text,
            name           text PRIMARY KEY);''')

         # entity table
       conn.execute('''CREATE TABLE entities(
            name        text PRIMARY KEY NOT NULL,
            type        text,
            description text,
            status      text);''')
            
         # user table
       conn.execute('''CREATE TABLE users(
            name   text PRIMARY KEY NOT NULL,
            type   text,
            status text);''')

         # models table
       conn.execute('''CREATE TABLE models(
            name        text PRIMARY KEY NOT NULL,
            type        text,
            description text,
            status      text);''')
         
         # providers table
       conn.execute('''CREATE TABLE providers(
            name             text PRIMARY KEY NOT NULL,
            type             text,
            description      text,
            providerType     text,
            software         text,
            team             text,
            sources          text,
            status           text,
            serializedConfig text)''')
    
    # All 3 functions return a cursor, USE THIS
    def getTypeTable(self, type):
        query = "SELECT * FROM " + type
        type_data = conn.execute(query)
        return type_data.fetchall()
    
    def getTypeForResource(self, type, resource):
        type_table_query = "SELECT * FROM " + type + "WHERE name=" + resource
        type_data = conn.execute(type_table_query)
        return type_data.fetchall()
    
    def getVariantResource(self, type, variable, resource):
        variant_table_query = "SELECT * FROM "+ type +" WHERE " + variable + "='"+resource+"';" 
        variant_data = conn.execute(variant_table_query)
        return variant_data.fetchall()    
           