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
      # print(cmd)
      return self.__conn.execute(cmd)
  
  def executemany(self, cmd, param):
    with self.__lock:
      # print(cmd)
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
            sourceName text NOT NULL,
            sourceVariant text NOT NULL,
            
            PRIMARY KEY(featureName, variantName),
            
            FOREIGN KEY(featureName) REFERENCES features(name),
            FOREIGN KEY(entity) REFERENCES entities(name),
            FOREIGN KEY(provider) REFERENCES providers(name),
            FOREIGN KEY(sourceName) REFERENCES sources(name))''') 

         #insert feature variant wine data
       conn.execute("""INSERT INTO feature_variant VALUES 
            ("2020-08-10T13:49:51.141Z", "Sulfur Dioxide that is trapped", "wine_id", "Non_free_Sulfur_Dioxide", "Simba Khadder", "Sample online provider", "float", "first-variant", "CREATED", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "54", "Wine Data", "New source Variant"),
            ("2020-08-10T13:49:51.141Z", "Sulfur Dioxide that is trapped, streaming derived", "wine_id", "Non_free_Sulfur_Dioxide", "Simba Khadder", "Sample online provider", "float", "streaming-variant", "CREATED", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "52", "Wine Data", "New source Variant"),
            ("2020-08-10T13:49:51.141Z", "acidity that is fixed", "wine_id", "fixed_acidity", "Simba Khadder", "Sample online provider", "float", "first-variant", "CREATED", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "57", "Wine Data", "New source Variant"),
            ("2020-08-11", "acidity that is fixed, normalized", "wine_id", "fixed_acidity", "Simba Khadder", "Sample online provider", "float", "normalized-variant", "CREATED", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "59", "Wine Data", "New source Variant"),
            ("2020-08-10T13:49:51.141Z", "clean part of density", "wine_id", "clean_density", "Simba Khadder", "Sample batch provider", "float", "default variant", "CREATED", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "67", "Wine Data", "New source Variant"),
            ("2020-08-10T13:49:51.141Z", "null-lost clean part of density", "wine_id", "clean_density", "Simba Khadder", "Sample batch provider", "float", "null-lost variant", "CREATED", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "67", "Wine Data", "New source Variant"),
            ("2020-08-10T13:49:51.141Z", "average purchase price", "wine_id", "LogAvgPurchasePrice", "Simba Khadder", "Sample batch provider", "float", "first-variant", "CREATED", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "67", "Wine Data", "New source Variant"),
            ("2020-08-10T13:49:51.141Z", "average purchase price, streaming derived", "wine_id", "LogAvgPurchasePrice", "Simba Khadder", "Sample batch provider", "float", "streaming-variant", "CREATED", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "67", "Wine Data", "New source Variant")"""
       )
         # Features table
       conn.execute('''CREATE TABLE features(
            name text NOT NULL,
            defaultVariant text NOT NULL,
            type text,
            PRIMARY KEY (name));''')

         #insert feture wine data
       conn.execute("""INSERT INTO features VALUES
            ("Non_free_Sulfur_Dioxide", "first-variant", "Feature"),
            ("fixed_acidity", "first-variant", "Feature"),
            ("clean_density", "default variant", "Feature"),
            ("LogAvgPurchasePrice", "first-variant", "Feature")
            """)
         # training set variant
       conn.execute('''CREATE TABLE training_set_variant(
            created text,
            description text,            
            trainingSetName text NOT NULL,
            owner text,
            provider text NOT NULL,
            variantName text,
            label text,
            status text,
            PRIMARY KEY(trainingSetName, variantName),
            FOREIGN KEY(provider) REFERENCES providers(name),
            FOREIGN KEY(trainingSetName) REFERENCES training_sets(name));''')
        
       conn.execute('''INSERT INTO training_set_variant VALUES
            ("2020-08-10T13:49:51.141Z", "Sulfur Dioxide that is trapped", "Non_free_Sulfur_Dioxide", "Simba Khadder", "Sample batch provider", "first-variant", "Wine Data", "CREATED"),
            ("2020-08-10T13:49:51.141Z", "Sulfur Dioxide that is trapped, streaming derived", "Non_free_Sulfur_Dioxide", "Simba Khadder", "Sample batch provider", "streaming-variant", "Wine Data", "CREATED"),
            ("2020-08-10T13:49:51.141Z", "acidity that is fixed", "fixed_acidity", "Simba Khadder", "Sample batch provider", "first-variant", "Wine Data", "CREATED"),
            ("2020-08-11", "acidity that is fixed, normalized", "fixed_acidity", "Simba Khadder", "Sample batch provider", "normalized-variant", "Wine Data", "CREATED"),
            ("2020-08-10T13:49:51.141Z", "clean part of density", "clean_density", "Simba Khadder", "Sample batch provider", "default variant", "Wine Data", "CREATED"),
            ("2020-08-10T13:49:51.141Z", "null-lost clean part of density", "clean_density", "Simba Khadder", "Sample online provider", "null-lost variant", "Wine Data", "CREATED"),
            ("2020-08-10T13:49:51.141Z", "average purchase price", "LogAvgPurchasePrice", "Simba Khadder", "Sample online provider", "first-variant", "Wine Data", "CREATED"),
            ("2020-08-10T13:49:51.141Z", "average purchase price, streaming derived", "LogAvgPurchasePrice", "Simba Khadder", "Sample online provider", "streaming-variant", "Wine Data", "CREATED")
       ''')

         # Training-set table
       conn.execute('''CREATE TABLE training_sets(
            type text NOT NULL,
            defaultVariant text,
            name text PRIMARY KEY NOT NULL);''')
       
       conn.execute("""INSERT INTO training_sets VALUES
            ("TrainingSet", "first-variant", "Non_free_Sulfur_Dioxide"),
            ("TrainingSet", "first-variant", "fixed_acidity"),
            ("TrainingSet", "default variant", "clean_density"),
            ("TrainingSet", "first-variant", "LogAvgPurchasePrice")
            """)

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

       conn.execute(''' INSERT INTO source_variant VALUES
            ("2020-08-10T13:49:51.141Z", "Sulfur Dioxide that is trapped",                   "Non_free_Sulfur_Dioxide", "JSON", "Simba Khadder", "Sample online provider", "first-variant",       "CREATED", "wine_id"),
            ("2020-08-10T13:49:51.141Z", "Sulfur Dioxide that is trapped, streaming derived", "Non_free_Sulfur_Dioxide", "JSON", "Simba Khadder", "Sample online provider", "streaming-variant", "CREATED", "wine_id"),
            ("2020-08-10T13:49:51.141Z", "acidity that is fixed",                           "fixed_acidity", "JSON", "Simba Khadder", "Sample online provider", "first-variant",      "CREATED", "wine_id"),
            ("2020-08-10T13:49:51.141Z", "acidity that is fixed, normalized",                "Non_free_Sulfur_Dioxide", "JSON", "Simba Khadder", "Sample online provider", "normalized-variant", "CREATED", "wine_id"),
            ("2020-08-10T13:49:51.141Z", "clean part of density",                             "Non_free_Sulfur_Dioxide", "JSON", "Simba Khadder", "Sample online provider", "default variant",  "CREATED", "wine_id"),
            ("2020-08-10T13:49:51.141Z", "null-lost clean part of density",                   "clean_density", "JSON", "Simba Khadder", "Sample batch provider", "default-variant", "CREATED", "wine_id"),
            ("2020-08-10T13:49:51.141Z", "average purchase price",                            "LogAvgPurchasePrice", "JSON", "Simba Khadder", "Sample batch provider", "first-variant",     "CREATED", "wine_id"),
            ("2020-08-10T13:49:51.141Z", "average purchase price, streaming derived",         "fixed_acidity", "JSON", "Simba Khadder", "Sample batch provider", "streaming-variant", "CREATED", "wine_id")
       ''')

         # sources table
       conn.execute('''CREATE TABLE sources(
         type           text NOT NULL,
         defaultVariant text,
         name           text PRIMARY KEY NOT NULL);''')
       
       conn.execute("""INSERT INTO sources VALUES
            ("Source", "first-variant", "Non_free_Sulfur_Dioxide"),
            ("Source", "first-variant", "fixed_acidity"),
            ("Source", "default-variant", "clean_density"),
            ("Source", "first-variant", "LogAvgPurchasePrice")
            """)

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

       conn.execute('''INSERT INTO label_variant VALUES
            ("2020-08-12T13:49:51.141Z", "rating weighted in higher favor of reviews given higher presidence", "wine_id", "Wine quality rating", "Simba Khadder", "Sample batch provider", "float", "fifth-variant", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "82", "CREATED"),
            ("2020-08-12T13:49:51.141Z", "rating weighted in higher favor of reviews given higher presidence", "wine_id", "Wine quality rating", "Simba Khadder", "Sample batch provider", "float", "second-variant", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "82", "CREATED"),
            ("2020-08-12T13:49:51.141Z", "Wine was spoiled or not", "wine_id", "Wine spoiled", "Simba Khadder", "Sample batch provider", "float", "third-variant", "wine_analysis_id", "2020-08-14T13:49:51.141Z", "92", "CREATED")
       ''')

         # labels table
       conn.execute('''CREATE TABLE labels(
            type           text,
            defaultVariant text,
            name           text PRIMARY KEY);''')
       
       conn.execute("""INSERT INTO labels VALUES
            ("Label", "fifth-variant", "Wine quality rating"),
            ("Label", "third-variant", "Wine spoiled")
            """)

         # entity table
       conn.execute('''CREATE TABLE entities(
            name        text PRIMARY KEY NOT NULL,
            type        text,
            description text,
            status      text);''')

       conn.execute('''INSERT INTO entities VALUES
            ("wine_id", "Entity", "dataset holding information on wine quality", "CREATED")
       ''')
            
         # user table
       conn.execute('''CREATE TABLE users(
            name   text PRIMARY KEY NOT NULL,
            type   text,
            status text);''')

       conn.execute('''INSERT INTO users VALUES
            ("Simba Khadder", "User", "CREATED"),
            ("Shabnam Mohktarani", "User", "CREATED"),
            ("Sam Inloes", "User", "CREATED")
       ''')

         # models table
       conn.execute('''CREATE TABLE models(
            name        text PRIMARY KEY NOT NULL,
            type        text,
            description text,
            status      text);''')

       conn.execute('''INSERT INTO models VALUES
            ("Wine random forest", "Model", "Model classifying wine by spoilage and quality assesment", "CREATED")
       ''')
         
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
        
       conn.execute('''INSERT INTO providers VALUES
            ("Sample batch provider", "Provider", "Batch provider for historic data", "Batch", "BigQuery", "Customer model team", "wine_id", "CREATED", "serialized"),
            ("Sample online provider", "Provider", "Online provider", "Online", "Redis","Customer model team", "wine_id", "CREATED", "serialized")
       ''')
    
    # All 3 functions return a cursor, USE THIS
    def get_type_table(self, type):
        query = "SELECT * FROM " + type
        type_data = conn.execute(query)
        return type_data.fetchall()
    
    def get_variant_resource(self, type, variable, resource):
        variant_table_query = "SELECT * FROM "+ type +" WHERE " + variable + "='"+resource+"';" 
        variant_data = conn.execute(variant_table_query)
        return variant_data.fetchall()
    