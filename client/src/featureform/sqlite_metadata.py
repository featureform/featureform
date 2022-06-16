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
               return self.__conn.execute(cmd)

     def executemany(self, cmd, param):
          with self.__lock:
               return self.__conn.executemany(cmd, param)

     def close(self):
          with self.__lock:
               return self.__conn.close()

     def commit(self):
          with self.__lock:
               return self.__conn.commit()



class SQLiteMetadata:
     def __init__(self):
     #    self.createTables()
          raw_conn = sqlite3.connect('metadata.db', check_same_thread=False)
          self.__conn = SyncSQLExecutor(raw_conn)

     def createTables(self):
     #   conn.execute('''DROP TABLE IF EXISTS feature_variant''')
     #   conn.execute('''DROP TABLE IF EXISTS features''')
     #   conn.execute('''DROP TABLE IF EXISTS training_set_variant''')
     #   conn.execute('''DROP TABLE IF EXISTS training_sets''')
     #   conn.execute('''DROP TABLE IF EXISTS source_variant''')
     #   conn.execute('''DROP TABLE IF EXISTS sources''')
     #   conn.execute('''DROP TABLE IF EXISTS label_variant''')
     #   conn.execute('''DROP TABLE IF EXISTS labels''')
     #   conn.execute('''DROP TABLE IF EXISTS entities''')
     #   conn.execute('''DROP TABLE IF EXISTS users''')
     #   __conn.execute('''DROP TABLE IF EXISTS models''')
     #   __conn.execute('''DROP TABLE IF EXISTS providers''')

     # Features variant table
          self.__conn.execute('''CREATE TABLE feature_variant(
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

          # Features table
          self.__conn.execute('''CREATE TABLE features(
          name text NOT NULL,
          defaultVariant text NOT NULL,
          type text,
          PRIMARY KEY (name));''')

          # training set variant
          self.__conn.execute('''CREATE TABLE training_set_variant(
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

          # Training-set table
          self.__conn.execute('''CREATE TABLE training_sets(
          type text NOT NULL,
          defaultVariant text,
          name text PRIMARY KEY NOT NULL);''')

          # source variant
          self.__conn.execute('''CREATE TABLE source_variant(
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
          self.__conn.execute('''CREATE TABLE sources(
          type           text NOT NULL,
          defaultVariant text,
          name           text PRIMARY KEY NOT NULL);''')

          # labels variant
          self.__conn.execute('''CREATE TABLE labels_variant(
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
          self.__conn.execute('''CREATE TABLE labels(
          type           text,
          defaultVariant text,
          name           text PRIMARY KEY);''')

          # entity table
          self.__conn.execute('''CREATE TABLE entities(
          name        text PRIMARY KEY NOT NULL,
          type        text,
          description text,
          status      text);''')

          # user table
          self.__conn.execute('''CREATE TABLE users(
          name   text PRIMARY KEY NOT NULL,
          type   text,
          status text);''')

          # models table
          self.__conn.execute('''CREATE TABLE models(
          name        text PRIMARY KEY NOT NULL,
          type        text,
          description text,
          status      text);''')

          # providers table
          self.__conn.execute('''CREATE TABLE providers(
          name             text PRIMARY KEY NOT NULL,
          type             text,
          description      text,
          providerType     text,
          software         text,
          team             text,
          sources          text,
          status           text,
          serializedConfig text)''')

          self.__conn.commit()
          self.__conn.close()

     # All 3 functions return a cursor, USE THIS
     def getTypeTable(self, type):
          query = "SELECT * FROM " + type
          type_data = self.__conn.execute(query)
          return type_data.fetchall()

     def getVariantResource(self, type, variable, resource):
          variant_table_query = "SELECT * FROM "+ type +" WHERE " + variable + "='"+resource+"';" 
          variant_data = self.__conn.execute(variant_table_query)
          return variant_data.fetchall()

     def insert(self, tablename, *args, check_nonexistence=""):
          query = "INSERT INTO "+tablename+" VALUES "+args+" "+check_nonexistence
          self.__conn.execute(query)
