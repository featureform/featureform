import sqlite3
from threading import Lock
import os


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
          self.path = '.featureform/SQLiteDB'
          if not os.path.exists(self.path):
               os.makedirs(self.path)
          raw_conn = sqlite3.connect(self.path+'/metadata.db', check_same_thread=False)
          self.__conn = SyncSQLExecutor(raw_conn)
          self.createTables()

     def createTables(self):
          # Features variant table
          self.__conn.execute('''CREATE TABLE IF NOT EXISTS feature_variant(
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

          # Features table
          self.__conn.execute('''CREATE TABLE IF NOT EXISTS features(
          name text NOT NULL,
          defaultVariant text NOT NULL,
          type text,
          PRIMARY KEY (name));''')

          # training set variant
          self.__conn.execute('''CREATE TABLE IF NOT EXISTS training_set_variant(
          created text,
          description text,            
          trainingSetName text NOT NULL,
          owner text,
          variantName text,
          label text,
          status text,
          features text,
          PRIMARY KEY(trainingSetName, variantName),
          FOREIGN KEY(trainingSetName) REFERENCES training_sets(name));''')

          # FOREIGN KEY(provider) REFERENCES providers(name),

          # Training-set table
          self.__conn.execute('''CREATE TABLE IF NOT EXISTS training_sets(
          type text NOT NULL,
          defaultVariant text,
          name text PRIMARY KEY NOT NULL);''')

          # source variant
          self.__conn.execute('''CREATE TABLE IF NOT EXISTS source_variant(
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
          self.__conn.execute('''CREATE TABLE IF NOT EXISTS sources(
          type           text NOT NULL,
          defaultVariant text,
          name           text PRIMARY KEY NOT NULL);''')

          # labels variant
          self.__conn.execute('''CREATE TABLE IF NOT EXISTS labels_variant(
          created         text,
          description     text,
          entity          text,
          labelName       text NOT NULL,
          owner           text,
          dataType        text,
          variantName     text,
          sourceEntity    text,
          sourceTimestamp text,
          sourceValue     text,
          status          text,
          sourceName      text,
          sourceVariant   text,
          PRIMARY KEY(labelName, variantName),
          FOREIGN KEY(labelName) REFERENCES labels(name));''')

#       provider        text,
#          FOREIGN KEY(provider) REFERENCES providers(name),

          # labels table
          self.__conn.execute('''CREATE TABLE IF NOT EXISTS labels(
          type           text,
          defaultVariant text,
          name           text PRIMARY KEY);''')

          # entity table
          self.__conn.execute('''CREATE TABLE IF NOT EXISTS entities(
          name        text PRIMARY KEY NOT NULL,
          type        text,
          description text,
          status      text);''')

          # user table
          self.__conn.execute('''CREATE TABLE IF NOT EXISTS users(
          name   text PRIMARY KEY NOT NULL,
          type   text,
          status text);''')

          # models table
          self.__conn.execute('''CREATE TABLE IF NOT EXISTS models(
          name        text PRIMARY KEY NOT NULL,
          type        text,
          description text,
          status      text);''')

          # providers table
          self.__conn.execute('''CREATE TABLE IF NOT EXISTS providers(
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

     # All 3 functions return a cursor, USE THIS
     def getTypeTable(self, type):
          query = "SELECT * FROM " + type
          type_data = self.__conn.execute(query)
          self.__conn.commit()
          return type_data.fetchall()

     def getVariantResource(self, type, column, resource):
          variant_table_query = "SELECT * FROM "+ type +" WHERE " + column + "='"+resource+"';"
          variant_data = self.__conn.execute(variant_table_query)
          self.__conn.commit()
          return variant_data.fetchall()
     
     def getNameVariant(self, type, column1, resource1, column2, resource2):
          variant_table_query = "SELECT * FROM "+ type +" WHERE " + column1 + "='"+resource1+"' AND " + column2 + "='"+resource2+"';"
          variant_data = self.__conn.execute(variant_table_query)
          self.__conn.commit()
          return variant_data.fetchall()

     def insert(self, tablename, *args):
          query = "INSERT OR IGNORE INTO "+tablename+" VALUES "+str(args)
          print("Printing the query")
          print(query)
          self.__conn.execute(query)
          self.__conn.commit()
          print("executed")

     # def commit_close(self):
     #      self.__conn.commit()
     #      self.__conn.close()
