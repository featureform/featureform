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

    def execute_stmt(self, stmt, vals):
        with self.__lock:
            return self.__conn.execute(stmt, vals)

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
        raw_conn = sqlite3.connect(self.path + '/metadata.db', check_same_thread=False)
        raw_conn.row_factory = sqlite3.Row
        self.__conn = SyncSQLExecutor(raw_conn)
        self.createTables()

    def createTables(self):
        # Features variant table
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS feature_variant(
          created text,
          description text,
          entity text NOT NULL,
          name text NOT NULL,
          owner text,
          provider text NOT NULL,
          data_type text NOT NULL,
          variant text NOT NULL,
          status text,
          source_entity text,
          source_timestamp text,
          source_value text,
          source_name text NOT NULL,
          source_variant text NOT NULL,

          PRIMARY KEY(name, variant),

          FOREIGN KEY(name) REFERENCES features(name),
          FOREIGN KEY(entity) REFERENCES entities(name),
          FOREIGN KEY(provider) REFERENCES providers(name),
          FOREIGN KEY(source_name) REFERENCES sources(name))''')

        # Features table
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS features(
          name text NOT NULL,
          default_variant text NOT NULL,
          type text,
          PRIMARY KEY (name));''')

        # training set variant
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS training_set_variant(
          created text,
          description text,            
          name text NOT NULL,
          owner text,
          variant text,
          label_name text,
          label_variant text,
          status text,
          features text,
          PRIMARY KEY(name, variant),
          FOREIGN KEY(name) REFERENCES training_sets(name));''')

        # Training-set table
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS training_sets(
          type text NOT NULL,
          default_variant text,
          name text PRIMARY KEY NOT NULL);''')

        # Training set features
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS training_set_features(
          training_set_name text NOT NULL,
          training_set_variant text NOT NULL,
          feature_name text NOT NULL,
          feature_variant text NOT NULL,
          UNIQUE(training_set_name, training_set_variant, feature_name, feature_variant));''')

        # source variant
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS source_variant(
          created     text,
          description text,
          name  text NOT NULL,
          source_type  text,
          owner       text,
          provider    text NOT NULL,
          variant     text,
          status      text,
          transformation bool,
          inputs text, 
          definition  BLOB,
          PRIMARY KEY(name, variant),
          FOREIGN KEY(provider) REFERENCES providers(name),
          FOREIGN KEY(name) REFERENCES sources(name));''')

        # sources table
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS sources(
          type           text NOT NULL,
          default_variant text,
          name           text PRIMARY KEY NOT NULL);''')

        # labels variant
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS label_variant(
          created         text,
          description     text,
          entity          text,
          name            text NOT NULL,
          owner           text,
          provider        text,
          data_type       text,
          variant         text,
          source_entity    text,
          source_timestamp text,
          source_value     text,
          status          text,
          source_name      text,
          source_variant   text,
          FOREIGN KEY(provider) REFERENCES providers(name),
          PRIMARY KEY(name, variant),
          FOREIGN KEY(name) REFERENCES labels(name));''')

        # labels table
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS labels(
          type           text,
          default_variant text,
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
          provider_type     text,
          software         text,
          team             text,
          sources          text,
          status           text,
          serialized_config text)''')

        self.__conn.commit()

    def getTypeTable(self, type):
        query = "SELECT * FROM " + type
        type_data = self.__conn.execute(query)
        self.__conn.commit()
        return type_data.fetchall()

    def getVariantResource(self, type, column, resource):
        variant_table_query = "SELECT * FROM " + type + " WHERE " + column + "='" + resource + "';"
        variant_data = self.__conn.execute(variant_table_query)
        self.__conn.commit()
        variant_data_list = variant_data.fetchall()
        if len(variant_data_list) == 0:
          raise ValueError("{} with {}: {} not found".format(type, column, resource))
        return variant_data_list

    def getNameVariant(self, type, column1, resource1, column2, resource2):
        variant_table_query = "SELECT * FROM " + type + " WHERE " + column1 + "='" + resource1 + "' AND " + column2 + "='" + resource2 + "';"
        variant_data = self.__conn.execute(variant_table_query)
        self.__conn.commit()
        variant_data_list = variant_data.fetchall()
        if len(variant_data_list) == 0:
          raise ValueError("{} with {}: {} and {}: {} not found".format(type, column1, resource1, column2, resource2))
        return variant_data_list

    def is_transformation(self, name, variant):
        query = "SELECT transformation FROM source_variant WHERE name='" + name + "' and variant='" + variant + "';"
        transformation = self.__conn.execute(query)
        self.__conn.commit()
        t = transformation.fetchall()
        if len(t) == 0:
            return 0
        if t[0][0] is 1:
            return 1
        else:
            return 0

    def insert_source(self, tablename, *args):
        stmt = "INSERT OR IGNORE INTO " + tablename + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        self.__conn.execute_stmt(stmt, args)
        self.__conn.commit()

    def insert(self, tablename, *args):
        query = "INSERT OR IGNORE INTO " + tablename + " VALUES " + str(args)
        self.__conn.execute(query)
        self.__conn.commit()

    def close(self):
        self.__conn.close()