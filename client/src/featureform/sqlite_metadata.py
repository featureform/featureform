import sqlite3
from threading import Lock
import sys
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
          variant    text,
          status      text,
          transformation text,
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

    def get_type_table(self, type):
        query = f"SELECT * FROM {type}"
        type_data = self.__conn.execute(query)
        self.__conn.commit()
        return type_data.fetchall()

    def query_resource(self, type, column, resource):
        query = f"SELECT * FROM {type} WHERE {column}='{resource}';"
        variant_data = self.__conn.execute(query)
        self.__conn.commit()
        variant_data_list = variant_data.fetchall()
        if len(variant_data_list) == 0 and column != "owner":
          raise ValueError(f"{type} with {column}: {resource} not found")
        return variant_data_list
  
    def fetch_data(self, query, type, name, variant):
        variant_data = self.__conn.execute(query)
        self.__conn.commit()
        variant_data_list = variant_data.fetchall()
        if len(variant_data_list) == 0:
          raise ValueError(f"{type} with name: {name} and variant: {variant} not found")
        return variant_data_list

    def fetch_data_safe(self, query, type, name, variant):
        variant_data = self.__conn.execute(query)
        self.__conn.commit()
        variant_data_list = variant_data.fetchall()
        if len(variant_data_list) == 0:
          return []
        return variant_data_list

    def get_user(self, name):
      return self.query_resource("users", "name", name)[0]

    def get_entity(self, name):
      return self.query_resource("entities", "name", name)[0]

    def get_feature(self, name):
      return self.query_resource("features", "name", name)[0]
    
    def get_label(self, name):
      return self.query_resource("labels", "name", name)[0]
    
    def get_source(self, name):
      return self.query_resource("sources", "name", name)[0]

    def get_training_set(self, name):
      return self.query_resource("training_sets", "name", name)[0]
    
    def get_model(self, name):
      return self.query_resource("models", "name", name)[0]
    
    def get_provider(self, name):
      return self.query_resource("providers", "name", name)[0]

    def get_feature_variant(self, name, variant):
        query = f"SELECT * FROM feature_variant WHERE name = '{name}' AND variant = '{variant}';"
        return self.fetch_data(query, "feature_variant", name, variant)[0]

    def get_feature_variants_from_provider(self, name):
        return self.query_resource("feature_variant", "provider", name)

    def get_feature_variants_from_source(self, name, variant):
        query = f"SELECT * FROM feature_variant WHERE source_name = '{name}' AND source_variant = '{variant}';"
        return self.fetch_data_safe(query, "feature_variant", name, variant)

    def get_feature_variants_from_feature(self, name):
      return self.query_resource("feature_variant", "name", name)

    def get_training_set_variant(self, name, variant):
        query = f"SELECT * FROM training_set_variant WHERE name = '{name}' AND variant = '{variant}';"
        return self.fetch_data(query, "training_set_variant", name, variant)[0]

    def get_training_set_variant_from_training_set(self, name):
        return self.query_resource("training_set_variant", "name", name)
    
    def get_training_set_variant_from_label(self, name, variant):
        query = f"SELECT * FROM training_set_variant WHERE label_name = '{name}' AND label_variant = '{variant}';"
        return self.fetch_data_safe(query, "training_set_variant", name, variant)
    
    def get_label_variant(self, name, variant):
        query = f"SELECT * FROM label_variant WHERE name = '{name}' AND variant = '{variant}';"
        return self.fetch_data(query, "label_variant", name, variant)[0]
    
    def get_label_variants_from_label(self, name):
        return self.query_resource("label_variant", "name", name)

    def get_label_variants_from_provider(self, name):
        query = f"SELECT * FROM label_variant WHERE provider = '{name}';"
        return self.query_resource("label_variant", "provider", name)
    
    def get_label_variants_from_source(self, name, variant):
        query = f"SELECT * FROM label_variant WHERE source_name = '{name}' AND source_variant = '{variant}';"
        return self.fetch_data_safe(query, "label_variant", name, variant)

    def get_source_variant(self, name, variant):
        query = f"SELECT * FROM source_variant WHERE name = '{name}' AND variant = '{variant}';"
        return self.fetch_data(query, "source_variant", name, variant)[0]

    def get_source_variants_from_source(self, name):
        return self.query_resource("source_variant", "name", name)

    def get_training_set_from_features(self, name, variant):
        query = f"SELECT * FROM training_set_features WHERE feature_name = '{name}' AND feature_variant = '{variant}';"
        return self.fetch_data_safe(query, "training_set_features", name, variant)

    def get_training_set_from_labels(self, name, variant):
        query = f"SELECT * FROM training_set_variant WHERE label_name = '{name}' AND label_variant = '{variant}';"
        return self.fetch_data_safe(query, "training_set_variant", name, variant)

    def get_training_set_features(self, name, variant):
        query = f"SELECT * FROM training_set_features WHERE training_set_name = '{name}' AND training_set_variant = '{variant}';"
        return self.fetch_data(query, "training_set_features", name, variant)

    def get_resource_with_source(self, type, source_name, source_variant):
        query = f"SELECT * FROM {type} WHERE source_name ='{source_name}' AND source_variant ='{source_variant}';"
        return self.fetch_data(query, type, source_name, source_variant)

    def is_transformation(self, name, variant):
        query = f"SELECT transformation FROM source_variant WHERE name='{name}' and variant='{variant}';"
        transformation = self.__conn.execute(query)
        self.__conn.commit()
        t = transformation.fetchall()
        if len(t) == 0:
            return 0
        return t[0][0]

    def insert_source(self, tablename, *args):
        stmt = f"INSERT OR IGNORE INTO {tablename} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        self.__conn.execute_stmt(stmt, args)
        self.__conn.commit()

    def insert(self, tablename, *args):
        query = f"INSERT OR IGNORE INTO {tablename} VALUES {str(args)}"
        self.__conn.execute(query)
        self.__conn.commit()

    def close(self):
        self.__conn.close()