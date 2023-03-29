import json
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
      
        # OnDemand Features variant table
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS ondemand_feature_variant(
          created text,
          description text,
          name text NOT NULL,
          owner text,
          variant text NOT NULL,
          status text,
          query text,

          PRIMARY KEY(name, variant),

          FOREIGN KEY(name) REFERENCES features(name))''')

        # Features table
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS features(
          name text NOT NULL,
          default_variant text NOT NULL,
          type text,
          PRIMARY KEY (name));''')

        # features type table
        # this is used to determine if it is a pre-calculated or client-calculated feature
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS feature_computation_mode (
          name text NOT NULL,
          variant text NOT NULL,
          mode text,
          is_on_demand integer,
          PRIMARY KEY (name, variant));''')

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

        # Training set lag features
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS training_set_lag_features(
          training_set_name text NOT NULL,
          training_set_variant text NOT NULL,
          feature_name text NOT NULL,
          feature_variant text NOT NULL,
          feature_new_name text,
          feature_lag real,
          UNIQUE(training_set_name, training_set_variant, feature_name, feature_variant, feature_new_name, feature_lag));''')

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
          type        text);''')

        # models training sets table
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS model_training_sets(
          model_name           text NOT NULL,
          training_set_name    text NOT NULL,
          training_set_variant text NOT NULL,
          UNIQUE(model_name, training_set_name, training_set_variant));''')

        # models features sets table
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS model_features(
          model_name           text NOT NULL,
          feature_name    text NOT NULL,
          feature_variant text NOT NULL,
          UNIQUE(model_name, feature_name, feature_variant));''')

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

        # source files for a resource
        # tracks the source files that have been used to create a resource and contains the files last_updated
        # this is used to determined if caches need to be invalidated
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS resource_source_files(
            resource_type text NOT NULL,
            name text NOT NULL,
            variant text NOT NULL,
            file_path text NOT NULL,
            updated_at text NOT NULL,
            PRIMARY KEY(resource_type, name, variant, file_path))''')

        # full-text search table
        self.__conn.execute('''CREATE VIRTUAL TABLE IF NOT EXISTS resources_fts USING fts5(
          resource_type, -- an "enum" column to filter with (e.g. feature, training_set, source, provider, entity, label, model, user)
          name,
          variant,
          description,
          status,
        );''')

        # resource table triggers to populate fts table
        self.__conn.execute('''CREATE TRIGGER IF NOT EXISTS feature_variant_after_insert
        AFTER INSERT ON feature_variant
        BEGIN
        INSERT INTO resources_fts(resource_type, name, variant, description, status)
        VALUES ('feature', new.name, new.variant, new.description, new.status);
        END;''')

        self.__conn.execute('''CREATE TRIGGER IF NOT EXISTS training_set_variant_after_insert
        AFTER INSERT ON training_set_variant
        BEGIN
        INSERT INTO resources_fts(resource_type, name, variant, description, status)
        VALUES ('training_set', new.name, new.variant, new.description, new.status);
        END;''')
        self.__conn.execute('''CREATE TRIGGER IF NOT EXISTS source_variant_after_insert
        AFTER INSERT ON source_variant
        BEGIN
        INSERT INTO resources_fts(resource_type, name, variant, description, status)
        VALUES ('source', new.name, new.variant, new.description, new.status);
        END;''')

        self.__conn.execute('''CREATE TRIGGER IF NOT EXISTS label_variant_after_insert
        AFTER INSERT ON label_variant
        BEGIN
        INSERT INTO resources_fts(resource_type, name, variant, description, status)
        VALUES ('label', new.name, new.variant, new.description, new.status);
        END;''')

        self.__conn.execute('''CREATE TRIGGER IF NOT EXISTS entities_after_insert
        AFTER INSERT ON entities
        BEGIN
        INSERT INTO resources_fts(resource_type, name, variant, description, status)
        VALUES ('entity', new.name, '', new.description, new.status);
        END;''')

        self.__conn.execute('''CREATE TRIGGER IF NOT EXISTS users_after_insert
        AFTER INSERT ON users
        BEGIN
        INSERT INTO resources_fts(resource_type, name, variant, description, status)
        VALUES ('user', new.name, '', '', new.status);
        END;''')

        self.__conn.execute('''CREATE TRIGGER IF NOT EXISTS models_after_insert
        AFTER INSERT ON models
        BEGIN
        INSERT INTO resources_fts(resource_type, name, variant, description, status)
        VALUES ('model', new.name, '', '', '');
        END;''')

        self.__conn.execute('''CREATE TRIGGER IF NOT EXISTS provider_after_insert
        AFTER INSERT ON providers
        BEGIN
        INSERT INTO resources_fts(resource_type, name, variant, description, status)
        VALUES ('provider', new.name, '', new.description, new.status);
        END;''')

        # Tags and Properties tables
        self.__conn.execute('''CREATE TABLE IF NOT EXISTS tags(
          name text NOT NULL,
          variant text,
          type text,
          tag_list text,
          UNIQUE(name,variant,type));''')

        self.__conn.execute('''CREATE TABLE IF NOT EXISTS properties(
          name text NOT NULL,
          variant text,
          type text,
          property_list text,
          UNIQUE(name,variant,type));''')

        self.__conn.commit()

    def get_type_table(self, tablename):
        if tablename in ["entities", "models", "users", "providers"]:
          query = '''SELECT r.*, t.tag_list as tags, p.property_list as properties
          FROM {0} r
          LEFT JOIN tags t ON t.name = r.name
          LEFT JOIN properties p ON p.name = r.name     ;
          '''.format(tablename)
        else:
          query = f"SELECT * FROM {tablename}"
        type_data = self.__conn.execute(query)
        self.__conn.commit()
        return type_data.fetchall()

    def query_resource(self, table, column, resource_name, should_fetch_tags_properties=False):
        if should_fetch_tags_properties:
           query = '''SELECT r.*, t.tag_list as tags, p.property_list as properties
           FROM {0} r
           LEFT JOIN tags t ON t.name = r.name
           LEFT JOIN properties p ON p.name = r.name
           WHERE r.{1} = '{2}';'''.format(table, column, resource_name)
        else:
          query = f"SELECT * FROM {table} WHERE {column}='{resource_name}';"
        resource_data = self.__conn.execute(query)
        self.__conn.commit()
        resource_data_list = resource_data.fetchall()
        if len(resource_data_list) == 0 and column != "owner":
          raise ValueError(f"{table} with {column}: {resource_name} not found")
        return resource_data_list

    def query_resource_variant(self, table, column, resource_name):
        query = '''SELECT r.*, t.tag_list as tags, p.property_list as properties
           FROM {0} r
           LEFT JOIN tags t ON t.name = r.name AND t.variant = r.variant
           LEFT JOIN properties p ON p.name = r.name AND p.variant = r.variant
           WHERE r.{1} = '{2}';'''.format(table, column, resource_name)
        variant_data = self.__conn.execute(query)
        self.__conn.commit()
        variant_data_list = variant_data.fetchall()
        if len(variant_data_list) == 0 and column != "owner":
          raise ValueError(f"{table} with {column}: {resource_name} not found")
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

    def get_user(self, name, should_fetch_tags_properties):
      return self.query_resource("users", "name", name, should_fetch_tags_properties)[0]

    def get_entity(self, name, should_fetch_tags_properties):
      return self.query_resource("entities", "name", name, should_fetch_tags_properties)[0]

    def get_feature(self, name, should_fetch_tags_properties):
      return self.query_resource("features", "name", name, should_fetch_tags_properties)[0]

    def get_label(self, name, should_fetch_tags_properties):
      return self.query_resource("labels", "name", name, should_fetch_tags_properties)[0]

    def get_source(self, name, should_fetch_tags_properties):
      return self.query_resource("sources", "name", name, should_fetch_tags_properties)[0]

    def get_training_set(self, name, should_fetch_tags_properties):
      return self.query_resource("training_sets", "name", name, should_fetch_tags_properties)[0]

    def get_model(self, name, should_fetch_tags_properties):
      return self.query_resource("models", "name", name, should_fetch_tags_properties)[0]

    def get_provider(self, name, should_fetch_tags_properties):
      return self.query_resource("providers", "name", name, should_fetch_tags_properties)[0]

    def get_feature_variant(self, name, variant):
        query = '''SELECT fv.*, t.tag_list as tags, p.property_list as properties
        FROM feature_variant fv
        LEFT JOIN tags t ON t.name = fv.name AND t.variant = fv.variant
        LEFT JOIN properties p ON p.name = fv.name AND p.variant = fv.variant
        WHERE fv.name = '{0}' AND fv.variant = '{1}';'''.format(name, variant)
        return self.fetch_data(query, "feature_variant", name, variant)[0]

    def get_feature_variants_from_provider(self, name):
        return self.query_resource_variant("feature_variant", "provider", name)

    def get_feature_variants_from_source(self, name, variant):
        query = '''SELECT fv.*, t.tag_list as tags, p.property_list as properties
        FROM feature_variant fv
        LEFT JOIN tags t ON t.name = fv.name AND t.variant = fv.variant
        LEFT JOIN properties p ON p.name = fv.name AND p.variant = fv.variant
        WHERE fv.source_name = '{0}' AND fv.source_variant = '{1}';'''.format(name, variant)
        return self.fetch_data_safe(query, "feature_variant", name, variant)

    def get_feature_variants_from_feature(self, name):
      return self.query_resource_variant("feature_variant", "name", name)
    
    def get_feature_variant_mode(self, name, variant):
      query = f"SELECT mode FROM feature_computation_mode WHERE name='{name}' AND variant='{variant}'"
      return self.fetch_data_safe(query, "feature_computation_mode", name, variant)[0]["mode"]
    
    def get_feature_variant_on_demand(self, name, variant):
      query = f"SELECT is_on_demand FROM feature_computation_mode WHERE name='{name}' AND variant='{variant}'"
      return bool(self.fetch_data_safe(query, "feature_computation_mode", name, variant)[0]["is_on_demand"])

    def get_ondemand_feature_query(self, name, variant):
      query = f"SELECT query FROM ondemand_feature_variant WHERE name='{name}' AND variant='{variant}'"
      return self.fetch_data_safe(query, "ondemand_feature_variant", name, variant)[0]["query"]

    def get_training_set_variant(self, name, variant):
        query = f"SELECT * FROM training_set_variant WHERE name = '{name}' AND variant = '{variant}';"
        query = '''SELECT v.*, t.tag_list as tags, p.property_list as properties
        FROM training_set_variant v
        LEFT JOIN tags t ON t.name = v.name AND t.variant = v.variant
        LEFT JOIN properties p ON p.name = v.name AND p.variant = v.variant
        WHERE v.name = '{0}' AND v.variant = '{1}';
        '''.format(name, variant)
        return self.fetch_data(query, "training_set_variant", name, variant)[0]

    def get_training_set_variant_from_training_set(self, name):
        return self.query_resource_variant("training_set_variant", "name", name)
    
    def get_training_set_variant_from_label(self, name, variant):
        query = '''SELECT v.*, t.tag_list as tags, p.property_list as properties
        FROM training_set_variant v
        LEFT JOIN tags t ON t.name = v.name AND t.variant = v.variant
        LEFT JOIN properties p ON p.name = v.name AND p.variant = v.variant
        WHERE v.label_name = '{0}' AND v.label_variant = '{1}';
        '''.format(name, variant)
        return self.fetch_data_safe(query, "training_set_variant", name, variant)
    
    def get_label_variant(self, name, variant):
        query = '''SELECT v.*, t.tag_list as tags, p.property_list as properties
        FROM label_variant v
        LEFT JOIN tags t ON t.name = v.name AND t.variant = v.variant
        LEFT JOIN properties p ON p.name = v.name AND p.variant = v.variant
        WHERE v.name = '{0}' AND v.variant = '{1}';
        '''.format(name, variant)
        return self.fetch_data(query, "label_variant", name, variant)[0]
    
    def get_label_variants_from_label(self, name):
        return self.query_resource_variant("label_variant", "name", name)

    def get_label_variants_from_provider(self, name):
        return self.query_resource_variant("label_variant", "provider", name)
    
    def get_label_variants_from_source(self, name, variant):
        query = '''SELECT v.*, t.tag_list as tags, p.property_list as properties
        FROM label_variant v
        LEFT JOIN tags t ON t.name = v.name AND t.variant = v.variant
        LEFT JOIN properties p ON p.name = v.name AND p.variant = v.variant
        WHERE v.source_name = '{0}' AND v.source_variant = '{1}';
        '''.format(name, variant)
        return self.fetch_data_safe(query, "label_variant", name, variant)

    def get_source_variant(self, name, variant):
        query = '''SELECT v.*, t.tag_list as tags, p.property_list as properties
        FROM source_variant v
        LEFT JOIN tags t ON t.name = v.name AND t.variant = v.variant
        LEFT JOIN properties p ON p.name = v.name AND p.variant = v.variant
        WHERE v.name = '{0}' AND v.variant = '{1}';
        '''.format(name, variant)
        return self.fetch_data(query, "source_variant", name, variant)[0]

    def get_source_variants_from_source(self, name):
        return self.query_resource_variant("source_variant", "name", name)

    def get_training_set_from_features(self, name, variant):
        query = '''SELECT v.*, t.tag_list as tags, p.property_list as properties
        FROM training_set_features v
        LEFT JOIN tags t ON t.name = v.training_set_name AND t.variant = v.training_set_variant
        LEFT JOIN properties p ON p.name = v.training_set_name AND p.variant = v.training_set_variant
        WHERE v.feature_name = '{0}' AND v.feature_variant = '{1}';
        '''.format(name, variant)
        return self.fetch_data_safe(query, "training_set_features", name, variant)

    def get_training_set_from_labels(self, name, variant):
        query = '''SELECT v.*, t.tag_list as tags, p.property_list as properties
        FROM training_set_variant v
        LEFT JOIN tags t ON t.name = v.name AND t.variant = v.variant
        LEFT JOIN properties p ON p.name = v.name AND p.variant = v.variant
        WHERE v.label_name = '{0}' AND v.label_variant = '{1}';
        '''.format(name, variant)
        return self.fetch_data_safe(query, "training_set_variant", name, variant)

    def get_training_set_features(self, name, variant):
        query = '''SELECT v.*, t.tag_list as tags, p.property_list as properties
        FROM training_set_features v
        LEFT JOIN tags t ON t.name = v.training_set_name AND t.variant = v.training_set_variant
        LEFT JOIN properties p ON p.name = v.training_set_name AND p.variant = v.training_set_variant
         WHERE v.training_set_name = '{0}' AND v.training_set_variant = '{1}';
        '''.format(name, variant)
        return self.fetch_data(query, "training_set_features", name, variant)
      
    def get_training_set_lag_features(self, name, variant):
        query = '''SELECT v.*, t.tag_list as tags, p.property_list as properties
        FROM training_set_lag_features v
        LEFT JOIN tags t ON t.name = v.training_set_name AND t.variant = v.training_set_variant
        LEFT JOIN properties p ON p.name = v.training_set_name AND p.variant = v.training_set_variant
         WHERE v.training_set_name = '{0}' AND v.training_set_variant = '{1}';
        '''.format(name, variant)
        return self.fetch_data_safe(query, "training_set_lag_features", name, variant)

    def get_resource_with_source(self, tablename, source_name, source_variant):
        query = '''SELECT v.*, t.tag_list as tags, p.property_list as properties
        FROM {0} v
        LEFT JOIN tags t ON t.name = v.name AND t.variant = v.variant
        LEFT JOIN properties p ON p.name = v.name AND p.variant = v.variant
         WHERE v.source_name = '{1}' AND v.source_variant = '{2}';
        '''.format(tablename, source_name, source_variant)
        return self.fetch_data(query, tablename, source_name, source_variant)

    def is_transformation(self, name, variant):
        query = f"SELECT transformation FROM source_variant WHERE name='{name}' and variant='{variant}';"
        transformation = self.__conn.execute(query)
        self.__conn.commit()
        t = transformation.fetchall()
        if len(t) == 0:
            return 0
        return t[0][0]

    def get_source_files_for_resource(self, resource_type, name, variant):
        query = f"SELECT * FROM resource_source_files WHERE resource_type='{resource_type}' and name='{name}' and variant='{variant}';"
        result = self.__conn.execute(query)
        self.__conn.commit()
        return result.fetchall()

    def get_source_file_last_updated(self, resource_type, name, variant, file_path):
        query = f"SELECT * FROM resource_source_files WHERE resource_type='{resource_type}' and name='{name}' and variant='{variant}' and file_path='{file_path}';"
        result = self.__conn.execute(query)
        self.__conn.commit()
        res = result.fetchone()
        return res[0] if res else None

    def insert_source(self, tablename, *args):
        stmt = f"INSERT OR IGNORE INTO {tablename} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        self.__conn.execute_stmt(stmt, args)
        self.__conn.commit()

    def insert(self, tablename, *args):
        query = f"INSERT OR IGNORE INTO {tablename} VALUES {str(args)}"
        self.__conn.execute(query)
        self.__conn.commit()

    def insert_or_update(self, tablename, keys, cols, *args):
        """
        Upserts a row into the table. `keys` indicate columns that are unique
        and `cols` are the columns that are updated.
        """
        query = (
            f"INSERT INTO {tablename} VALUES {str(args)} "
            f"ON CONFLICT ({','.join(keys)}) DO UPDATE SET {','.join([f'{col}=excluded.{col}' for col in cols])}"
        )
        self.__conn.execute(query)
        self.__conn.commit()

    # TODO get something generic working, possibly consider using the above insert or update
    def upsert(self, tablename, *args):
        if tablename == "tags" or tablename == "properties":
           query, is_update = self.upsert_tags_properties(tablename, *args)
        else:
           raise NotImplementedError(f"UPSERT not implemented for table {tablename}")

        if is_update:
          self.__conn.execute(query)
          self.__conn.commit()
        else:
           self.insert(tablename, *args)

    def close(self):
        self.__conn.close()

    def search(self, phrase, resource_type=None):
        if resource_type is None:
          query = f"SELECT * FROM resources_fts WHERE resources_fts match '{{name variant description status}} : {phrase}*';"
        else:
          query = f"SELECT * FROM resources_fts WHERE resources_fts match '{{name variant description status}} : {phrase}*' AND resource_type = '{resource_type}';"

        search_results = self.__conn.execute(query)
        self.__conn.commit()
        results_list = search_results.fetchall()

        return results_list

    def upsert_tags_properties(self, tablename, *args):
        result_idx = 0
        type_idx = 2
        updatable_column_idx = -1
        query = ""
        updatable_column = "tag_list" if tablename == "tags" else "property_list"
        default_value = [] if tablename == "tags" else {}
        is_update = False
        name = args[0]
        variant = args[1]

        cursor = self.__conn.execute(f"SELECT * FROM {tablename} WHERE name = '{name}' AND variant = '{variant}';")
        self.__conn.commit()
        existing = cursor.fetchall()

        if len(existing):
          existing_val = json.loads(existing[result_idx][updatable_column]) if existing[result_idx][updatable_column] is not None else default_value
          if tablename == "tags":
            updated = json.dumps(list(set(existing_val + json.loads(args[updatable_column_idx]))))
          else:
             updated = json.dumps({**existing_val, **json.loads(args[updatable_column_idx])})
          args = list(args)
          args[updatable_column_idx] = updated
          query = f"UPDATE {tablename} SET name = '{name}', variant = '{variant}', type = '{args[type_idx]}', {updatable_column} = '{args[updatable_column_idx]}'"
          query += f"WHERE name = '{name}' AND variant = '{variant}';"
          is_update = True

        return (query, is_update)
