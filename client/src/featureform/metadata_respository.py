import json
import os
from abc import ABC
from typing import Set

from cachetools.func import lru_cache
from featureform import absolute_file_paths
from featureform.db import DB
from featureform.enums import SourceType
from featureform.local_utils import get_sql_transformation_sources
from sqlalchemy import text


class MetadataRepository(ABC):
    pass


class MetadataRepositoryLocalImpl(MetadataRepository):
    def __init__(self, db: DB):
        self.db = db

    def _fetch_data(self, query, type, name, variant):
        with self.db.transaction() as trx:
            result = trx.execute(text(query), {"name": name, "variant": variant})
            variant_data_list = result.fetchall()
            if len(variant_data_list) == 0:
                raise ValueError(f"{type} {name} ({variant}) not found")
            return variant_data_list

    def get_source_variant(self, name, variant):
        query = """
            SELECT v.*, t.tag_list as tags, p.property_list as properties
            FROM source_variant v
            LEFT JOIN tags t ON t.name = v.name AND t.variant = v.variant
            LEFT JOIN properties p ON p.name = v.name AND p.variant = v.variant
            WHERE v.name = :name AND v.variant = :variant;
        """

        return self._fetch_data(query, "source_variant", name, variant)[0]

    def query_resource(
        self, table, column, resource_name, should_fetch_tags_properties=False
    ):
        if should_fetch_tags_properties:
            query = text(
                """SELECT r.*, t.tag_list as tags, p.property_list as properties
                        FROM {0} r
                        LEFT JOIN tags t ON t.name = r.name
                        LEFT JOIN properties p ON p.name = r.name
                        WHERE r.{1} = :resource_name;""".format(
                    table, column
                )
            )
        else:
            query = text(f"SELECT * FROM {table} WHERE {column}= :resource_name;")
        with self.db.transaction() as trx:
            resource_data = trx.execute(
                query, {"resource_name": resource_name}
            ).fetchall()
        if len(resource_data) == 0 and column != "owner":
            raise ValueError(f"{table} with {column}: {resource_name} not found")
        return resource_data

    def query_resource_variant(self, table, column, resource_name):
        query = text(
            """SELECT r.*, t.tag_list as tags, p.property_list as properties
                        FROM {0} r
                        LEFT JOIN tags t ON t.name = r.name AND t.variant = r.variant
                        LEFT JOIN properties p ON p.name = r.name AND p.variant = r.variant
                        WHERE r.{1} = :resource_name;""".format(
                table, column
            )
        )
        with self.db.transaction() as trx:
            variant_data = trx.execute(
                query, {"resource_name": resource_name}
            ).fetchall()
        if len(variant_data) == 0 and column != "owner":
            raise ValueError(f"{table} with {column}: {resource_name} not found")
        return variant_data

    def is_transformation(self, name, variant):
        with self.db.transaction() as trx:
            query = text(
                "SELECT transformation FROM source_variant WHERE name=:name and variant=:variant"
            )
            result = trx.execute(query, {"name": name, "variant": variant})
            t = result.fetchall()
            if len(t) == 0:
                return 0
            return t[0][0]

    def get_source_files_for_resource(self, resource_type, name, variant):
        with self.db.transaction() as trx:
            query = text(
                "SELECT * FROM resource_source_files WHERE resource_type=:resource_type and name=:name and variant=:variant"
            )
            result = trx.execute(
                query,
                {"resource_type": resource_type, "name": name, "variant": variant},
            )
            return result.fetchall()

    def get_training_set_variant(self, name, variant):
        query = """SELECT v.*, t.tag_list as tags, p.property_list as properties
        FROM training_set_variant v
        LEFT JOIN tags t ON t.name = v.name AND t.variant = v.variant
        LEFT JOIN properties p ON p.name = v.name AND p.variant = v.variant
        WHERE v.name = :name AND v.variant = :variant
        """

        return self._fetch_data(query, "training_set_variant", name, variant)[0]

    def get_training_set_features(self, name, variant):
        query = """
            SELECT v.*, t.tag_list as tags, p.property_list as properties
            FROM training_set_features v
            LEFT JOIN tags t ON t.name = v.training_set_name AND t.variant = v.training_set_variant
            LEFT JOIN properties p ON p.name = v.training_set_name AND p.variant = v.training_set_variant
            WHERE v.training_set_name = :name AND v.training_set_variant = :variant
        """
        return self._fetch_data(query, "training_set_features", name, variant)

    def get_label_variant(self, name, variant):
        query = """
            SELECT v.*, t.tag_list as tags, p.property_list as properties
            FROM label_variant v
            LEFT JOIN tags t ON t.name = v.name AND t.variant = v.variant
            LEFT JOIN properties p ON p.name = v.name AND p.variant = v.variant
            WHERE v.name = :name AND v.variant = :variant
        """
        return self._fetch_data(query, "label_variant", name, variant)[0]

    def get_feature_variant(self, name, variant):
        query = """
            SELECT fv.*, t.tag_list as tags, p.property_list as properties
            FROM feature_variant fv
            LEFT JOIN tags t ON t.name = fv.name AND t.variant = fv.variant
            LEFT JOIN properties p ON p.name = fv.name AND p.variant = fv.variant
            WHERE fv.name = :name AND fv.variant = :variant
        """
        return self._fetch_data(query, "feature_variant", name, variant)[0]

    def persist_source_file_for_resource(
        self, resource_type, resource_name, resource_variant, source_file
    ):
        self.db.insert_or_update(
            "resource_source_files",
            ["resource_type", "name", "variant", "file_path"],
            ["updated_at"],
            resource_type,
            resource_name,
            resource_variant,
            source_file,
            str(os.path.getmtime(source_file)),
        )

    @lru_cache(maxsize=128)
    def get_source_files_for_source(
        self, source_name: str, source_variant: str
    ) -> Set[str]:
        """
        Returns all the source files for a source.
        """
        source = self.get_source_variant(source_name, source_variant)
        transform_type = self.is_transformation(source_name, source_variant)

        sources = set()
        if transform_type == SourceType.PRIMARY_SOURCE.value:
            return {source["definition"]}
        elif transform_type == SourceType.SQL_TRANSFORMATION.value:
            query = source["definition"]
            transformation_sources = get_sql_transformation_sources(query)
            for source_name, source_variant in transformation_sources:
                sources.update(
                    self.get_source_files_for_source(source_name, source_variant)
                )
        elif transform_type == SourceType.DF_TRANSFORMATION.value:
            dependencies = json.loads(source["inputs"])
            for name, variant in dependencies:
                sources.update(self.get_source_files_for_source(name, variant))
        elif transform_type == SourceType.DIRECTORY.value:
            path = source["definition"]
            for absolute_file, _ in absolute_file_paths(path):
                sources.add(absolute_file)
        else:
            raise Exception(f"Unknown source type: {transform_type}")
        return sources
