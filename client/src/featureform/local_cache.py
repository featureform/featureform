import json
import os
from functools import lru_cache
from typing import Callable, Set

import pandas as pd
from featureform import SQLiteMetadata
from featureform.local_utils import get_sql_transformation_sources
from featureform.resources import SourceType  # fix to do client.source.import
from pandas.core.generic import NDFrame
from typeguard import typechecked

from .file_utils import absolute_file_paths


class LocalCache:
    def __init__(self):
        feature_form_dir = os.environ.get("FEATUREFORM_DIR", ".featureform")
        self.cache_dir = os.environ.get(
            "FEATUREFORM_CACHE_DIR", os.path.join(feature_form_dir, "cache")
        )

    @typechecked
    def get_or_put(
        self,
        resource_type: str,
        resource_name: str,
        resource_variant: str,
        source_name: str,
        source_variant: str,
        func: Callable[[], NDFrame],
    ) -> NDFrame:
        """
        Caches the result of a callable to a local file. If the source files have changed, the cache is invalidated.
        """
        cache_file_path = self._cache_file_path(
            resource_type, resource_name, resource_variant
        )

        with SQLiteMetadata() as db:
            # check db for source files
            source_files_from_db = db.get_source_files_for_resource(
                resource_type, resource_name, resource_variant
            )
            if source_files_from_db:
                self._invalidate_cache_if_source_files_changed(
                    source_files_from_db, cache_file_path
                )

            # get source files from db or compute the sources
            source_files: Set[str] = (
                set(map(lambda x: x["file_path"], source_files_from_db))
                if source_files_from_db
                else self.get_source_files_for_source(db, source_name, source_variant)
            )

            return self._get_or_put(
                db,
                resource_type,
                resource_name,
                resource_variant,
                cache_file_path,
                source_files,
                func,
            )

    @typechecked
    def get_or_put_training_set(
        self,
        training_set_name: str,
        training_set_variant: str,
        func: Callable[[], NDFrame],
    ) -> NDFrame:
        """
        Caches the result of a training set to a local file. Difference between this one and the one above
        is how this needs to fetch all the source files for the training set.
        """
        resource_type = "training_set"

        file_path = self._cache_file_path(
            resource_type, training_set_name, training_set_variant
        )

        with SQLiteMetadata() as db:
            # check db for source files
            source_files_from_db = db.get_source_files_for_resource(
                resource_type, training_set_name, training_set_variant
            )

            # Only check to invalidate the cache if we have source files in the db
            if source_files_from_db:
                self._invalidate_cache_if_source_files_changed(
                    source_files_from_db, file_path
                )

            source_files = set()
            if source_files_from_db:
                source_files.update(
                    set(map(lambda x: x["file_path"], source_files_from_db))
                )
            else:
                ts_variant = db.get_training_set_variant(
                    training_set_name, training_set_variant
                )
                label_variant = db.get_label_variant(
                    ts_variant["label_name"], ts_variant["label_variant"]
                )
                source_files.update(
                    self.get_source_files_for_source(
                        db,
                        label_variant["source_name"],
                        label_variant["source_variant"],
                    )
                )

                features = db.get_training_set_features(
                    training_set_name, training_set_variant
                )
                for feature in features:
                    feature_variant = db.get_feature_variant(
                        feature["feature_name"], feature["feature_variant"]
                    )
                    source_files.update(
                        self.get_source_files_for_source(
                            db,
                            feature_variant["source_name"],
                            feature_variant["source_variant"],
                        )
                    )

            return self._get_or_put(
                db,
                resource_type,
                training_set_name,
                training_set_variant,
                file_path,
                source_files,
                func,
            )

    def _get_or_put(
        self,
        db,
        resource_type,
        resource_name,
        resource_variant,
        file_path,
        source_files,
        func,
    ) -> NDFrame:
        if os.path.exists(file_path):
            return pd.read_pickle(file_path)
        else:
            # create the dir if not exists and write the file
            df = func()
            os.makedirs(self.cache_dir, exist_ok=True)
            df.to_pickle(file_path)
            for source_file in source_files:
                db.insert_or_update(
                    "resource_source_files",
                    ["resource_type", "name", "variant", "file_path"],
                    ["updated_at"],
                    resource_type,
                    resource_name,
                    resource_variant,
                    source_file,
                    str(os.path.getmtime(source_file)),
                )
            return df

    @lru_cache(maxsize=128)
    def get_source_files_for_source(self, db, source_name, source_variant) -> Set[str]:
        """
        Recursively gets the source files for a given source. Each call is cached.
        """
        source = db.get_source_variant(source_name, source_variant)
        transform_type = db.is_transformation(source_name, source_variant)

        sources = set()
        if transform_type == SourceType.PRIMARY_SOURCE.value:
            return {source["definition"]}
        elif transform_type == SourceType.SQL_TRANSFORMATION.value:
            query = source["definition"]
            transformation_sources = get_sql_transformation_sources(query)
            for source_name, source_variant in transformation_sources:
                sources.update(
                    self.get_source_files_for_source(db, source_name, source_variant)
                )
        elif transform_type == SourceType.DF_TRANSFORMATION.value:
            dependencies = json.loads(source["inputs"])
            for name, variant in dependencies:
                sources.update(self.get_source_files_for_source(db, name, variant))
        elif transform_type == SourceType.DIRECTORY.value:
            path = source["definition"]
            for absolute_file, _ in absolute_file_paths(path):
                sources.add(absolute_file)
        else:
            raise Exception(f"Unknown source type: {transform_type}")
        return sources

    def _invalidate_cache_if_source_files_changed(
        self, source_files_from_db, cache_file_path
    ):
        if any(
            self._file_has_changed(source_file["updated_at"], source_file["file_path"])
            for source_file in source_files_from_db
        ):
            if os.path.exists(cache_file_path):
                os.remove(cache_file_path)

    def _cache_file_path(self, resource_type: str, name: str, variant: str):
        key = f"{resource_type}__{name}__{variant}"
        return f"{self.cache_dir}/{key}.pkl"

    @staticmethod
    def _file_has_changed(last_updated_at, file_path):
        """
        Currently using last updated at for determining if a file has changed. We can consider using the file hash
        if this becomes a performance issue.
        """
        os_last_updated = os.path.getmtime(file_path)
        return os_last_updated > float(last_updated_at)
