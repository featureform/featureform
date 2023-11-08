import json
from abc import ABC, abstractmethod
from typing import List

from typeguard import typechecked

from . import SQLiteMetadata
from .resources import (
    Feature,
    LocalConfig,
    Resource,
    FeatureVariant,
    Source,
    SourceVariant,
    Provider,
    TrainingSet,
    Label,
    Model,
    Entity,
    LabelVariant,
    TrainingSetVariant,
    User,
    ResourceColumnMapping,
    TrainingSetFeatures,
)


@typechecked
class MetadataRepository(ABC):
    @abstractmethod
    def create_resource(self, resource: Resource):
        raise NotImplementedError

    @abstractmethod
    def update_resource(self, resource):
        raise NotImplementedError

    @abstractmethod
    def get_feature_variant(self, name: str, variant: str) -> FeatureVariant:
        raise NotImplementedError

    @abstractmethod
    def get_features(self) -> List[Feature]:
        raise NotImplementedError

    @abstractmethod
    def get_label_variant(self, name: str, variant: str) -> LabelVariant:
        raise NotImplementedError

    @abstractmethod
    def get_labels(self) -> List[Label]:
        raise NotImplementedError

    @abstractmethod
    def get_source_variant(self, name: str, variant: str) -> SourceVariant:
        raise NotImplementedError

    @abstractmethod
    def get_sources(self) -> List[Source]:
        raise NotImplementedError

    @abstractmethod
    def get_training_set_variant(self, name: str, variant: str) -> TrainingSetVariant:
        raise NotImplementedError

    @abstractmethod
    def get_training_sets(self) -> List[TrainingSet]:
        raise NotImplementedError

    @abstractmethod
    def get_entity(self, name: str) -> Entity:
        raise NotImplementedError

    @abstractmethod
    def get_entities(self) -> List[Entity]:
        raise NotImplementedError

    @abstractmethod
    def get_model(self, name: str) -> Model:
        raise NotImplementedError

    @abstractmethod
    def get_models(self) -> List[Model]:
        raise NotImplementedError

    @abstractmethod
    def get_provider(self, name: str) -> Provider:
        raise NotImplementedError

    @abstractmethod
    def get_providers(self) -> List[Provider]:
        raise NotImplementedError

    @abstractmethod
    def get_user(self, name: str) -> User:
        raise NotImplementedError

    @abstractmethod
    def get_users(self) -> List[User]:
        raise NotImplementedError

    @abstractmethod
    def get_tags_for_resource(
        self, name: str, variant: str, resource_type: str
    ) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def get_training_set_features(
        self, name: str, variant: str
    ) -> List[TrainingSetFeatures]:
        raise NotImplementedError

    @abstractmethod
    def get_feature_variants_from_source(
        self, source_name: str, source_variant: str
    ) -> List[FeatureVariant]:
        raise NotImplementedError

    @abstractmethod
    def get_training_set_from_features(
        self, feature_name: str, feature_variant: str
    ) -> List[TrainingSetFeatures]:
        raise NotImplementedError

    @abstractmethod
    def get_label_variants_from_source(
        self, source_name: str, source_variant: str
    ) -> List[LabelVariant]:
        raise NotImplementedError

    @abstractmethod
    def get_training_set_variant_from_label(
        self, label_name: str, label_variant: str
    ) -> List[TrainingSetVariant]:
        raise NotImplementedError


class MetadataRepositoryLocalImpl(MetadataRepository):
    def __init__(self, db: SQLiteMetadata):
        self.db = db

    def create_resource(self, resource: Resource):
        # TODO actual db creation should happen here -- I shouldn't call back to the resource
        resource._create_local(self.db)

    def update_resource(self, resource):
        raise NotImplementedError

    def get_model(self, name: str) -> Model:
        model_row = self.db.get_model(name, should_fetch_tags_properties=True)
        return Model(
            name=model_row["name"],
            description="",
            tags=json.loads(model_row["tags"]) if model_row["tags"] else [],
            properties=json.loads(model_row["properties"])
            if model_row["properties"]
            else {},
        )

    def get_models(self) -> List[Model]:
        model_rows = self.db.query_resource("models", should_fetch_tags_properties=True)
        return [
            Model(
                name=row["name"],
                tags=json.loads(row["tags"]) if row["tags"] else [],
                properties=json.loads(row["properties"]) if row["properties"] else {},
            )
            for row in model_rows
        ]

    def get_feature_variant(self, name, variant) -> FeatureVariant:
        result = self.db.get_feature_variant(name, variant)
        return FeatureVariant(
            created=result["created"],
            name=result["name"],
            variant=result["variant"],
            source=(result["source_name"], result["source_variant"]),
            value_type=result["data_type"],
            is_embedding=bool(result["is_embedding"]),
            dims=result["dimension"],
            entity=result["entity"],
            owner=result["owner"],
            provider=result["provider"],
            status=result["status"],
            location=ResourceColumnMapping(
                result["source_entity"],
                result["source_value"],
                result["source_timestamp"],
            ),
            description=result["description"],
            tags=json.loads(result["tags"]) if result["tags"] else [],
            properties=json.loads(result["properties"]) if result["properties"] else {},
        )

    def get_features(self) -> List[Feature]:
        feature_rows = self.db.query_resource("features")
        features = []
        for feature_row in feature_rows:
            variants = [
                r["variant"]
                for r in self.db.query_resource(
                    "feature_variant", "name", feature_row["name"]
                )
            ]
            features.append(
                Feature(
                    name=feature_row["name"],
                    default_variant=feature_row["default_variant"],
                    variants=variants,
                )
            )

        return features

    def get_label_variant(self, name: str, variant: str) -> LabelVariant:
        result = self.db.get_label_variant(name, variant)
        return LabelVariant(
            name=result["name"],
            variant=result["variant"],
            source=(result["source_name"], result["source_variant"]),
            value_type=result["data_type"],
            created=result["created"],
            entity=result["entity"],
            status=result["status"],
            owner=result["owner"],
            provider=result["provider"],
            location=ResourceColumnMapping(
                result["source_entity"],
                result["source_value"],
                result["source_timestamp"],
            ),
            description=result["description"],
            tags=json.loads(result["tags"]) if result["tags"] else [],
            properties=json.loads(result["properties"]) if result["properties"] else {},
        )

    def get_labels(self) -> List[Label]:
        label_rows = self.db.query_resource("labels")
        labels = []
        for label_row in label_rows:
            variants = [
                r["variant"]
                for r in self.db.query_resource(
                    "label_variant", "name", label_row["name"]
                )
            ]
            labels.append(
                Label(
                    name=label_row["name"],
                    default_variant=label_row["default_variant"],
                    variants=variants,
                )
            )

        return labels

    def get_source_variant(self, name: str, variant: str) -> SourceVariant:
        result = self.db.get_source_variant(name, variant)
        name_variant_list = []
        input_json = json.loads(result["inputs"] if result["inputs"] else [])
        if input_json and len(input_json[0]):  # we store empty inputs as [[]]
            name_variant_list = [{"name": v[0], "variant": v[1]} for v in input_json]
        return SourceVariant(
            created=result["created"],
            name=result["name"],
            definition=str(result["definition"]),
            variant=result["variant"],
            owner=result["owner"],
            provider=result["provider"],
            source_type=result["source_type"],
            transformation=result["transformation"],
            description=result["description"],
            tags=json.loads(result["tags"]) if result["tags"] else [],
            properties=json.loads(result["properties"]) if result["properties"] else {},
            status=result["status"],
            inputs=name_variant_list,
        )

    def get_sources(self) -> List[Source]:
        source_rows = self.db.query_resource("sources")
        sources = []
        for source_row in source_rows:
            variants = [
                r["variant"]
                for r in self.db.query_resource(
                    "source_variant", "name", source_row["name"]
                )
            ]
            sources.append(
                Source(
                    name=source_row["name"],
                    default_variant=source_row["default_variant"],
                    variants=variants,
                )
            )

        return sources

    def get_source_variant_text(self, name: str, variant: str) -> str:
        result = self.db.get_source_variant_text(name, variant)
        return result

    def get_training_set_variant(self, name: str, variant: str) -> TrainingSetVariant:
        result = self.db.get_training_set_variant(name, variant)
        ts_feature_rows = self.db.get_training_set_features(name, variant)
        feature_name_variants = [
            (r["feature_name"], r["feature_variant"]) for r in ts_feature_rows
        ]
        return TrainingSetVariant(  # does local mode use provider for TS
            created=result["created"],
            name=result["name"],
            variant=result["variant"],
            owner=result["owner"],
            label=(result["label_name"], result["label_variant"]),
            features=feature_name_variants,
            description=result["description"],
            tags=json.loads(result["tags"]) if result["tags"] else [],
            properties=json.loads(result["properties"]) if result["properties"] else {},
            status=result["status"],
        )

    def get_training_set(self, name: str) -> TrainingSet:
        result = self.db.get_training_set(name, should_fetch_tags_properties=False)
        return TrainingSet(
            name=result["name"],
            default_variant=result["default_variant"],
            variants=[
                r["variant"]
                for r in self.db.query_resource(
                    "training_set_variant", "name", result["name"]
                )
            ],
        )

    def get_training_sets(self) -> List[TrainingSet]:
        training_set_rows = self.db.query_resource("training_sets")
        training_sets = []
        for ts_row in training_set_rows:
            variants = [
                r["variant"]
                for r in self.db.query_resource(
                    "training_set_variant", "name", ts_row["name"]
                )
            ]
            training_sets.append(
                TrainingSet(
                    name=ts_row["name"],
                    default_variant=ts_row["default_variant"],
                    variants=variants,
                )
            )

        return training_sets

    def get_entity(self, name: str) -> Entity:
        entity_row = self.db.get_entity(name, should_fetch_tags_properties=True)
        return Entity(
            name=entity_row["name"],
            description=entity_row["description"],
            status=entity_row["status"],
            tags=json.loads(entity_row["tags"]) if entity_row["tags"] else [],
            properties=json.loads(entity_row["properties"])
            if entity_row["properties"]
            else {},
        )

    def get_entities(self) -> List[Entity]:
        entity_rows = self.db.query_resource(
            "entities", should_fetch_tags_properties=True
        )
        return [
            Entity(
                name=row["name"],
                description=row["description"],
                status=row["status"],
                tags=json.loads(row["tags"]) if row["tags"] else [],
                properties=json.loads(row["properties"]) if row["properties"] else {},
            )
            for row in entity_rows
        ]

    def get_provider(self, name: str) -> Provider:
        result = self.db.get_provider(name, should_fetch_tags_properties=True)
        return Provider(
            name=result["name"],
            description=result["description"],
            team=result["team"],
            config=LocalConfig(),
            function="",  # look into this
            status=result["status"],
            tags=json.loads(result["tags"]) if result["tags"] else [],
            properties=json.loads(result["properties"]) if result["properties"] else {},
        )

    def get_providers(self) -> List[Provider]:
        provider_rows = self.db.query_resource(
            "providers", should_fetch_tags_properties=True
        )
        return [
            Provider(
                name=row["name"],
                description=row["description"],
                team=row["team"],
                config=LocalConfig(),
                function="",  # look into this
                status=row["status"],
                tags=json.loads(row["tags"]) if row["tags"] else [],
                properties=json.loads(row["properties"]) if row["properties"] else {},
            )
            for row in provider_rows
        ]

    def get_user(self, name: str) -> User:
        result = self.db.get_user(name, should_fetch_tags_properties=True)
        return User(
            name=result["name"],
            status=result["status"],
            tags=json.loads(result["tags"]) if result["tags"] else [],
            properties=json.loads(result["properties"]) if result["properties"] else {},
        )

    def get_users(self) -> List[User]:
        user_rows = self.db.query_resource("users", should_fetch_tags_properties=True)
        return [
            User(
                name=row["name"],
                status=row["status"],
                tags=json.loads(row["tags"]) if row["tags"] else [],
                properties=json.loads(row["properties"]) if row["properties"] else {},
            )
            for row in user_rows
        ]

    def get_tags_for_resource(self, name, variant, resource_type) -> List[str]:
        row_data = self.db.get_tags(
            name=name, variant=variant, resource_type=resource_type
        )
        if len(row_data):
            return row_data[0][0]
        else:
            return []

    def get_training_set_features(
        self, name: str, variant: str
    ) -> List[TrainingSetFeatures]:
        db_result = self.db.get_training_set_features(name=name, variant=variant)
        tsf_list = []
        for row in db_result:
            tsf_list.append(
                TrainingSetFeatures(
                    training_set_name=row["training_set_name"],
                    training_set_variant=row["training_set_variant"],
                    feature_name=row["feature_name"],
                    feature_variant=row["feature_variant"],
                )
            )

        return tsf_list

    def get_feature_variants_from_source(
        self, source_name: str, source_variant: str
    ) -> List[FeatureVariant]:
        db_result = self.db.get_feature_variants_from_source(
            name=source_name, variant=source_variant
        )
        feature_variant_list = []
        for row in db_result:
            feature_variant_list.append(
                FeatureVariant(
                    created=row["created"],
                    name=row["name"],
                    variant=row["variant"],
                    source=(row["source_name"], row["source_variant"]),
                    value_type=row["data_type"],
                    is_embedding=bool(row["is_embedding"]),
                    dims=row["dimension"],
                    entity=row["entity"],
                    owner=row["owner"],
                    provider=row["provider"],
                    status=row["status"],
                    location=ResourceColumnMapping(
                        row["source_entity"],
                        row["source_value"],
                        row["source_timestamp"],
                    ),
                    description=row["description"],
                    tags=json.loads(row["tags"]) if row["tags"] else [],
                    properties=json.loads(row["properties"])
                    if row["properties"]
                    else {},
                )
            )

        return feature_variant_list

    def get_training_set_from_features(
        self, feature_name: str, feature_variant: str
    ) -> List[TrainingSetFeatures]:
        db_result = self.db.get_training_set_from_features(
            name=feature_name, variant=feature_variant
        )
        tsf_list = []
        for row in db_result:
            tsf_list.append(
                TrainingSetFeatures(
                    training_set_name=row["training_set_name"],
                    training_set_variant=row["training_set_variant"],
                    feature_name=row["feature_name"],
                    feature_variant=row["feature_variant"],
                )
            )

        return tsf_list

    def get_label_variants_from_source(
        self, source_name: str, source_variant: str
    ) -> List[LabelVariant]:
        db_result = self.db.get_label_variants_from_source(
            name=source_name, variant=source_variant
        )
        label_variant_list = []
        for row in db_result:
            label_variant_list.append(
                LabelVariant(
                    name=row["name"],
                    variant=row["variant"],
                    source=(row["source_name"], row["source_variant"]),
                    value_type=row["data_type"],
                    created=row["created"],
                    entity=row["entity"],
                    status=row["status"],
                    owner=row["owner"],
                    provider=row["provider"],
                    location=ResourceColumnMapping(
                        row["source_entity"],
                        row["source_value"],
                        row["source_timestamp"],
                    ),
                    description=row["description"],
                    tags=json.loads(row["tags"]) if row["tags"] else [],
                    properties=json.loads(row["properties"])
                    if row["properties"]
                    else {},
                )
            )

        return label_variant_list

    def get_training_set_variant_from_label(
        self, label_name: str, label_variant: str
    ) -> List[TrainingSetVariant]:
        db_result = self.db.get_training_set_variant_from_label(
            name=label_name, variant=label_variant
        )
        training_set_variant_list = []
        for row in db_result:
            ts_feature_rows = self.db.get_training_set_features(
                row["name"], row["variant"]
            )
            feature_name_variants = [
                (r["feature_name"], r["feature_variant"]) for r in ts_feature_rows
            ]
            training_set_variant_list.append(
                TrainingSetVariant(
                    created=row["created"],
                    name=row["name"],
                    variant=row["variant"],
                    owner=row["owner"],
                    label=(row["label_name"], row["label_variant"]),
                    features=feature_name_variants,
                    description=row["description"],
                    tags=json.loads(row["tags"]) if row["tags"] else [],
                    properties=json.loads(row["properties"])
                    if row["properties"]
                    else {},
                    status=row["status"],
                )
            )

        return training_set_variant_list
