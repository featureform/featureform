from typeguard import typechecked
from dataclasses import dataclass

import json


@typechecked
@dataclass
class PineconeConfig:
    project_id: str = ""
    environment: str = ""
    api_key: str = ""

    def software(self) -> str:
        return "pinecone"

    def type(self) -> str:
        return "PINECONE_ONLINE"

    def serialize(self) -> bytes:
        config = {
            "ProjectID": self.project_id,
            "Environment": self.environment,
            "ApiKey": self.api_key,
        }
        return bytes(json.dumps(config), "utf-8")

    def deserialize(self, config):
        config = json.loads(config)
        self.project_id = config["ProjectID"]
        self.environment = config["Environment"]
        self.api_key = config["ApiKey"]
        return self


@typechecked
@dataclass
class WeaviateConfig:
    url: str = ""
    api_key: str = ""

    def software(self) -> str:
        return "weaviate"

    def type(self) -> str:
        return "WEAVIATE_ONLINE"

    def serialize(self) -> bytes:
        if self.url == "":
            raise Exception("URL cannot be empty")
        config = {
            "URL": self.url,
            "ApiKey": self.api_key,
        }
        return bytes(json.dumps(config), "utf-8")

    def deserialize(self, config):
        config = json.loads(config)
        self.url = config["URL"]
        self.api_key = config["ApiKey"]
        return self
