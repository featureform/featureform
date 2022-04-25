# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

from resources import ResourceState, Provider, RedisConfig, PostgresConfig, SnowflakeConfig, User, Location, Source, PrimaryData, SQLTable, SQLTransformation, Entity
from typing import Tuple, Callable, TypedDict, List
from typeguard import typechecked

NameVariant = Tuple[str, str]


class EntityRegistrar:

    def __init__(self, registrar, entity):
        self.__registrar = registrar
        self.__entity = entity

    def name() -> str:
        return self.__entity.name


class UserRegistrar:

    def __init__(self, registrar, user):
        self.__registrar = registrar
        self.__user = user

    def name(self) -> str:
        return self.__user.name

    def make_default_owner(self):
        self.__registrar.set_default_owner(self.name())


class OfflineProvider:

    def __init__(self, registrar, provider):
        self.__registrar = registrar
        self.__provider = provider

    def name(self) -> str:
        return self.__provider.name


class OfflineSQLProvider(OfflineProvider):

    def __init__(self, registrar, provider):
        super().__init__(registrar, provider)
        self.__registrar = registrar
        self.__provider = provider

    def register_table(self,
                       name: str,
                       variant: str,
                       table: str,
                       owner: str | UserRegistrar = "",
                       description: str = ""):
        self.__registrar.register_primary_data(name=name,
                                               variant=variant,
                                               location=SQLTable(table),
                                               owner=owner,
                                               provider=self.name(),
                                               description=description)

    def sql_transformation(self,
                           variant: str,
                           owner: str | UserRegistrar = "",
                           name: str = "",
                           description: str = ""):
        return self.__registrar.sql_transformation(name=name,
                                                   variant=variant,
                                                   owner=owner,
                                                   provider=self.name(),
                                                   description=description)


class OnlineProvider:

    def __init__(self, registrar, provider):
        self.__registrar = registrar
        self.__provider = provider

    def name(self) -> str:
        return self.__provider.name


class SQLTransformationDecorator:

    def __init__(self,
                 variant: str,
                 owner: str,
                 provider: str,
                 name: str = "",
                 description: str = ""):
        self.name = name
        self.variant = variant
        self.owner = owner
        self.provider = provider
        self.description = description

    def __call__(self, fn: Callable[[], str]):
        if self.description == "":
            self.description = fn.__doc__
        if self.name == "":
            self.name = fn.__name__
        self.__set_query(fn())

    @typechecked
    def __set_query(self, query: str):
        if query == "":
            raise ValueError("Query cannot be an empty string")
        self.query = query

    def to_source(self) -> Source:
        return Source(
            name=self.name,
            variant=self.variant,
            definition=SQLTransformation(self.query),
            owner=self.owner,
            provider=self.provider,
            description=self.description,
        )


class SourceRegistrar:

    def __init__(self, registrar, source):
        self.__registrar = registrar
        self.__source = source

    def id() -> NameVariant:
        return (self.__source.name, self.__source.variant)


class ColumnMapping(TypedDict):
    name: str
    variant: str
    column: str


class Registrar:

    def __init__(self):
        self.__state = ResourceState()
        self.__decorators = []
        self.__default_user = ""

    def register_user(self, name: str) -> UserRegistrar:
        user = User(name)
        self.__state.add(user)
        return UserRegistrar(self, user)

    def set_default_owner(self, user: str):
        self.__default_user = user

    def default_user(self) -> str:
        return self.__default_user

    def register_redis(self,
                       name: str,
                       description: str = "",
                       team: str = "",
                       host: str = "0.0.0.0",
                       port: int = 6379,
                       password: str = "",
                       db: int = 0):
        config = RedisConfig(host=host, port=port, password=password, db=db)
        provider = Provider(name=name,
                            function="ONLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__state.add(provider)
        return OnlineProvider(self, provider)

    def register_snowflake(
        self,
        name: str,
        username: str,
        password: str,
        account: str,
        organization: str,
        database: str,
        schema: str = "PUBLIC",
        description: str = "",
        team: str = "",
    ):
        config = SnowflakeConfig(account=account,
                                 database=database,
                                 organization=organization,
                                 username=username,
                                 password=password,
                                 schema=schema)
        provider = Provider(name=name,
                            function="OFFLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__state.add(provider)
        return OfflineSQLProvider(self, provider)

    def register_postgres(self,
                          name: str,
                          description: str = "",
                          team: str = "",
                          host: str = "0.0.0.0",
                          port: int = 5432,
                          user: str = "postgres",
                          password: str = "password",
                          database: str = "postgres"):
        config = PostgresConfig(host=host,
                                port=port,
                                database=database,
                                user=user,
                                password=password)
        provider = Provider(name=name,
                            function="OFFLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__state.add(provider)
        return OfflineSQLProvider(self, provider)

    def register_primary_data(self,
                              name: str,
                              variant: str,
                              location: Location,
                              provider: str | OfflineProvider,
                              owner: str | UserRegistrar = "",
                              description: str = ""):
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.default_user()
        if not isinstance(provider, str):
            provider = provider.name()
        source = Source(name=name,
                        variant=variant,
                        definition=PrimaryData(location=location),
                        owner=owner,
                        provider=provider,
                        description=description)
        self.__state.add(source)
        return SourceRegistrar(self, source)

    def register_sql_transformation(self,
                                    name: str,
                                    variant: str,
                                    query: str,
                                    provider: str | OfflineProvider,
                                    owner: str | UserRegistrar = "",
                                    description: str = ""):
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.default_user()
        if not isinstance(provider, str):
            provider = provider.name()
        source = Source(
            name=name,
            variant=variant,
            definition=SQLTransformation(query),
            owner=owner,
            provider=provider,
            description=description,
        )
        self.__state.add(source)
        return SourceRegistrar(self, source)

    def sql_transformation(self,
                           variant: str,
                           owner: str | UserRegistrar,
                           provider: str | OfflineProvider,
                           name: str = "",
                           description: str = ""):
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.default_user()
        if not isinstance(provider, str):
            provider = provider.name()
        decorator = SQLTransformationDecorator(
            name=name,
            variant=variant,
            provider=provider,
            owner=owner,
            description=description,
        )
        self.__decorators.append(decorator)
        return decorator

    def state(self):
        while len(self.__decorators) > 0:
            decorator = self.__decorators.pop()
            self.__state.add(decorator.to_source())
        return self.__state

    def register_entity(self, name: str, description: str = ""):
        entity = Entity(name=name, description=description)
        self.__state.add(entity)
        return EntityRegistrar(self, entity)

    def register_resources(
        self,
        name: str,
        source: NameVariant | SourceRegistrar | SQLTransformationDecorator,
        entity: str | EntityRegistrar,
        entity_column: str,
        features: List[ColumnMapping],
        labels: List[ColumnMapping],
        timestamp_column: str,
        inference_store: str | OnlineProvider,
    ):
        pass


global_registrar = Registrar()
register_user = global_registrar.register_user
register_redis = global_registrar.register_redis
register_snowflake = global_registrar.register_snowflake
register_postgres = global_registrar.register_postgres
