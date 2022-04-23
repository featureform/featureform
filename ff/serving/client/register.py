# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

from resources import ResourceState, Provider, RedisConfig, PostgresConfig, SnowflakeConfig, User, Location, PrimaryData, Table


class UserRegistrar:

    def __init__(self, registrar, user):
        self.__registrar = registrar
        self.__user = user

    def name(self) -> str:
        return self.__user.name


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
                       owner: str | UserRegistrar,
                       description: str = ""):
        self.__registrar.register_primary_data(name=name,
                                               variant=variant,
                                               location=Table(table),
                                               owner=owner,
                                               provider=self.name(),
                                               description=description)


class Registrar:

    def __init__(self):
        self.__state = ResourceState()

    def register_user(self, name: str) -> UserRegistrar:
        user = User(name)
        self.__state.add(user)
        return UserRegistrar(self, user)

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
                              owner: str | UserRegistrar,
                              provider: str | OfflineProvider,
                              description: str = ""):
        if not isinstance(owner, str):
            owner = owner.name()
        if not isinstance(provider, str):
            provider = provider.name()
        primary_data = PrimaryData(name=name,
                                   variant=variant,
                                   location=location,
                                   owner=owner,
                                   provider=provider,
                                   description=description)
        self.__state.add(primary_data)


global_registrar = Registrar()
register_user = global_registrar.register_user
register_redis = global_registrar.register_redis
register_snowflake = global_registrar.register_snowflake
register_postgres = global_registrar.register_postgres
