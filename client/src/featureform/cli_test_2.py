import featureform as ff

r =ff.register_redis(
name="redis-quickstart",
host="quickstart-redis",  # The internal dns name for redis
port=6379,
description="A Redis deployment we created for the Featureform quickstart"
)

p = ff.register_postgres(
name="postgres-quickstart",
host="quickstart-postgres",  # The internal dns name for postgres
port="5432",
user="postgres",
password="password",
database="postgres",
description="A Postgres deployment we created for the Featureform quickstart"
)

