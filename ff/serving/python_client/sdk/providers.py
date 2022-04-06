providers_to_register = []

class Provider:
    def __init__(self, name = str, provider_type = str, host = str, db = int, ssl = bool, client_certificate= str, bucket = str):
        self.name = name
        self.provider_type = provider_type
        self.host = host
        self.db = db
        self.ssl = ssl
        self.client_certificate = client_certificate
        self.bucket = bucket
      

def register_provider(name, provider_type, host, db, ssl, client_certificate, bucket):
    provider = Provider(name, provider_type, host, db, ssl, client_certificate, bucket)
    providers_to_register.append(provider)

def register_redis(*args, **kwargs):
    register_provider(kwargs['name'], 'Redis', kwargs['host'], kwargs['db'], kwargs['ssl'], kwargs['client_certificate'], kwargs['bucket'])

def register_bigquery(*args, **kwargs):
    register_provider(kwargs['name'], 'BigQuery', kwargs['host'], kwargs['db'], kwargs['ssl'], kwargs['client_certificate'], kwargs['bucket'])

def register_cassandra(*args, **kwargs):
    register_provider(kwargs['name'], 'Cassandra', kwargs['host'], kwargs['db'], kwargs['ssl'], kwargs['client_certificate'], kwargs['bucket'])

def register_postgres(*args, **kwargs):
    register_provider(kwargs['name'], 'Postgres', kwargs['host'], kwargs['db'], kwargs['ssl'], kwargs['client_certificate'], kwargs['bucket'])

def register_mysql(*args, **kwargs):
    register_provider(kwargs['name'], 'MySQL', kwargs['host'], kwargs['db'], kwargs['ssl'], kwargs['client_certificate'], kwargs['bucket'])

def register_deltalake(*args, **kwargs):
    register_provider(kwargs['name'], 'DeltaLake', kwargs['host'], kwargs['db'], kwargs['ssl'], kwargs['client_certificate'], kwargs['bucket'])

def register_s3(*args, **kwargs):
    register_provider(kwargs['name'], 'S3', kwargs['host'], kwargs['db'], kwargs['ssl'], kwargs['client_certificate'],kwargs['bucket'])

def register_spark(*args, **kwargs):
    register_provider(kwargs['name'], 'Spark', kwargs['host'], kwargs['db'], kwargs['ssl'], kwargs['client_certificate'], kwargs['bucket'])

def register_flink(*args, **kwargs):
    register_provider(kwargs['name'], 'Flink', kwargs['host'], kwargs['db'], kwargs['ssl'], kwargs['client_certificate'], kwargs['bucket'])


register_redis(
    name = "demo-redis",
    host = "https://demo-redis-env.alias",
    db = 1,
    ssl = True,
    client_certificate = "~/.demo/redis-cert.pem",
    bucket = ""
)





