providers_to_register = []

def register_db(name = "", provider_type = "", host = "", db= None, ssl= "", client_certificate = ""):
    provider = {"name": name, "provider_type": provider_type, "host": host, "db": db, "ssl" : ssl, "client_certificate" : client_certificate}
    providers_to_register.append(provider)


def register_redis(name = "", host = "", db = None, ssl= "", client_certificate = ""):
    register_db(name, "redis", host, db, ssl, client_certificate)

def register_bigquery(name = "", host = "", db= None, ssl= "", client_certificate = ""):
    register_db(name, "bigquery", host, db, ssl, client_certificate)

def register_cassandra(name = "", host = "", db= None, ssl= "", client_certificate = ""):
    register_db(name, "cassandra", host, db, ssl, client_certificate)

def register_postgres(name = "", host = "", db= None, ssl= "", client_certificate = ""):
    register_db(name, "postgres", host, db, ssl, client_certificate)

def register_mysql(name = "", host = "", db= None, ssl= "", client_certificate = ""):
    register_db(name, "mysql", host, db, ssl, client_certificate)

def register_deltalake(name = "", host = "", db= None, ssl= "", client_certificate = ""):
    register_db(name, "deltalake", host, db, ssl, client_certificate)

def register_s3(name = "", host = "", db= None, ssl= "", client_certificate = "", bucket= ""):
    provider = {"name": name, "provider_type": "s3", "host": host, "db": db, "ssl" : ssl, "client_certificate" : client_certificate, "bucket" : bucket}
    providers_to_register.append(provider)

def register_spark(name = "", host = "", ssl= "", client_certificate = ""):
    provider = {"name": name, "provider_type": "spark", "host": host, "ssl" : ssl, "client_certificate" : client_certificate}
    providers_to_register.append(provider)

def register_flink(name = "", host = "", ssl= "", client_certificate = ""):
    provider = {"name": name, "provider_type": "flink", "host": host, "ssl" : ssl, "client_certificate" : client_certificate}
    providers_to_register.append(provider)





