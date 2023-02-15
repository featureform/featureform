import grpc
import os
import pkg_resources
import http3

version_check_url = "https://version.featureform.com"


def insecure_channel(host):
    return grpc.insecure_channel(host, options=(('grpc.enable_http_proxy', 0),))


def secure_channel(host, cert_path):
    cert_path = cert_path or os.getenv('FEATUREFORM_CERT')
    if cert_path:
        with open(cert_path, 'rb') as f:
            credentials = grpc.ssl_channel_credentials(f.read())
    else:
        credentials = grpc.ssl_channel_credentials()
    channel = grpc.secure_channel(host, credentials)
    return channel


async def check_up_to_date(local, client):
    try:
        c = http3.AsyncClient()
        version = pkg_resources.get_distribution("featureform").version
        await c.get(version_check_url, params={"local": local, "client": client, "version": version})
    except:
        pass
