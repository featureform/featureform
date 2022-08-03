import grpc
import os


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
