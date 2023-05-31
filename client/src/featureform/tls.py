import grpc
import os
import requests
import json

insecure_protocol = "http://"
secure_protocol = "https://"


def insecure_channel(host):
    return grpc.insecure_channel(host, options=(("grpc.enable_http_proxy", 0),))


def secure_channel(host, cert_path):
    cert_path = cert_path or os.getenv("FEATUREFORM_CERT")
    if cert_path:
        with open(cert_path, "rb") as f:
            credentials = grpc.ssl_channel_credentials(f.read())
    else:
        credentials = grpc.ssl_channel_credentials()
    channel = grpc.secure_channel(host, credentials)
    return channel


def fetch_cluster_version(version_url=""):
    requests.packages.urllib3.disable_warnings()
    res = requests.get(url=version_url, verify=False)
    response = json.loads(res.text)
    return response["version"]


def get_version_local():
    local_port = os.getenv("LOCALMODE_DASHBOARD_PORT", 3000)
    version_url = "localhost:{}/data/version".format(local_port)
    return fetch_cluster_version(insecure_protocol + version_url)


def get_version_hosted(host):
    cluster_version = ""
    versionUrl = "{}/data/version".format(host)
    if host.__contains__(":443"):
        cluster_version = fetch_cluster_version(secure_protocol + versionUrl)
    else:
        cluster_version = fetch_cluster_version(insecure_protocol + versionUrl)
    return cluster_version
