import pkg_resources
import requests
import threading

version_check_url = "https://version.featureform.com"


def get_package_version():
    return pkg_resources.get_distribution("featureform").version

def check_up_to_date(local, client):
    download_thread = threading.Thread(target=run_version_check, name="Downloader", args=(local, client))
    download_thread.start()

def run_version_check(local, client):
    try:
        version = get_package_version()
        requests.get(version_check_url, params={"local": local, "client": client, "version": version})
    except:
        pass