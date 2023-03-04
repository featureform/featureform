import os
import requests
from .format import *


def search(phrase):
    host = os.getenv('FEATUREFORM_HOST')
    if host is None:
        raise RuntimeError("Host not found.")
    
    response = requests.get(f"http://{host}/search?q={phrase}")

    if response.status_code is not 200:
        print(f"Search request for {phrase} resulted in HTTP status {response.status_code}")
        return
    
    results = response.json()
    
    if len(results) == 0:
        print(f"Search phrase {phrase} returned no results.")
    else:
        format_rows("NAME", "VARIANT", "TYPE")
        for r in results:
            format_rows(r["Name"], r["Variant"], r["Type"])
    
    return results
