from .format import *
from .sqlite_metadata import *

cutoff_length = 25

def search_local(phrase):
    db = SQLiteMetadata()
    results = db.search(phrase=phrase)

    if len(results) == 0:
        print(f"Search phrase {phrase} returned no results.")
        return []
    else:
        return results
