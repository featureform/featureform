from .format import *
from .sqlite_metadata import *

cutoff_length = 25

def search_local(phrase):
    db = SQLiteMetadata()
    results = db.search(phrase=phrase)

    if len(results) == 0:
        print(f"Search phrase {phrase} returned no results.")
    else:
        format_rows("NAME", "VARIANT", "TYPE")
        for r in results:
            desc = r["description"][:cutoff_length] + "..." if len(r["description"]) > 0 else "" 
            format_rows(r["name"], r["variant"], r["resource_type"])
    
    return results
