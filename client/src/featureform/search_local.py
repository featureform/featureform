from .format import *
from .sqlite_metadata import *

cutoff_length = 25

def search_local(phrase):
    db = SQLiteMetadata()
    results = db.search(phrase=phrase)

    if len(results) == 0:
        print(f"Search phrase {phrase} returned no results.")
    else:
        format_rows("RESOURCE TYPE", "NAME", "VARIANT", "DESCRIPTION", "STATUS")
        for r in results:
            desc = r["description"][:cutoff_length] + "..." if len(r["description"]) > 0 else "" 
            format_rows(r["resource_type"], r["name"], r["variant"], desc, r["status"])
    
    return results
