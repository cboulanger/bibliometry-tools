import os
import sys
from email.utils import parseaddr
from src.openalex import OpenAlexToNeo4J
from dotenv import load_dotenv
from src.graphdb import GraphDb
import json

if __name__ == "__main__":
    load_dotenv()

    _, email = parseaddr(os.environ.get("OPENALEX_EMAIL"))
    if not email:
        raise ValueError("Please define the environment variable OPENALEX_EMAIL to contain your email address")
    graphdb = GraphDb(uri=os.environ.get("NEO4J_URI"),
                      user=os.environ.get("NEO4J_USER"),
                      password=os.environ.get("NEO4J_PASSWORD"),
                      database=os.environ.get("NEO4J_DB") or None)
    importer = OpenAlexToNeo4J(email, verbose=True, graphdb=graphdb)
    issn = sys.argv[1]
    cache_path = f"data/{issn}.json"
    if os.path.exists(cache_path):
        with open(cache_path) as cache_file:
            all_items = json.load(cache_file)
    else:
        print(f"Retrieving all items for ISSN {issn} ...")
        all_items = importer.get_multiple_entities("work", f"host_venue.issn:{issn}")
        with open(cache_path, "w") as cache_file:
            json.dump(all_items, cache_file)
    count = len(all_items)
    print(f"Importing {count} items...\n\n")
    while len(all_items):
        work_data = all_items.pop(0)
        print("=" * 80)
        print("Importing " + work_data['title'] + "...")
        try:
            importer.import_work(work_data)
            with open(cache_path, "w") as cache_file:
                json.dump(all_items, cache_file)
        except Exception as err:
            print("ERROR: " + str(err))
