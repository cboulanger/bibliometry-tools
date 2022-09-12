import os
import sys
from email.utils import parseaddr
from dotenv import load_dotenv
from src.graphdb import GraphDb
from src.openalex import OpenAlexToNeo4J

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

    oa_id = sys.argv[1]
    all_items = importer.get_multiple_entities("work", f"cites:{oa_id}")
    print(f"Importing {len(all_items)} citing items...\n\n")
    while len(all_items):
        work_data = all_items.pop(0)
        print("=" * 80)
        print("Importing " + str(work_data['title']) + "...")
        try:
            importer.import_work(work_data)
        except Exception as err:
            print("ERROR: " + str(err))
