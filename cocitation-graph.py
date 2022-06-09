import os
from src.openalex import OpenAlexWork, OpenAlexCreator
from src.cocitation import Cocitation
from dotenv import load_dotenv
from src.graphdb import GraphDb

if __name__ == "__main__":
    load_dotenv()
    graphdb = GraphDb(uri=os.environ.get("NEO4J_URI"),
                      user=os.environ.get("NEO4J_USER"),
                      password=os.environ.get("NEO4J_PASSWORD"),
                      database=os.environ.get("NEO4J_DB") or None)

    cocitation = Cocitation(graphdb, OpenAlexWork(), OpenAlexCreator())
    cocitation.calculate()


