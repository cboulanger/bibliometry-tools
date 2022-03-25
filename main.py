from neo4j import GraphDatabase
from urllib.parse import quote
import requests
import json


class GraphDb:

    def __init__(self, uri, user, password, database):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def close(self):
        self.driver.close()

    def merge_node(self, node_type: str, node_data: dict):
        with self.driver.session(database=self.database) as session:
            return session.write_transaction(self._merge_node_transaction, node_type, node_data)

    def merge_relationship(self, source_node_selector: str, target_node_selector: str, relationship: str):
        with self.driver.session(database=self.database) as session:
            return session.write_transaction(self._merge_relationship_transaction, source_node_selector,
                                             target_node_selector, relationship)

    @staticmethod
    def _merge_node_transaction(tx, node_type: str, node_data: dict):
        if not node_type:
            raise RuntimeError("No type given")
        if not len(node_data.items()):
            raise RuntimeError("No data")
        set_statements = []
        for field, value in node_data.items():
            if value is None:
                continue
            if type(value) is int or type(value) is float or type(value) is str:
                set_statements.append(f"a.{field} = " + json.dumps(value))
            else:
                # ignore other types since they need to be modeled as relationships
                pass
        set_expr = ",\n    ".join(set_statements)
        id_field = node_type.lower() + "Id"
        item_id = node_data[id_field]
        query = f"MERGE (a:{node_type} {{{id_field}:'{item_id}'}}) \nSET {set_expr} \nRETURN id(a)"
        print(query)
        result = tx.run(query)
        return result.single()[0]

    @staticmethod
    def _merge_relationship_transaction(tx, source_node_selector: str, target_node_selector: str, relationship: str):
        if not source_node_selector or not target_node_selector or not relationship:
            raise RuntimeError("Invalid arguments")
        query = f"MATCH (a:{source_node_selector}), (b:{target_node_selector}) MERGE (a)-[r:{relationship}]->(b) RETURN id(r)"
        print(query)
        result = tx.run(query)
        return result.single()[0]


class SemanticScholar:
    baseUri = "https://api.semanticscholar.org/graph/v1"

    def _getPaperFields(self, references=False, citations=False) -> []:
        fields = ["paperId",
                  "url",
                  "title",
                  "abstract",
                  "venue",
                  "year",
                  "referenceCount",
                  "citationCount",
                  "influentialCitationCount",
                  "isOpenAccess",
                  "fieldsOfStudy",
                  "s2FieldsOfStudy",
                  "authors"]
        if references:
            fields.append("references")
        if citations:
            fields.append("citations")

        return ",".join(fields)

    def _getAuthorFields(self, papers=False):
        fields = ["authorId",
                  "externalIds",
                  "url",
                  "name",
                  "aliases",
                  "affiliations",
                  "homepage",
                  "paperCount",
                  "citationCount",
                  "hIndex"]
        if papers:
            fields.append("papers")
        return ",".join(fields)

    def _getUrl(self, type, item_id=None, query=None, papers=False, references=False, citations=False):
        url = ""
        if type == "paper":
            fields = self._getPaperFields(references=references, citations=citations)
        elif type == "author":
            fields = self._getAuthorFields(papers=papers)
        else:
            raise RuntimeError("Missing type")
        if query is not None:
            url = f"{self.baseUri}/{type}/search?query={quote(query)}&fields={fields}"
        elif item_id is not None:
            url = f"{self.baseUri}/{type}/{item_id}?fields={fields}"
        else:
            raise RuntimeError("Invalid arguments")
        return url

    def _retrieve_all(self, url):
        offset = 0
        total = 0
        limit = 100
        data = []
        while offset <= total:
            paged_url = url + f"&offset={offset}&limit={limit}"
            result = requests.get(paged_url).json()
            if result["total"] == 0:
                break
            data.extend(result["data"])
            offset += limit
        return data

    def paper(self, query=None, item_id=None, citations=False, references=False):
        url = self._getUrl(type="paper", item_id=item_id, query=query, citations=citations, references=references)
        return requests.get(url).json() if item_id else self._retrieve_all(url)

    def author(self, query=None, item_id=None, papers=False):
        url = self._getUrl(type="author", item_id=item_id, query=query, papers=papers)
        return requests.get(url).json() if item_id else self._retrieve_all(url)


if __name__ == "__main__":
    # neo4j database API object
    neo4jdb = GraphDb(uri="bolt://localhost:7687", user="neo4j", password="lts2021", database="semanticscholar")
    # semantic scholar API object
    semschol = SemanticScholar()
    # query an author
    authors = semschol.author(query="Christian Boulanger", papers=True)
    paper_count = 0


    def create_id_selector(item):
        if item.get("paperId"):
            return "Paper {paperId:'" + item.get("paperId") + "'}"
        elif item.get("authorId"):
            return "Author {authorId:'" + item.get("authorId") + "'}"
        raise RuntimeError("No paperId or authorId")


    for author in authors:
        # save author node
        neo4jdb.merge_node("Author", author)
        # mark as same (this needs to be manually edited later to remove false positives)
        for same_person in authors:
            if same_person.get("authorId") != author.get("authorId"):
                neo4jdb.merge_relationship(
                    source_node_selector=create_id_selector(author),
                    target_node_selector=create_id_selector(same_person),
                    relationship="IS_SAME_AS"
                )
        # merge and connect papers
        papers = author.get("papers")
        for paper in papers:
            paper_data = semschol.paper(item_id=paper.get("paperId"))
            neo4jdb.merge_node("Paper", paper_data)
            neo4jdb.merge_relationship(
                source_node_selector=create_id_selector(author),
                target_node_selector=create_id_selector(paper),
                relationship="IS_CREATOR_OF"
            )
            paper_count += 1

    print("Merged", str(len(authors)), "authors and", str(paper_count), "papers")
    neo4jdb.close()
