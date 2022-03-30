import os
from neo4j import GraphDatabase
from urllib.parse import quote
import requests
import json
from typing import Union
import time
import pickle
from email.utils import parseaddr

class GraphDb:

    def __init__(self, uri, user, password, database):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def close(self):
        self.driver.close()

    def merge_node(self, node_type: str, node_data: dict):
        with self.driver.session(database=self.database) as session:
            return session.write_transaction(self._merge_node_transaction, node_type, node_data)

    def merge_relationship(self,
                           source_node_selector: Union[str, int, tuple],
                           target_node_selector: Union[str, int, tuple],
                           relationship: str):
        with self.driver.session(database=self.database) as session:
            return session.write_transaction(self._merge_relationship_transaction, source_node_selector,
                                             target_node_selector, relationship)

    def get_node(self, identifier):
        with self.driver.session(database=self.database) as session:
            return session.read_transaction(self._get_node_tx, identifier)

    @staticmethod
    def _merge_node_transaction(tx, node_type: str, node_data: dict) -> int:
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
        query = f"MERGE (a:{node_type} {{id:'{node_data['id']}'}}) \nSET {set_expr} \nRETURN id(a)"
        #print(query)
        try:
            result = tx.run(query).single()
        except Exception as err:
            raise RuntimeError(f"Query failed:\nQuery: {query}\nError: {err}")
        node_id = int(result[0])
        return node_id

    @staticmethod
    def _merge_relationship_transaction(tx,
                                        source_node_selector: Union[str, int, tuple],
                                        target_node_selector: Union[str, int, tuple],
                                        relationship: str):
        if not source_node_selector or not target_node_selector or not relationship:
            raise RuntimeError("Invalid arguments")
        if type(source_node_selector) is tuple:
            node_label, id_property, id_value = source_node_selector
            id_value = json.dumps(id_value)
            source_node_selector = f"{node_label} {{{id_property}:{id_value}}}"
        if type(target_node_selector) is tuple:
            node_label, id_property, id_value = target_node_selector
            id_value = json.dumps(id_value)
            target_node_selector = f"{node_label} {{{id_property}:{id_value}}}"
        if type(source_node_selector) is str and type(target_node_selector) is str:
            query = f"MATCH (a:{source_node_selector}), (b:{target_node_selector})"
        elif type(source_node_selector) is str and type(target_node_selector) is int:
            query = f"MATCH (a:{source_node_selector}), (b) WHERE id(b)={target_node_selector}"
        elif type(source_node_selector) is int and type(target_node_selector) is str:
            query = f"MATCH (a), (b:{target_node_selector}) WHERE id(a)={source_node_selector})"
        elif type(source_node_selector) is int and type(target_node_selector) is int:
            query = f"MATCH (a),(b) WHERE id(a)={source_node_selector} and id(b)={target_node_selector}"
        else:
            raise ValueError("Invalid values for source and/or target selector")
        query += f" MERGE (a)-[r:{relationship}]->(b) RETURN id(r)"
        #print(query)
        try:
            result = tx.run(query).single()
        except Exception as err:
            raise RuntimeError(f"Query failed:\nQuery: {query}\nError: {err}")
        return result[0] if result is not None else None
    @staticmethod
    def _get_node_tx(tx, identifier):
        query = f"MATCH (a:{identifier}) RETURN a"
        result = tx.run(query).single()
        return result[0] if result is not None else None

class ItemNotFoundException(RuntimeError):
    pass


class SemanticScholar:
    delay_in_seconds = 3
    baseUri = "https://api.semanticscholar.org/graph/v1"

    @staticmethod
    def _get_paper_fields(references=False, citations=False) -> []:
        fields = ["paperId",
                  "externalIds",
                  "url",
                  "title",
                  "abstract",
                  "venue",
                  "year",
                  "referenceCount",
                  "citationCount",
                  # "influentialCitationCount",
                  "isOpenAccess",
                  "fieldsOfStudy",
                  "s2FieldsOfStudy",
                  "authors"]
        paper_fields = fields.copy()
        if references:
            for field in fields:
                paper_fields.append(f"references.{field}")
        if citations:
            for field in fields:
                paper_fields.append(f"citations.{field}")

        return ",".join(paper_fields)

    @staticmethod
    def _get_author_fields(papers=False):
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

    def _make_url(self, item_type, item_id=None, query=None, papers=False, references=False, citations=False):
        url = ""
        if item_type == "paper":
            fields = self._get_paper_fields(references=references, citations=citations)
        elif item_type == "author":
            fields = self._get_author_fields(papers=papers)
        else:
            raise RuntimeError("Missing type")
        if query is not None:
            url = f"{self.baseUri}/{item_type}/search?query={quote(query)}&fields={fields}"
        elif item_id is not None:
            url = f"{self.baseUri}/{item_type}/{item_id}?fields={fields}"
        else:
            raise RuntimeError("Invalid arguments")
        return url

    def _call_api_with_delay(self, url):
        result = requests.get(url).json()
        if "message" in result:
            raise RuntimeError("Message returned from API:" + result['message'])
        if "error" in result:
            if "not found" in result['error']:
                raise ItemNotFoundException(result['error'])
            raise RuntimeError("Error returned from API:" + result['error'])
        time.sleep(self.delay_in_seconds)
        return result

    def _retrieve_paged_result(self, url, data=None, offset=None):
        if data is None:
            data = []
        limit = 100
        offset = total = 0 if offset is None else limit
        while offset <= total:
            paged_url = url + f"&offset={offset}&limit={limit}"
            result = self._call_api_with_delay(paged_url)
            if result["total"] == 0:
                break
            total = result["total"]
            data.extend(result["data"])
            offset += limit
        return data

    def _retrieve_result(self, url):
        result = self._call_api_with_delay(url)
        if "offset" in result and "data" in result:
            return self._retrieve_paged_result(url, data=result['data'], offset=result['offset'])
        return self._call_api_with_delay(url)

    def paper(self, query=None, item_id=None, citations=False, references=False) -> Union[dict, list]:
        url = self._make_url(item_type="paper", item_id=item_id, query=query, citations=citations, references=references)
        return self._retrieve_result(url)

    def author(self, query=None, item_id=None, papers=False) -> Union[dict, list]:
        url = self._make_url(item_type="author", item_id=item_id, query=query, papers=papers)
        return self._retrieve_result(url)


class SemanticScholarToNeo4J:
    neo4jdb: GraphDb = None
    semantic: SemanticScholar = None

    def __init__(self):
        # neo4j database API object
        self.neo4jdb = GraphDb(uri="bolt://localhost:7687", user="neo4j", password="lts2021",
                               database="semanticscholar")
        # semantic scholar API object
        self.semantic = SemanticScholar()

    def __del__(self):
        self.neo4jdb.close()

    def create_id_selector(self, item):
        """
        given a sc object, create a neo4j node selector
        :param item:
        :return:
        """
        if "paperId" in item and item["paperId"]:
            return "Paper {paperId:'" + item["paperId"] + "'}"
        elif "authorId" in item and item["authorId"]:
            return "Author {authorId:'" + item["authorId"] + "'}"
        elif "title" in item and item["title"]:
            return "Paper {title:" + json.dumps(item["title"]) + "}"
        elif "name" in item and item["name"]:
            return "Author {name:" + json.dumps(item["name"]) + "}"
        raise RuntimeError("No usable id value in " + json.dumps(item))

    def import_paper(self, paper_data):
        if paper_data['externalIds'] and "DOI" in paper_data['externalIds']:
            paper_data['urn'] = "doi:" + paper_data['externalIds']['DOI']
        self.neo4jdb.merge_node("Paper", paper_data)

    def import_with_authors_and_citations(self, paper_data, relationship=None):
        """
        Given a semantic scholar paper object, import it, its authors, and the cited and citing papers
        :param paper_data:
        :return:
        """
        # import the paper
        authors = paper_data['authors']
        print((" - " + relationship + " " if relationship else "") + (" / ".join([a['name'] for a in authors])) + ", " +
              paper_data['title'])
        self.import_paper(paper_data)
        # import the paper's authors
        for author in authors:
            self.neo4jdb.merge_node("Author", author)
            self.neo4jdb.merge_relationship(
                source_node_selector=self.create_id_selector(author),
                target_node_selector=self.create_id_selector(paper_data),
                relationship="IS_CREATOR_OF"
            )
        # import citing and cited papers without recursing into their references and citations
        if "references" in paper_data and not relationship:
            for cited_paper_data in paper_data['references']:
                if not "title" in cited_paper_data:
                    continue
                self.import_with_authors_and_citations(cited_paper_data, relationship="citing")
                try:
                    self.neo4jdb.merge_relationship(
                        source_node_selector=self.create_id_selector(paper_data),
                        target_node_selector=self.create_id_selector(cited_paper_data),
                        relationship="CITES"
                    )
                except RuntimeError as err:
                    print(err)
        if "citations" in paper_data and not relationship:
            for citing_paper_data in paper_data['citations']:
                if not "title" in citing_paper_data:
                    continue
                self.import_with_authors_and_citations(citing_paper_data, relationship="cited by")
                try:
                    self.neo4jdb.merge_relationship(
                        source_node_selector=self.create_id_selector(citing_paper_data),
                        target_node_selector=self.create_id_selector(paper_data),
                        relationship="CITES"
                    )
                except RuntimeError as err:
                    print(err)

    def import_by_doi(self, doi: str) -> {}:
        """
        Given a DOI, imports the semantic scholar paper and author data, connecting authors and paper
        Returns the s2 paper data
        """
        paper_data = self.semantic.paper(item_id="DOI:" + doi, citations=True, references=True)
        self.import_with_authors_and_citations(paper_data)
        return paper_data

    @staticmethod
    def get_journal_dois(crossref_pub_id):
        """
        :param crossref_pub_id: find out at https://www.crossref.org/titleList/
        :return: list of dois
        """
        url = f"https://doi.crossref.org/search/doi?pid=email@address.com&format=doilist&pubid={crossref_pub_id}"
        list_as_text: str = requests.get(url).text
        return [line.strip().split(" ").pop(0) for line in list_as_text.split("\n")[1:] if line.strip()]

    def import_from_journal(self, crossref_pub_id):
        cache_path = f"data/crossref_{crossref_pub_id}.pkl"
        if os.path.exists(cache_path):
            with open(cache_path, "rb") as cache_file:
                dois = pickle.load(cache_file)
        else:
            dois = self.get_journal_dois(crossref_pub_id)
            with open(cache_path, "wb") as cache_file:
                pickle.dump(dois, cache_file)
        print("Processing " + str(len(dois)) + " papers...")
        while len(dois) > 0:
            doi = dois.pop(0)
            try:
                self.import_by_doi(doi)
            except ItemNotFoundException as err:
                print(err)
                continue
            except Exception as err:
                dois.append(doi)
                raise err
            finally:
                # save list of remaining dois
                with open(cache_path, "wb") as cache_file:
                    pickle.dump(dois, cache_file)
        # all dois have been processed, delete file
        os.remove(cache_path)

class ApiError(RuntimeError):
    pass

class OpenAlexToNeo4J:
    neo4jdb: GraphDb = None
    email = ""
    verbose = False
    base_url = "https://api.openalex.org"
    entity_types = ['work', 'author', 'institution', 'venue']
    node_labels = ['Work', 'Author', 'Institution', 'Venue']

    def __init__(self, email=None, verbose=False):
        if bool(email):
            self.email = email
        self.verbose = verbose
        # neo4j database API object
        self.neo4jdb = GraphDb(uri="bolt://localhost:7687", user="neo4j", password="lts2021", database="openalex")

    def __del__(self):
        self.neo4jdb.close()

    def log(self, message):
        if self.verbose:
            print(message)

    def get_headers(self):
        headers = {
            "Accept": "application/json",
            "User-Agent": f"requests mailto:{self.email}"
        }
        return headers

    def raise_api_error(self, url, response: requests.Response):
        if response.status_code == 404:
            raise ApiError(url + " returned 404 page not found")
        try:
            json_response = response.json()
            error = json_response['error']
            message = json_response['message']
        except:
            error = response.text
            message = None
        raise ApiError(
            f"Call to {url} failed.\nError: {error}" + (f"\nMessage: {message}" if bool(message) else ""))

    def get_short_id(self, entity_id: str):
        if entity_id is None:
            raise ValueError("Id is None")
        if entity_id.startswith("https://openalex.org/"):
            entity_id = entity_id[21:]
        return entity_id

    def get_single_entity(self, entity_type: str, entity_id: str) -> dict:
        entity_id = self.get_short_id(entity_id)
        if entity_type not in self.entity_types:
            raise ValueError(f"{entity_type} is not a valid entity type")
        url = f"{self.base_url}/{entity_type}s/{entity_id}"
        response = requests.get(url, headers=self.get_headers())
        if response.status_code == 200:
            data = response.json()
            data['api_url'] = url
            return data
        else:
            self.raise_api_error(url, response)

    def get_multiple_entities(self, entity_type: str, entity_filter: str) -> list:
        if entity_type not in self.entity_types:
            raise ValueError(f"{entity_type} is not a valid entity type")
        page = 1
        per_page = 100
        results = []
        while True:
            url = f"{self.base_url}/{entity_type}s?filter={entity_filter}&per-page={per_page}&page={page}"
            response = requests.get(url, headers=self.get_headers())
            if response.status_code == 200:
                data = response.json()
                results.extend(data['results'])
                if len(results) == data['meta']['count']:
                    # add "api_url" property for debugging
                    for item in results:
                        if bool(item['id']):
                            entity_id = self.get_short_id(item['id'])
                            data['api_url'] = f"{self.base_url}/{entity_type}s/{entity_id}"
                    return results
                page += 1
            else:
                self.raise_api_error(url, response)

    def get_type_from_entity_id(self, entity_id: str) -> str:
        entity_id = self.get_short_id(entity_id)
        entity_type = None
        for et in self.entity_types:
            if et[0] == entity_id[0].lower():
                entity_type = et
                break
        if entity_type is None:
            raise RuntimeError(f"Invalid id {entity_id}")
        return entity_type

    def get_label_from_entity_id(self, entity_id: str) -> str:
        entity_type = self.get_type_from_entity_id(entity_id)
        return entity_type[0].upper() + entity_type[1:]

    def create_node(self, node_label: str, node_data: dict) -> int:
        if node_label not in self.node_labels:
            raise ValueError(f"Invalid label {node_label}")
        if len(node_data.keys()) == 0 or self.count_non_empty_properties(node_data) == 0:
            print(node_data)
            raise ValueError(f"No data")
        return self.neo4jdb.merge_node(node_label, node_data)

    def node_exists(self, entity_id):
        entity_label = self.get_label_from_entity_id(entity_id)
        return self.neo4jdb.get_node(f'{entity_label} {{id:"{entity_id}"}}') is not None

    def create_relationship(self,
                            source_label: str, source_id: Union[str, int],
                            target_label: str, target_id: Union[str, int],
                            relationship: str,
                            source_id_property = "id", target_id_property = "id",):
        if source_label not in self.node_labels or target_label not in self.node_labels:
            raise ValueError("Invalid Source or target label")
        self.neo4jdb.merge_relationship(
            source_node_selector = source_label + f"{{{source_id_property}:" + json.dumps(source_id) + "}" if type(source_id) is str else source_id,
            target_node_selector = target_label + f"{{{target_id_property}:" + json.dumps(target_id) + "}" if type(target_id) is str else target_id,
            relationship=relationship,
        )

    def get_full_entity_data(self, entity_id: str):
        if entity_id is None:
            raise ValueError("Id is None")
        entity_type = self.get_type_from_entity_id(entity_id)
        data = self.get_single_entity(entity_type, entity_id)
        return data

    def count_non_empty_properties(self, data: dict) -> int:
        """
        Returns the number of non-empty properties on the dict
        :param data: a dict
        :return: bool
        """
        counter = 0
        for value in data.values():
            if bool(value):
                counter += 1
        return counter

    def import_author(self, author_data, retrieve_full_data=False) -> int:
        if retrieve_full_data:
            author_data = self.get_full_entity_data(author_data['id'])
        if 'display_name_alternatives' in author_data:
            author_data['display_name_alternatives'] = "\n".join(author_data['display_name_alternatives'])
        if 'last_known_institution' in author_data and bool(author_data['last_known_institution']):
            try:
                institution_data = author_data['last_known_institution']
                self.import_institution(institution_data, retrieve_full_data=True)
                self.create_relationship(
                    "Author", author_data['id'],
                    "Institution", institution_data['id'],
                    "MEMBER_OF")
            except ApiError as err:
                print(f"Could not import Institution '{institution_data['display_name']}': {err}")
        node_id = self.create_node("Author", author_data)
        self.log(f"Imported Author {author_data['display_name']}")
        return node_id

    def import_venue(self, data_: dict, retrieve_full_data=False) -> int:
        if retrieve_full_data:
            if bool(data_['id']):
                data_ = self.get_full_entity_data(data_['id'])
            else:
                raise ValueError("Invalid id")
        if data_['display_name'] is None and 'publisher' in data_ and bool(data_['publisher']):
            data_['display_name'] = data_['publisher']
        if 'issn' in data_ and bool(data_['issn']):
            data_['issn'] = ";".join(data_['issn'])
        node_id = self.create_node("Venue", data_)
        venue_name = data_['display_name'] or json.dumps(data_)
        self.log(f"Imported Venue {venue_name}")
        return node_id

    def import_institution(self, inst_data: dict, retrieve_full_data=False) -> int:
        if retrieve_full_data:
            inst_data = self.get_full_entity_data(inst_data['id'])
        if "display_name_alternatives" in inst_data and len(inst_data['display_name_alternatives']) > 0:
            inst_data['display_name_alternatives'] = "\n".join(inst_data['display_name_alternatives'])
        if "ids" in inst_data:
            for key, value in inst_data['ids'].items():
                inst_data[key] = value
        if "geo" in inst_data:
            for key, value in inst_data['geo'].items():
                inst_data[key] = value
        if retrieve_full_data and 'associated_institutions' in inst_data:
            for ass_inst_data in inst_data['associated_institutions']:
                # create links only to institutions that already exist
                if ass_inst_data['id'] != inst_data['id'] and self.node_exists(ass_inst_data['id']):
                    self.create_relationship(
                        "Institution", inst_data['id'],
                        "Institution", ass_inst_data['id'],
                        "AFFILIATED_WITH")
        node_id = self.create_node("Institution", inst_data)
        self.log(f"Imported Institution {inst_data['display_name']}")
        return node_id

    def import_work(self, work_data, retrieve_full_data=False, import_cited_works=True) -> int:
        if retrieve_full_data:
            work_data = self.get_full_entity_data(work_data['id'])
        if 'host_venue' in work_data \
                and type(work_data['host_venue']) is dict \
                and self.count_non_empty_properties(work_data['host_venue']) >= 1:
            venue_id = work_data['host_venue']['id']
            try:
                venu_node_id = self.import_venue(
                    work_data['host_venue'],
                    retrieve_full_data = bool(venue_id))
                self.create_relationship(
                    "Work", work_data['id'],
                    "Venue", venue_id or venu_node_id,
                    "PUBLISHED_IN")
            except ApiError as err:
                print(f"Could not import Work '{work_data['title']}': {err}")
        for authorship in work_data['authorships']:
            author_id = authorship['author']['id']
            try:
                author_node_id = self.import_author(authorship['author'], retrieve_full_data=True)
                self.create_relationship(
                    "Author", author_id or author_node_id,
                    "Work", work_data['id'],
                    "CREATOR_OF")
            except ApiError as err:
                print(f"Could not import Author {authorship['author']['display_name']}:{err}")
        if 'biblio' in work_data:
            for key, value in work_data['biblio'].items():
                work_data[key] = value
        node_id = self.create_node("Work", work_data)
        if import_cited_works and 'referenced_works' in work_data:
            cited_works_oa_ids = work_data['referenced_works']
            num_cited_works = len(cited_works_oa_ids)
            for idx, cited_work_oa_id in enumerate(cited_works_oa_ids):
                self.log(f">>> Importing {idx+1} of {num_cited_works} cited works:")
                try:
                    node_id = self.import_work({"id": cited_work_oa_id},
                                               retrieve_full_data=True,
                                               import_cited_works=False)
                    self.create_relationship(
                        "Work", work_data['id'],
                        "Work", cited_work_oa_id or node_id,
                        "CITES")
                except ApiError as err:
                    print(f"Could not import Cited Work '{cited_work_oa_id}':{err}")
        self.log(f"Imported Work {work_data['title']}")
        return node_id


if __name__ == "__main__":
    _, email = parseaddr(os.environ.get("OPENALEX_EMAIL"))
    if email == '':
        raise ValueError("Please define the environment variable OPENALEX_EMAIL to contain your email address")
    importer = OpenAlexToNeo4J(email, verbose=True)
    issn = "0174-0202"
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
        importer.import_work(work_data)
        with open(cache_path, "w") as cache_file:
            json.dump(all_items, cache_file)
