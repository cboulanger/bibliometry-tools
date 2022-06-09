import time
from src.graphdb import GraphDb, Work, Creator
import requests
import json
from typing import Union
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class OpenAlexWork(Work):
    LABEL = "Work"
    PROP_DISPLAY_NAME = "display_name"
    PROP_PUBLICATION_YEAR = "publication_year"
    REL_CITES = "CITES"


class OpenAlexCreator(Creator):
    LABEL = "Author"
    PROP_DISPLAY_NAME = "display_name"
    REL_CREATOR_OF = "CREATOR_OF"


class ApiError(RuntimeError):
    pass


class OpenAlexToNeo4J:
    http: requests.Session
    graphdb: GraphDb = None
    email = ""
    verbose = False
    base_url = "https://api.openalex.org"
    entity_types = ['work', 'author', 'institution', 'venue']
    node_labels = ['Work', 'Author', 'Institution', 'Venue']

    def __init__(self, email=None, verbose=False, graphdb: GraphDb = None):
        if bool(email):
            self.email = email
        self.verbose = verbose
        # neo4j database API object
        if graphdb is None:
            raise ValueError("No GraphDb instance provided")
        self.graphdb = graphdb
        # configure http api access to handle transient errors
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.http = requests.Session()
        self.http.mount("https://", adapter)

    def __del__(self):
        self.graphdb.close()

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
            f"Call to {url} failed.\nError: {response.status_code} {error}" + (
                f"\nMessage: {message}" if bool(message) else ""))

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
        response = self.http.get(url, headers=self.get_headers())
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
            response = self.http.get(url, headers=self.get_headers())
            if response.status_code == 200:
                data = response.json()
                results.extend(data['results'])
                if len(results) >= data['meta']['count']:
                    break
                page += 1

            else:
                self.raise_api_error(url, response)
            time.sleep(1)
        # add "api_url" property for debugging
        for item in results:
            if bool(item['id']):
                entity_id = self.get_short_id(item['id'])
                data['api_url'] = f"{self.base_url}/{entity_type}s/{entity_id}"
        return results

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
        return self.graphdb.merge_node(node_label, node_data)

    def get_entity_node_id(self, entity_id: str) -> Union[int, None]:
        """
        Given an openalex entity id, return its node id if that entity exists in the graph
        or None if it doesn't exist
        :param entity_id:str
        :return:bool
        """
        entity_label = self.get_label_from_entity_id(entity_id)
        return self.graphdb.get_node_id(f'{entity_label} {{id:"{entity_id}"}}')

    def entity_exists(self, entity_id: str) -> bool:
        """
        Given an openalex entity id, return true if that entity exists in the graph, otherwise false
        :param entity_id:str
        :return:bool
        """
        return self.get_entity_node_id(entity_id) is not None

    def create_relationship(self,
                            source_label: str, source_id: Union[str, int],
                            target_label: str, target_id: Union[str, int],
                            relationship: str,
                            source_id_property="id", target_id_property="id", ):
        if source_label not in self.node_labels or target_label not in self.node_labels:
            raise ValueError("Invalid Source or target label")
        source_node_selector = source_label + f"{{{source_id_property}:" + json.dumps(source_id) + "}" if type(
            source_id) is str else source_id
        target_node_selector = target_label + f"{{{target_id_property}:" + json.dumps(target_id) + "}" if type(
            target_id) is str else target_id
        self.graphdb.merge_relationship(
            source_node_selector=source_node_selector,
            target_node_selector=target_node_selector,
            relationship=relationship,
        )
        self.log(f"Created relationship ({source_node_selector})-[{relationship}]->({target_node_selector})")

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
            try:
                author_data = self.get_full_entity_data(author_data['id'])
            except ApiError as err:
                print(f"Could not access full data for Author '{author_data}': {err}")
                pass
        if 'display_name_alternatives' in author_data:
            author_data['display_name_alternatives'] = "\n".join(author_data['display_name_alternatives'])
        # create node
        author_node_id = self.create_node("Author", author_data)
        # link institution
        if 'last_known_institution' in author_data and bool(author_data['last_known_institution']):
            institution_data = author_data['last_known_institution']
            institution_id = institution_data['id']
            institution_node_id = self.get_entity_node_id(institution_id) if institution_id else None
            if not institution_node_id:
                institution_node_id = self.import_institution(institution_data, retrieve_full_data=True)
            self.create_relationship(
                "Author", author_node_id,
                "Institution", institution_node_id,
                "MEMBER_OF")
        self.log(f"Imported Author {author_data['display_name']}")
        return author_node_id

    def import_venue(self, venue_data: dict, retrieve_full_data=False) -> int:
        if retrieve_full_data:
            try:
                venue_data = self.get_full_entity_data(venue_data['id'])
            except ApiError as err:
                print(f"Could not access full data for Venue '{venue_data}': {err}")
        if venue_data['display_name'] is None and 'publisher' in venue_data and bool(venue_data['publisher']):
            venue_data['display_name'] = venue_data['publisher']
        if 'issn' in venue_data and bool(venue_data['issn']):
            venue_data['issn'] = ";".join(venue_data['issn'])
        node_id = self.create_node("Venue", venue_data)
        venue_name = venue_data['display_name'] or json.dumps(venue_data)
        self.log(f"Imported Venue {venue_name}")
        return node_id

    def import_institution(self, inst_data: dict, retrieve_full_data=False) -> int:
        if retrieve_full_data:
            try:
                inst_data = self.get_full_entity_data(inst_data['id'])
            except ApiError as err:
                print(f"Could not access full data for Institution '{inst_data}': {err}")
        if "display_name_alternatives" in inst_data and len(inst_data['display_name_alternatives']) > 0:
            inst_data['display_name_alternatives'] = "\n".join(inst_data['display_name_alternatives'])
        if "ids" in inst_data:
            for key, value in inst_data['ids'].items():
                inst_data[key] = value
        if "geo" in inst_data:
            for key, value in inst_data['geo'].items():
                inst_data[key] = value
        inst_node_id = self.create_node("Institution", inst_data)
        if retrieve_full_data and 'associated_institutions' in inst_data:
            for ass_inst_data in inst_data['associated_institutions']:
                ass_inst_id = ass_inst_data['id']
                ass_inst_node_id = self.get_entity_node_id(ass_inst_id) if ass_inst_id else None
                # create links only to institutions that already exist
                if ass_inst_node_id and ass_inst_id != inst_node_id:
                    self.create_relationship(
                        "Institution", inst_node_id,
                        "Institution", ass_inst_node_id,
                        "AFFILIATED_WITH")
        self.log(f"Imported Institution {inst_data['display_name']}")
        return inst_node_id

    def import_work(self, work_data, retrieve_full_data=False, import_cited_works=True) -> int:
        if retrieve_full_data:
            try:
                work_data = self.get_full_entity_data(work_data['id'])
            except ApiError as err:
                print(f"Could not access full data for Work '{work_data}': {err}")
        if 'biblio' in work_data:
            for key, value in work_data['biblio'].items():
                work_data[key] = value
        work_node_id = self.create_node("Work", work_data)
        if 'host_venue' in work_data \
                and type(work_data['host_venue']) is dict \
                and self.count_non_empty_properties(work_data['host_venue']) >= 1:
            venue_id = work_data['host_venue']['id']
            venue_node_id = self.get_entity_node_id(venue_id) if venue_id else None
            try:
                if not venue_node_id:
                    venue_node_id = self.import_venue(work_data['host_venue'], retrieve_full_data=bool(venue_id))
                self.create_relationship(
                    "Work", work_node_id,
                    "Venue", venue_node_id,
                    "PUBLISHED_IN")
            except ApiError as err:
                print(f"Could not import Work '{work_data['title']}': {err}")
        for authorship in work_data['authorships']:
            author_id = authorship['author']['id']
            author_node_id = self.get_entity_node_id(author_id)
            try:
                if not author_node_id:
                    author_node_id = self.import_author(authorship['author'], retrieve_full_data=True)
                self.create_relationship(
                    "Author", author_node_id,
                    "Work", work_node_id,
                    "CREATOR_OF")
            except ApiError as err:
                print(f"Could not import Author {authorship['author']['display_name']}:{err}")
        if import_cited_works and 'referenced_works' in work_data:
            cited_works_oa_ids = work_data['referenced_works']
            num_cited_works = len(cited_works_oa_ids)
            for idx, cited_work_oa_id in enumerate(cited_works_oa_ids):
                self.log(f">>> Importing {idx + 1} of {num_cited_works} cited works:")
                try:
                    cited_work_oa_id = self.import_work({"id": cited_work_oa_id},
                                                        retrieve_full_data=True,
                                                        import_cited_works=False)
                    self.create_relationship(
                        "Work", work_node_id,
                        "Work", cited_work_oa_id,
                        "CITES")
                except ApiError as err:
                    print(f"Could not import Cited Work '{cited_work_oa_id}':{err}")
        self.log(f"Imported Work {work_data['title']}")
        return work_node_id
