import time

from src.graphdb import GraphDb
import requests
import json
from typing import Union

class ApiError(RuntimeError):
    pass

class OpenAlexToNeo4J:
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
            time.sleep(1)

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

    def node_exists(self, entity_id):
        entity_label = self.get_label_from_entity_id(entity_id)
        return self.graphdb.get_node(f'{entity_label} {{id:"{entity_id}"}}') is not None

    def create_relationship(self,
                            source_label: str, source_id: Union[str, int],
                            target_label: str, target_id: Union[str, int],
                            relationship: str,
                            source_id_property="id", target_id_property="id", ):
        if source_label not in self.node_labels or target_label not in self.node_labels:
            raise ValueError("Invalid Source or target label")
        self.graphdb.merge_relationship(
            source_node_selector=source_label + f"{{{source_id_property}:" + json.dumps(source_id) + "}" if type(
                source_id) is str else source_id,
            target_node_selector=target_label + f"{{{target_id_property}:" + json.dumps(target_id) + "}" if type(
                target_id) is str else target_id,
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
            institution_data = author_data['last_known_institution']
            try:
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
                    retrieve_full_data=bool(venue_id))
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
                self.log(f">>> Importing {idx + 1} of {num_cited_works} cited works:")
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
