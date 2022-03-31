import os
from src.graphdb import GraphDb
from urllib.parse import quote
import requests
import json
from typing import Union
import time
import pickle

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

    def __init__(self, graphdb: GraphDb= None):
        # neo4j database API object
        if graphdb is None:
            raise ValueError("No GraphDb instance provided")
        self.neo4jdb = graphdb
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
