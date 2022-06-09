from neo4j import GraphDatabase, Result
import json
from typing import Union
from abc import ABC


class Work(ABC):
    LABEL = None
    PROP_DISPLAY_NAME = None
    PROP_PUBLICATION_YEAR = None
    REL_CITES = None


class Creator(ABC):
    LABEL = None
    PROP_DISPLAY_NAME = None
    REL_CREATOR_OF = None


class MergeFailedError(RuntimeError):
    pass


class GraphDb:

    def __init__(self, uri, user, password, database):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def close(self):
        self.driver.close()

    def run_query(self, query) -> Result:
        with self.driver.session(database=self.database) as session:
            return session.read_transaction(lambda tx: tx.run(query))

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

    def get_node_id(self, identifier):
        with self.driver.session(database=self.database) as session:
            return session.read_transaction(self._get_node_id_tx, identifier)

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
            raise MergeFailedError(f"Query failed:\nQuery: {query}\nError: {err}")
        if result is None:
            raise MergeFailedError("The following merge failed: " + query)
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
            query = f"MATCH (a), (b:{target_node_selector}) WHERE id(a)={source_node_selector}"
        elif type(source_node_selector) is int and type(target_node_selector) is int:
            query = f"MATCH (a),(b) WHERE id(a)={source_node_selector} and id(b)={target_node_selector}"
        else:
            raise ValueError("Invalid values for source and/or target selector")
        query += f" MERGE (a)-[r:{relationship}]->(b) RETURN id(r)"
        #print(query)
        try:
            result = tx.run(query).single()
        except Exception as err:
            raise MergeFailedError(f"Query failed:\nQuery: {query}\nError: {err}")
        if result is None:
            raise MergeFailedError("The following merge failed: " + query)
        return result[0]

    @staticmethod
    def _get_node_tx(tx, identifier):
        query = f"MATCH (a:{identifier}) RETURN a"
        result = tx.run(query).single()
        return result[0] if result is not None else None

    @staticmethod
    def _get_node_id_tx(tx, identifier):
        query = f"MATCH (a:{identifier}) RETURN id(a)"
        result = tx.run(query).single()
        return result[0] if result is not None else None

