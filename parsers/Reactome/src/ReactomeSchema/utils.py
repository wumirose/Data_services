from neo4j import GraphDatabase
import json
import os


class GraphDb:
    def __init__(self):
        self.__uri = 'bolt://localhost:7688/'
        self.__user = 'neo4j'
        self.__pwd = 'qwerty1234'
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query))
            if response:
                return response
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()


db = GraphDb()


def unique_nodes_edges():
    """
    Get and count unique node labels and unique edges
    :return: dict
    """
    if not (os.path.exists('unique_nodes.json') and os.path.exists('unique_edges.json')):
        cypher_query = """
            MATCH (n)
            UNWIND labels(n) AS label
            WITH DISTINCT label, 
                COUNT(DISTINCT n) AS node_count
                ORDER BY label
            WITH collect({label: label, count: node_count}) AS node_labels
            MATCH ()-[r]->()
            WITH DISTINCT type(r) AS edge_type, 
                COUNT(DISTINCT r) AS edge_count, 
                node_labels
            ORDER BY edge_type
            WITH collect({edge_type: edge_type, count: edge_count}) AS edge_types, 
                node_labels
            RETURN {node_labels: node_labels, edge_types: edge_types}
        """
        results = db.query(cypher_query, db='reactome')

        node_data = json.dumps(results[0][0])['node_labels']
        edge_data = json.dumps(results[0][0])['edge_types']
        with open('unique_nodes.json', 'w') as f:
            f.write(node_data)
        with open('unique_edges.json', 'w') as f:
            f.write(edge_data)
    else:
        with open('unique_edges.json', 'r') as f:
            node_json_data = json.loads(f.read())
        with open('unique_edges.json', 'r') as f:
            edge_json_data = json.loads(f.read())
    print("+" * 15)
    print(f"Unique Nodes Types: {len(node_json_data)}")
    print(f"Unique Edges Types: {len(edge_json_data)}")
    print("+" * 15, '\n')


def get_nodes(node_label=None):
    # Nodes for all node types
    if not node_label:
        node_ = db.query('MATCH (n) RETURN n', db='reactome')
    else:
        if isinstance(node_label, str):
            node_ = db.query(f'MATCH (n:{node_label}) RETURN n', db='reactome')
        else:
            # Nodes for all labels in the list
            match = ':'.join(node_label)
            node_ = f'MATCH (n{match}) RETURN n'
            node_ = db.query(node_, db='reactome')
    return node_


def get_node_edge_json(nodes, relationships):
    # Process nodes and relationships and create JSON object
    json_obj = {'nodes': [], 'edges': []}
    for node in nodes:
        node_data = {'id': node['n'].element_id, 'labels': list(node['n'].labels), 'properties': dict(node['n'])}
        json_obj['nodes'].append(node_data)

    for relationship in relationships:
        relationship_data = {'id': relationship['r'].id, 'type': relationship['r'].type,
                             'start_node_id': relationship['r'].start_node.id,
                             'end_node_id': relationship['r'].end_node.id, 'properties': dict(relationship['r'])}
        json_obj['edges'].append(relationship_data)

    # Convert JSON object to JSON string
    json_str = json.dumps(json_obj, indent=4)

    return json_str


def get_edges(edge_label=None):
    if not edge_label:
        # Get all relationships
        relationships = db.query("MATCH ()-[r]->() RETURN r", db='reactome')
    else:
        # Get all relationships with type 'A'
        relationships = db.query(f"MATCH ()-[r:{edge_label}]->() RETURN r", db='reactome')
    return relationships

