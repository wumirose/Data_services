import requests
from bs4 import BeautifulSoup

from utils import *

db = GraphDb()

if __name__ == '__main__':
    unique_nodes_edges()
    # nodes = get_nodes(node_label=None)
    # relationships = get_edges(edge_label=None)
    # get_node_edge_json(nodes, relationships)
