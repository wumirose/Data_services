import json
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI
import uvicorn
from utils import *

db = GraphDb()
app = FastAPI()


def extract_hierarchy(ul):
    hierarchies = dict()
    for ix, li in enumerate(ul.find_all('li', recursive=False)):
        node = li.span.text.strip()
        children = li.find('ul')
        if children:
            children_hierarchy = extract_hierarchy(children)
            hierarchies.update({node: children_hierarchy})
    return hierarchies


response = requests.get('https://reactome.org/content/schema/DatabaseObject')
soup = BeautifulSoup(response.content, 'html.parser')
tree = soup.find('div', class_='dataschema-tree').find('ul', class_='tree')
hierarchy = extract_hierarchy(tree)
with open('data.json', 'w') as f:
    f.write(json.dumps(hierarchy, indent=4))


@app.get('/')
def index():
    with open('data.json') as fl:
        data = json.load(fl)
    return data


if __name__ == '__main__':
    unique_nodes_edges()
    # nodes = get_nodes(node_label=None)
    # relationships = get_edges(edge_label=None)
    # get_node_edge_json(nodes, relationships)
    uvicorn.run(app, host="127.0.0.1", port=5049)

