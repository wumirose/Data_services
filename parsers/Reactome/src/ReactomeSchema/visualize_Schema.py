import json
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
import uvicorn
from utils import *

app = FastAPI()
templates = Jinja2Templates(directory="templates")

response = requests.get('https://reactome.org/content/schema/DatabaseObject')
soup = BeautifulSoup(response.content, 'html.parser')
tree = soup.find('div', class_='dataschema-tree').find('ul', class_='tree')
hierarchy = extract_hierarchy(tree)
if not os.path.exists('schema.json'):
    with open('schema.json', 'w') as f:
        f.write(json.dumps(hierarchy, indent=4))


@app.get('/', tags=['Json Schema'])
async def index():
    with open('schema.json') as fl:
        data = json.load(fl)
    return data


@app.get("/nodes/", tags=['Nodes Json'])
async def create_item():
    with open('unique_nodes.json') as fl:
        nodes = json.load(fl)
    return nodes


@app.get("/edges/", tags=['Edges Json'])
async def create_item():
    with open('unique_edges.json') as fl:
        edges = json.load(fl)
    return edges

@app.get("/triples", tags=['Triples Dataframe'])
async def triple(request: Request):
    dataframe = get_triples()
    table = dataframe.to_html()
    return templates.TemplateResponse("index.html", {"request": request, "table": table})


if __name__ == '__main__':
    unique_nodes_edges()
    # nodes = get_nodes(node_label=None)
    # relationships = get_edges(edge_label=None)
    # get_node_edge_json(nodes, relationships)
    uvicorn.run(app, host="127.0.0.1", port=5049)
