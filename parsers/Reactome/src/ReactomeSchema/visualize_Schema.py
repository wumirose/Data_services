from flask import Flask, render_template
import json
import requests
from bs4 import BeautifulSoup


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


app = Flask(__name__)


@app.route('/')
def index():
    with open('data.json') as fl:
        data = json.load(fl)
    return render_template('index.html', data=json.dumps(data))


if __name__ == '__main__':
    app.run(debug=True)
