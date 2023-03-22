import requests
from utils import *

db = GraphDb()


if __name__ == '__main__':
    unique_nodes_edges()
    chebi_data = 0
    # # Use ChEBI API to lookup CHEBI IDs for each ChemicalDrug node
    for record in get_nodes('Pathway{stId:"R-HSA-1236975"}'):
        node_data = record['n']
        print('++nodeProperties++')
        for k, v in dict(record['n']).items():
            print(k, '-->', v)
        print("+" * 15, '\n')

        if node_data.get('displayName'):
            display_name = node_data.get('displayName')
            # Use ChEBI API to lookup CHEBI ID for displayName
            url = f"https://www.ebi.ac.uk/webservices/chebi/2.0/test/getLiteEntity?search={display_name}&searchCategory=CHEBI+NAME&maximumResults=200&starsCategory=ALL"
            response = requests.get(url)
            try:
                chebi_data = response.json()
            except requests.exceptions.JSONDecodeError:
                print('\x1b[0;30;41m' + 'Invalid JSON data returned from ChEBI API for: ' + '\x1b[0m' + display_name)
                print("+" * 15, '\n')
            if chebi_data:
                # Extract CHEBI ID
                chebi_id = chebi_data['chebiId']
                print(f"Node name: {node_data['name']}, CHEBI ID: {chebi_id}")
        break

    rs = db.query('MATCH(ewas: EntityWithAccessionedSequence{stId: "R-HSA-199420"}), (ewas) - [: referenceEntity]->('
                  're:ReferenceEntity) RETURN ewas.displayName AS EWAS, re.identifier AS Identifier', db='reactome')
    print('Identifiers for proteins or chemicals')
    print(rs)
    print("+" * 15, '\n')

    # # Following the reference entity and database links in order to get the identifier and the database of reference
    id_dbReference = db.query('MATCH (ewas:EntityWithAccessionedSequence{stId:"R-HSA-199420"}), (ewas)-[:referenceEntity]->('
        're:ReferenceEntity)-[:referenceDatabase] >(rd:ReferenceDatabase) RETURN ewas.displayName AS EWAS, '
        're.identifier AS Identifier, rd.displayName AS Database', db='reactome')
    print('Using the reference entity and database links to get the identifier and the database of reference')
    print(id_dbReference)
    print("+" * 15, '\n')
