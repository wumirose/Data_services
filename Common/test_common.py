import os
import shutil
import json
import pytest

from rdflib import Graph
from Common.utils import GetData, EdgeNormUtils


def test_get_uniprot_virus_date_stamp():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    date_stamp: str = gd.get_uniprot_virus_date_stamp(data_file_path)

    assert(date_stamp == '20200617')


def test_pull_via_http():
    from Common.utils import GetData

    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    byte_count: int = gd.pull_via_http('https://renci.org/mission-and-vision', data_file_path)

    assert byte_count

    assert(os.path.exists(os.path.join(data_file_path, 'mission-and-vision')))

    os.remove(os.path.join(data_file_path, 'mission-and-vision'))


def test_get_taxon_id_list():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    type_virus: str = '9'

    taxonid_set: set = gd.get_ncbi_taxon_id_set(data_file_path, type_virus)

    assert(len(taxonid_set) == 189019)


def test_get_virus_files():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    type_virus: str = '9'

    taxonid_set: set = gd.get_ncbi_taxon_id_set(data_file_path, type_virus)

    assert(len(taxonid_set) == 189019)

    file_list: list = gd.get_uniprot_virus_file_list(data_file_path, taxonid_set)

    assert(len(file_list) == 3993)


def test_get_goa_files_chain():
    gd = GetData()

    data_file_path: str = os.path.dirname(os.path.abspath(__file__))

    type_virus: str = '9'

    taxonid_set: set = gd.get_ncbi_taxon_id_set(data_file_path, type_virus)

    assert(len(taxonid_set) == 189019)

    file_list: list = gd.get_uniprot_virus_file_list(data_file_path, taxonid_set)

    assert(len(file_list) == 3993)

    data_file_path += '/Virus_GOA_files/'

    file_subset: list = file_list[:2]

    actual_count: int = gd.get_goa_ftp_files(data_file_path, file_subset, '/pub/databases/GO/goa', '/proteomes/')

    assert(actual_count == len(file_subset))

    # remove the test data
    shutil.rmtree(data_file_path)


def test_edge_norm():
    # get the edge norm object
    en = EdgeNormUtils()

    # create an edge list
    edge_list: list = [{'predicate': 'SEMMEDDB:CAUSES', 'relation': '', 'edge_label': ''}, {'predicate': 'RO:0000052', 'relation': '', 'edge_label': ''}]

    # normalize the data
    en.normalize_edge_data(edge_list)

    # check the return
    assert(edge_list[0]['predicate'] == 'SEMMEDDB:CAUSES')
    assert(edge_list[0]['relation'] == 'biolink:causes')
    assert(edge_list[0]['edge_label'] == 'biolink:causes')

    assert(edge_list[1]['predicate'] == 'RO:0000052')
    assert(edge_list[1]['relation'] == 'biolink:affects')
    assert(edge_list[1]['edge_label'] == 'biolink:affects')


@pytest.mark.skip(reason="INot quite ready yet")
def test_get_biolink_ld_json():
    # instantiate the object that has the method to do this
    gd = GetData()

    context_json = json.load(open('context.jsonld'))

    context = context_json['@context']

    # input_data = 'https://raw.githubusercontent.com/NCATS-Tangerine/kgx/master/tests/resources/rdf/test1.nt'

    input_data = 'D:/Work/Robokop/Data_services/Ubergraph_data/properties-nonredundant.ttl'

    # get the biolink json-ld data
    g: Graph = gd.get_biolink_graph(input_data)

    # assert that we got it. more detailed interrogation to follow
    assert(isinstance(g, Graph))

    # ref = URIRef('http://identifiers.org/chembl.compound/')

    # print out the entire Graph in the RDF Turtle format
    print('\n' + g.serialize(format="turtle", context=context).decode("utf-8"))
