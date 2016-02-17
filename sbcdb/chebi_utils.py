'''
SYNBIOCHEM-DB (c) University of Manchester 2015

SYNBIOCHEM-DB is licensed under the MIT License.

To view a copy of this license, visit <http://opensource.org/licenses/MIT/>.

@author:  neilswainston
'''
import math
import sys

import libchebipy
import py2neo

from sbcdb import py2neo_utils
import sbcdb
import synbiochem.design


def load(url):
    '''Loads ChEBI data from libChEBIpy.'''
    nodes = {}
    rels = {}

    # Add root, applying recursion:
    __add_node('CHEBI:24431', nodes, rels)

    # Contact Neo4j database, create Graph object:
    graph = py2neo_utils.get_graph(url)
    py2neo_utils.create(graph, nodes, 512)
    py2neo_utils.create(graph, rels, 256)


def __add_node(chebi_id, nodes, rels):
    '''Constructs a node from libChEBI.'''
    if chebi_id not in nodes:
        entity = libchebipy.ChebiEntity(chebi_id)

        properties = {}
        properties['chebi'] = entity.get_id()
        properties['name'] = entity.get_name()
        properties['names'] = [name.get_name() for name in entity.get_names()]
        properties['formula'] = entity.get_formula()
        properties['charge'] = 0 if math.isnan(entity.get_charge()) \
            else entity.get_charge()

        properties['inchi'] = entity.get_inchi()
        properties['smiles'] = entity.get_smiles()

        for db_acc in entity.get_database_accessions():
            namespace = synbiochem.design.resolve_namespace(db_acc.get_type(),
                                                            True)

            if namespace is not None:
                properties[namespace] = db_acc.get_accession_number()

        sbcdb.normalise_masses(properties)

        node = py2neo.Node.cast('Chemical', properties)
        nodes[chebi_id] = node

        for incoming in entity.get_incomings():
            target_id = incoming.get_target_chebi_id()
            target_node = __add_node(target_id, nodes, rels)
            rels[len(rels)] = py2neo.rel(target_node, incoming.get_type(),
                                         node, source='chebi')
    else:
        node = nodes[chebi_id]

    return node


def main(argv):
    '''main method'''
    load(*argv)


if __name__ == "__main__":
    main(sys.argv[1:])
