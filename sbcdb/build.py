'''
SYNBIOCHEM-DB (c) University of Manchester 2015

SYNBIOCHEM-DB is licensed under the MIT License.

To view a copy of this license, visit <http://opensource.org/licenses/MIT/>.

@author:  neilswainston
'''
import multiprocessing
import sys

from sbcdb import chebi_utils, chemical_utils, mnxref_utils, \
    ncbi_taxonomy_utils, reaction_utils, rhea_utils, spectra_utils, utils

import logging
import sys

def setup_custom_logger(name):
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.FileHandler('log.txt', mode='w')
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(screen_handler)
    return logger

logger = setup_custom_logger('sbcdb')

def build_csv(dest_dir, array_delimiter, num_threads):
    '''Build database CSV files.'''
    writer = utils.Writer(dest_dir)

    # Get Organism data:
    logger.info('Parsing NCBI Taxonomy')
    ncbi_taxonomy_utils.load(writer, array_delimiter)

    # Get Chemical and Reaction data.
    # Write chemistry csv files:
    chem_man = chemical_utils.ChemicalManager(array_delimiter=array_delimiter)
    reac_man = reaction_utils.ReactionManager()

    logger.info('Parsing MNXref')
    mnx_loader = mnxref_utils.MnxRefLoader(chem_man, reac_man, writer)
    mnx_loader.load()

    logger.info('Parsing ChEBI')
    chebi_utils.load(chem_man, writer)

    # Get Spectrum data:
    logger.info('Parsing spectrum data')
    #spectra_utils.load(writer, chem_man, array_delimiter=array_delimiter)

    chem_man.write_files(writer)

    # Get Reaction / Enzyme / Organism data:
    # print 'Parsing KEGG'
    # kegg_utils.load(reac_man, num_threads=num_threads)

    logger.info('Parsing Rhea')
    rhea_utils.load(reac_man, num_threads=num_threads)

    reac_man.write_files(writer)


def main(args):
    '''main method'''
    num_threads = 0

    if len(args) > 2:
        try:
            num_threads = int(args[2])
        except ValueError:
            if args[2] == 'True':
                num_threads = multiprocessing.cpu_count()

    logger.info('Running build with %d threads' % num_threads)

    build_csv(args[0], args[1], num_threads)


if __name__ == '__main__':
    main(sys.argv[1:])
