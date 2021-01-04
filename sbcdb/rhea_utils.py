'''
SYNBIOCHEM-DB (c) University of Manchester 2015

SYNBIOCHEM-DB is licensed under the MIT License.

To view a copy of this license, visit <http://opensource.org/licenses/MIT/>.

@author:  neilswainston
'''
import tempfile
import urllib.request


__RHEA_URL = 'ftp://ftp.ebi.ac.uk/pub/databases/rhea/tsv/rhea2uniprot.tsv'


def load(reaction_manager, source=__RHEA_URL, num_threads=0):
    '''Loads Rhea data.'''
    # Parse data:
    temp_file = tempfile.NamedTemporaryFile()
    urllib.request.urlretrieve(source, temp_file.name)
    data = _parse(temp_file.name)
    reaction_manager.add_react_to_enz(data, 'rhea', num_threads)


def _parse(filename):
    '''Parses file.'''
    data = {}

    with open(filename, 'r') as textfile:
        next(textfile)

        for line in textfile:
            tokens = line.split('\t')

            if len(tokens) == 4:
                uniprot_id = tokens[3].strip()

                if not tokens[0] or not tokens[2]:
                    print (','.join(tokens))

                _add(data, tokens[0], uniprot_id)
                _add(data, tokens[2], uniprot_id)

    return data


def _add(data, rhea_id, uniprot_id):
    '''Adds Rhea id and Uniprot id to data.'''
    if rhea_id in data:
        data[rhea_id].append(uniprot_id)
    else:
        data[rhea_id] = [uniprot_id]
