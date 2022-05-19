import logging
import os
import sys
from typing import List

from elasticsearch import Elasticsearch
from udata_search_service.config import Config

ELASTIC_HOST = os.environ.get('ELASTIC_HOST', 'localhost')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT', '9200')

ALL_INDICES = [
    'dataset',
    'reuse',
    'organization',
]


def set_alias(index_suffix_name: str, indices: List[str] = None, delete: bool = True) -> None:
    '''
    Properly end an indexation by swapping alias.
    Previous alias is deleted if needed.
    Alias will be removed from previous indices and set to
    the new indices (that match the suffix `index_suffix_name`).
    A list of indices on which to apply the alias migration can be passed.
    See an example logic https://github.com/elastic/elasticsearch-dsl-py/blob/master/examples/alias_migration.py
    '''
    es = Elasticsearch([{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}])

    indices = indices or ALL_INDICES
    for index in indices:
        if index not in ALL_INDICES:
            logging.error('Unknown index alias %s', index)
            sys.exit(-1)

        index_alias = Config.UDATA_INSTANCE_NAME + '-' + index
        pattern = index_alias + '-*'
        index_name = index_alias + '-' + index_suffix_name
        logging.info('Creating alias "%s" on index "%s"', index_alias, index_name)

        previous_indices = None
        if delete:
            if es.indices.exists_alias(name=index_alias):
                previous_indices = es.indices.get_alias(name=index_alias).keys()

        es.indices.update_aliases(
            body={
                "actions": [
                    {"remove": {"alias": index_alias, "index": pattern}},
                    {"add": {"alias": index_alias, "index": index_name}},
                ]
            }
        )

        if delete and previous_indices:
            for previous_index in previous_indices:
                if previous_index != index_name:
                    es.indices.delete(index=previous_index)
