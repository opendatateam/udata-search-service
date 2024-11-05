import logging
import os
import sys
from typing import List

from udata_search_service.infrastructure.search_clients import ElasticClient
from udata_search_service.config import Config

ELASTIC_HOST = os.environ.get('ELASTIC_HOST', 'localhost')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT', '9200')

ALL_INDICES = [
    'dataset',
    'reuse',
    'organization',
    'dataservice',
]


def set_alias(search_client: ElasticClient, index_suffix_name: str, indices: List[str] = None, delete: bool = True) -> None:
    '''
    Properly end an indexation by swapping alias.
    Previous alias is deleted if needed.
    Alias will be removed from previous indices and set to
    the new indices (that match the suffix `index_suffix_name`).
    A list of indices on which to apply the alias migration can be passed.
    See an example logic https://github.com/elastic/elasticsearch-dsl-py/blob/master/examples/alias_migration.py
    '''

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
            if search_client.es.indices.exists_alias(name=index_alias):
                previous_indices = search_client.es.indices.get_alias(name=index_alias).keys()

        search_client.es.indices.update_aliases(
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
                    search_client.es.indices.delete(index=previous_index)
