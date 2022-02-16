import logging
import os

from elasticsearch import Elasticsearch

ELASTIC_HOST = os.environ.get('ELASTIC_HOST', 'localhost')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT', '9200')

INDICES = [
    'dataset',
    'reuse',
    'organization',
]


def set_alias(index_suffix_name, delete=True, timeout=None):
    '''
    Properly end an indexation by swapping alias.
    Previous alias is deleted if needed.
    Alias will be removed from previous indices and set to
    the new indices (that match the suffix `index_suffix_name`)
    '''
    es = Elasticsearch([{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}])

    for index_alias in INDICES:
        index_name = index_alias + '-' + index_suffix_name
        logging.info('Creating alias "%s" on index "%s"', index_alias, index_name)
        if es.indices.exists_alias(name=index_alias):
            alias = es.indices.get_alias(name=index_alias)
            previous_indices = alias.keys()
            if index_name not in previous_indices:
                es.indices.put_alias(index=index_name, name=index_alias,
                                     request_timeout=timeout)
            for previous_index in previous_indices:
                if previous_index != index_name:
                    es.indices.delete_alias(index=previous_index, name=index_alias,
                                            request_timeout=timeout)
                    if delete:
                        es.indices.delete(index=previous_index, request_timeout=timeout)
        else:
            es.indices.put_alias(index=index_name, name=index_alias,
                                 request_timeout=timeout)
