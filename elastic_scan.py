import logging
import socket
from dask import delayed,  bag as db
from elasticsearch import Elasticsearch


AGGREGATION = 'aggregation'
HITS = 'hits'


def __get_npartitions(doc_count, scroll_size):
    """
    Determine the amount of scrolls required to load all data, which is also the amount of partitions

    :param doc_count:
    :type doc_count: int

    :param scroll_size:
    :type scroll_size: int

    :returns: The required amount of partitions
    :rtype: int
    """
    return (doc_count // scroll_size) + 1


def __elastic_scanner(index, client, auth=('', ''), query={}, scroll_size=10000, timeout='1m', responsetype='hits'):
    """
    Create a index scanning thread

    :param index: An index to scan
    :type index: str

    :param client: A list of addresses or a string
    :type client: list, str

    :param auth: A tuple in the form of (USERNAME, PASSWORD)
    :type auth: tuple

    :param query: A query to execute for filtering
    :type query: dict

    :param scroll_size: Maximum amount of docs returned per iteration
    :type scroll_size: int

    :param timeout: How long to keep the scroll session open
    :type timeout: str

    :rtype: Generator
    """
    # Connect to Elasticsearch
    _es = Elasticsearch(hosts=client, http_auth=auth)

    # Initialise scroll
    if responsetype == HITS:
        for hit in _es.search(index=index, size=scroll_size, scroll=timeout, body=query)['hits']['hits']:
            try:
                yield hit['_source']
            except Exception as e:
                logging.error(f'Failed to yield scroll _source: {e}')
    elif responsetype == AGGREGATION:
        try:
            yield {'index': index, 'aggregation': _es.search(index=index, size=scroll_size, scroll=timeout, body=query)['aggregations']}
        except Exception as e:
            logging.error(f'Failed to yield aggregation response: {e}')


def __get_doc_count(index, client, auth=('', ''), query={}):
    """
    Request the amount of documents that will be retrieved

    :param index: An index to scan
    :type index: str

    :param client: A list of addresses or a string
    :type client: list, str

    :param query: A query to execute for filtering
    :type query: dict

    :param auth: A tuple in the form of (USERNAME, PASSWORD)
    :type auth: tuple

    :rtype: int
    """
    return int(Elasticsearch(hosts=client, http_auth=auth).search(index=index, body=query, size=0)['hits']['total'])


def __scan(index, client, auth=('', ''), query={}, scroll_size=10000, timeout='1m', responsetype=HITS):
    """
    Scan an Elasticsearch index

    :param index: An index to scan
    :type index: str

    :param client: A list of addresses or a string
    :type client: list, str

    :param query: A query to execute for filtering
    :type query: dict

    :param auth: A tuple in the form of (USERNAME, PASSWORD)
    :type auth: tuple

    :param scroll_size: Maximum amount of docs returned per iteration
    :type scroll_size: int

    :param timeout: How long to keep the scroll session open
    :type timeout: str

    :rtype: list
    """
    # Connect to Elasticsearch
    return list(__elastic_scanner(index, client, auth=auth, query=query, scroll_size=scroll_size, timeout='1m', responsetype=responsetype))


def __add_slice(query, sliceid, slicemax):
    """
    Add the scroll slice to a query

    :param query: The current query to execute
    :type query: dict

    :param sliceid: The scroll index
    :type sliceid: int

    :param slicemax: The total amount of scrolls
    :type slicemax: int

    :rtype: dict
    """
    query['slice'] = {
        'id': sliceid,
        'max': slicemax
    }
    return query


def scan_index(index, client, auth=('', ''), query={}, scroll_size=10000, timeout='1m', responsetype=HITS):
    """
    Scan a complete index

    :param index: An index to scan
    :type index: str

    :param client: A list of addresses or a string, or Elasticsearch object
    :type client: [list, str]

    :param auth: A tuple in the form of (USERNAME, PASSWORD)
    :type auth: tuple

    :param query: A query to execute for filtering
    :type query: dict

    :param scroll_size: Maximum amount of docs returned per iteration
    :type scroll_size: int

    :param timeout: How long to keep the scroll session open
    :type timeout: str

    :rtype: delayed
    """
    _dataholder = []
    _doc_count = __get_doc_count(
        index=index, client=client, auth=auth, query=query)

    if scroll_size < 1:
        scroll_size = 10000
        logging.warning(
            f'Logging size can\'t be below 1 doc, reverting to default ({scroll_size})')

    if responsetype == HITS:
        # Count partitions for shard
        n_partitions = __get_npartitions(_doc_count, scroll_size)
        if n_partitions > 1:
            for _sliceidx in range(n_partitions):
                if _doc_count >= scroll_size:
                    # Add slicing to the query
                    _q = __add_slice(query=query, sliceid=_sliceidx,
                                     slicemax=n_partitions)
                    _dataholder.append(
                        delayed(__scan)(
                            index=index,
                            client=client,
                            query=_q,
                            scroll_size=scroll_size,
                            timeout=timeout,
                            responsetype=responsetype,
                            auth=auth
                        )
                    )
                    _doc_count -= scroll_size
                elif _doc_count > 0:
                    # Add slicing to the query
                    _q = __add_slice(query=query,
                                     sliceid=_sliceidx,
                                     slicemax=n_partitions)
                    _dataholder.append(
                        delayed(__scan)(
                            index=index,
                            client=client,
                            query=_q,
                            scroll_size=_doc_count,
                            timeout=timeout,
                            responsetype=responsetype,
                            auth=auth
                        )
                    )
        else:
            _dataholder.append(
                delayed(__scan)(
                    index=index,
                    client=client,
                    query=query,
                    scroll_size=_doc_count,
                    timeout=timeout,
                    responsetype=responsetype,
                    auth=auth
                )
            )
    elif responsetype == AGGREGATION:
        _dataholder.append(
            delayed(__scan)(
                index=index,
                client=client,
                query=query,
                scroll_size=scroll_size,
                timeout=timeout,
                responsetype=responsetype,
                auth=auth
            )
        )

    return _dataholder
