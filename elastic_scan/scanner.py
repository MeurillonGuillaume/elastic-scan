import logging
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
    _res = (doc_count // scroll_size) + 1
    logging.info(f'Scan will create {_res} partitions')
    return _res


def __elastic_scanner(index, client, query={}, scroll_size=10000, timeout='1m', responsetype='hits'):
    """
    Create a index scanning thread

    :param index: An index to scan
    :type index: str

    :param client_address: A list of addresses or a string
    :type client_address: list, str

    :param query: A query to execute for filtering
    :type query: dict

    :param scroll_size: Maximum amount of docs returned per iteration
    :type scroll_size: int

    :param timeout: How long to keep the scroll session open
    :type timeout: str

    :rtype: Generator
    """
    # Connect to Elasticsearch
    _es = Elasticsearch(client)

    # Initialise scroll
    if responsetype == HITS:
        for hit in _es.search(index=index, size=scroll_size, scroll=timeout, body=query)['hits']['hits']:
            try:
                yield hit['_source']
            except Exception as e:
                logging.error(e)
    elif responsetype == AGGREGATION:
        try:
            yield {'index': index, 'aggregation': _es.search(index=index, size=scroll_size, scroll=timeout, body=query)['aggregations']}
        except Exception as e:
            logging.error(e)


def __scan(index, client, query={}, scroll_size=10000, timeout='1m', responsetype=HITS):
    """
    Scan an Elasticsearch index

    :param index: An index to scan
    :type index: str

    :param client_address: A list of addresses or a string
    :type client_address: list, str

    :param query: A query to execute for filtering
    :type query: dict

    :param scroll_size: Maximum amount of docs returned per iteration
    :type scroll_size: int

    :param timeout: How long to keep the scroll session open
    :type timeout: str

    :rtype: list
    """
    # Connect to Elasticsearch
    _es = Elasticsearch(client)
    return list(__elastic_scanner(index, client, query=query, scroll_size=scroll_size, timeout='1m', responsetype=responsetype))


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


def scan_index(index, client_address, query={}, scroll_size=10000, n_partitions=None, timeout='1m', responsetype=HITS):
    """
    Scan a complete index

    :param index: An index to scan
    :type index: str

    :param client_address: A list of addresses or a string
    :type client_address: list, str

    :param query: A query to execute for filtering
    :type query: dict

    :param scroll_size: Maximum amount of docs returned per iteration
    :type scroll_size: int

    :param timeout: How long to keep the scroll session open
    :type timeout: str

    :rtype: delayed
    """

    _dataholder = []
    _shards = __get_shard_info(index=index, client=client_address)

    if responsetype == HITS:
        for _shard in _shards:
            # Count partitions for shard
            n_partitions = __get_npartitions(
                _shard['docCount'], scroll_size)
            _tmp_size = _shard['docCount']
            for _sliceidx in range(n_partitions):
                if _tmp_size >= scroll_size:
                    # Add slicing to the query
                    _q = __add_slice(query=query,
                                     sliceid=_sliceidx,
                                     slicemax=n_partitions)
                    _dataholder.append(
                        delayed(__scan)(
                            index=_shard['index'],
                            client=f"http://{_shard['endpoint']}:9200",
                            query=_q,
                            scroll_size=scroll_size,
                            timeout=timeout,
                            responsetype=responsetype
                        )
                    )
                    _tmp_size -= scroll_size
                else:
                    # Add slicing to the query
                    _q = __add_slice(query=query,
                                     sliceid=_sliceidx,
                                     slicemax=n_partitions)
                    _dataholder.append(
                        delayed(__scan)(
                            index=_shard['index'],
                            client=f"http://{_shard['endpoint']}:9200",
                            query=_q,
                            scroll_size=_tmp_size,
                            timeout=timeout,
                            responsetype=responsetype
                        )
                    )
                    _tmp_size -= _tmp_size
    elif responsetype == AGGREGATION:
        _unique_indices = list(set([_shard['index'] for _shard in _shards]))
        for _idx in _unique_indices:
            _dataholder.append(
                delayed(__scan)(
                    index=_idx,
                    client=client_address,
                    query=query,
                    scroll_size=scroll_size,
                    timeout=timeout,
                    responsetype=responsetype
                )
            )

    return _dataholder


def __get_shard_info(index, client):
    """
    Request info about the shards used for an index or alias

    :param index: The name of the index or alias
    :type index: str

    :param client: The endpoint to request info from
    :type client: str, list

    :rtype: list
    :returns: A list of endpoints with information required to query a primary shard of an index
    """
    return [{
        'shardID': int(shard['shard']),
        'docCount': int(shard['docs']),
        'endpoint': shard['ip'],
        'index': shard['index']
    } for shard in Elasticsearch(client).cat.shards(index=index, format='json')
        if shard['prirep'] == 'p' and int(shard['docs']) > 0]
