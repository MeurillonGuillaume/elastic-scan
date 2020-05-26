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
    _res = (doc_count // scroll_size) + 1
    logging.info(f'Scan will create {_res} partitions')
    return _res


def __elastic_scanner(index, client, auth=('', ''), query={}, scroll_size=10000, timeout='1m', responsetype='hits'):
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
    _es = Elasticsearch(hosts=client, http_auth=auth)

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


def __get_doc_count(index, client, auth, query):
    return Elasticsearch(hosts=client, http_auth=auth).search(index=index, body=query, size=0)['hits']['total']


def __scan(index, client, auth=('', ''), query={}, scroll_size=10000, timeout='1m', responsetype=HITS):
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


def __get_shard_info(index, client, auth=('', '')):
    """
    Request info about the shards used for an index or alias

    :param index: The name of the index or alias
    :type index: str

    :param client: The endpoint to request info from
    :type client: str, list

    :rtype: list
    :returns: A list of endpoints with information required to query a primary shard of an index
    """
    return [
        {
            'shardID': int(shard['shard']),
            'docCount': int(shard['docs']),
            'endpoint': shard['ip'],
            'index': shard['index']
        } for shard in Elasticsearch(hosts=client, http_auth=auth).cat.shards(index=index, format='json') if shard['prirep'] == 'p' and int(shard['docs']) > 0]


def __get_hostname_ips(client_addresses):
    _result = {}
    if isinstance(client_addresses, list):
        for _address in client_addresses:
            _address = str(_address)
            _key = _address

            # Replace preposition
            for _filter in ['https', 'http', '://']:
                _address = _address.replace(_filter, '')

            # Replace eventual sockets
            if len(_address.split(':')) > 1:
                for _tmp in _address.split(':'):
                    if len(_tmp) > 1:
                        _address = _tmp
                        break

            _result[socket.gethostbyname(_address)] = _key
    return _result


def scan_index(index, client, auth=('', ''), query={}, scroll_size=10000, n_partitions=None, timeout='1m', responsetype=HITS):
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
    _hostname_ips = __get_hostname_ips(client_addresses=client)
    _doc_count = __get_doc_count(
        index=index, client=client, auth=auth, query=query)

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
                else:
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
