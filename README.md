# Elasticsearch-scan

This repository contains Python3-code for scanning Elasticsearch indices.

## How does it work

Based on the no-longer functioning [Dask-elasticsearch](https://github.com/rmax/dask-elasticsearch) repository, which scrolls Elasticsearch indices in parallel using scrolling with slicing, based on the Elasticsearch `scan()` function ([see source](https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/helpers/actions.py#L386)).

Scan is not entirely adapted, it has been adapted to automatically generate partitions instead of accepting user-input for partition count. Each partition contains data from one scroll, which is 10000 records by default, and can be adapted if required.

The amount of partitions is either automatically generated, or manually submitted.

## Example

```python
from dask import bag as db
from dask.distributed import Client
from elastic_scan import scan_index

if __name__ == "__main__":
    client = Client() # Create local parallellised session
    data = db.from_delayed(scan_index(
        index='index-name-date',
        client='http://elastic_server:9200/',
        http_auth=(USERNAME, PASSWORD)
    )) # Create a distributed data structure from the index, this structure will be partitioned automatically
    client.rebalance(data) # Make sure data is distributed, required when running a cluster
    print(f'Bag holds {data.npartitions} partitions')
    local_result = data.compute() # Fetch results distributed + load locally
    print(local_result) # Show the results
```

## Execute on cluster

In order to execute the scanner on a cluster-based system, the Elasticsearch-scan package has to be installed on **every worker-node**, as wel the [Elasticsearch package](https://pypi.org/project/elasticsearch/).

## Result

When using the example above, the result will be a [Bag](https://examples.dask.org/bag.html) (low-level Dask data structure) with as many partitions as has been calculated before fetching the data. The Bag will be lazilly-loaded on the server-side, and partitions will be balanced across all available nodes.

On executing operations on this Bag that has been created, the first thing which will happen is the data will be fetches in parallel. After that, all operations will be executed.
