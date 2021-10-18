"""
    Tool used to stress test k-NN plugin
"""
import json
import sys

from opensearchpy import OpenSearch, RequestsHttpConnection
import random


def get_connection(url):
    return OpenSearch(
        hosts=[{'host': url, 'port': 80}],
        use_ssl=False,
        verify_certs=False,
        scheme="http",
        port=80,
        connection_class=RequestsHttpConnection
    )


def bulk_ingest_random_data(osearch, index_name, field_name, doc_count, bulk_size, dimension):
    print("Indexing {} documents for index: {}".format(doc_count, index_name))

    bulk_data = []
    for i in range(doc_count):
        op_dict = {"index": {
            "_index": index_name,
        }, "_id": str(i)}
        bulk_data.append(op_dict)
        bulk_data.append({field_name: [random.random() for _ in range(dimension)]})
        if len(bulk_data) >= 2 * bulk_size:
            print("Index count: {}".format(i+1))
            osearch.bulk(index=index_name, body=bulk_data, request_timeout=30)
            bulk_data = list()

    if len(bulk_data) >= 0:
        osearch.bulk(index=index_name, body=bulk_data, request_timeout=30)

    osearch.indices.refresh(index_name)


def create_query(field_name, vector, k, size):
    return json.dumps({
        "size": size,
        "query": {
            "knn": {
                field_name: {
                    "vector": vector,
                    "k": k
                }
            }
        }
    })


def run_queries(osearch, index_name, field_name, k, size, num_queries, dimension):
    print("Running {} queries with k={} against index: {}".format(num_queries, k, index_name))
    for i in range(num_queries):
        if (i + 1) % 100 == 0:
            print("[Query] Running query #{}".format((i + 1)))

        _ = osearch.search(
            index=index_name, body=create_query(field_name, [random.random() for _ in range(dimension)], k, size),
            timeout="90s"
        )


def main(argv):
    url = argv[1]
    case = argv[2]

    print("Testing: ", url)
    osearch = get_connection(url)
    if case == "ingest":
        index_name = argv[3]
        field_name = argv[4]
        dimension = int(argv[5])
        doc_count = int(argv[6])
        bulk_ingest_random_data(osearch, index_name, field_name, doc_count, 300, dimension)
    elif case == "search":
        index_name = argv[3]
        field_name = argv[4]
        dimension = int(argv[5])
        k = int(argv[6])
        size = int(argv[7])
        num_queries = int(argv[8])
        run_queries(osearch, index_name, field_name, k, size, num_queries, dimension)


if __name__ == "__main__":
    main(sys.argv)
