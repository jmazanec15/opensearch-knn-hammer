"""
    Tool used to stress test k-NN plugin
"""
import json
import sys

import requests
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


def create_index_from_model(osearch, index_name, field_name, model_id):
    print("Creating index from model...")

    # Hard code mapping and settings
    settings = {
        "number_of_shards": 3,
        "number_of_replicas": 1,
        "index": {
            "knn": True
        }
    }

    mapping = {
        "properties": {
            field_name: {
                "type": "knn_vector",
                "model_id": model_id
            }
        }
    }

    print("Deleting index if it exists...")
    osearch.indices.delete(index=index_name, ignore=[400, 404])
    request_body = {
        'settings': settings,
        'mappings': mapping
    }
    osearch.indices.create(index=index_name, body=request_body)


def create_default_index(osearch, index_name, field_name, dimension):
    print("Creating default index...")

    # Hard code mapping and settings
    settings = {
        "number_of_shards": 3,
        "number_of_replicas": 1,
    }

    mapping = {
        "properties": {
            field_name: {
                "type": "knn_vector",
                "dimension": dimension
            }
        }
    }

    print("Deleting index if it exists...")
    osearch.indices.delete(index=index_name, ignore=[400, 404])
    request_body = {
        'settings': settings,
        'mappings': mapping
    }
    osearch.indices.create(index=index_name, body=request_body)


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

    if len(bulk_data) >= bulk_size:
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


def train(url, training_index, training_field, dimension, model_id, description):
    body = {
        "training_index": training_index,
        "training_field": training_field,
        "dimension": dimension,
        "description": description,
        "method": {
            "name": "ivf",
            "engine": "faiss",
            "space_type": "l2",
            "parameters": {
                "nlist": 512,
                "encoder": {
                    "name": "pq",
                    "parameters": {
                        "code_size": 8,
                        "code_count": 32
                    }
                }
            }
        }
    }

    print((requests.post("http://" + url + "/_plugins/_knn/models/" + model_id + "/_train", json.dumps(body), headers={"content-type": "application/json"})).json())


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
    if case == "add_data":
        index_name = argv[3]
        field_name = argv[4]
        dimension = int(argv[5])
        doc_count = int(argv[6])
        bulk_ingest_random_data(osearch, index_name, field_name, doc_count, 300, dimension)
    elif case == "add_train_data":
        index_name = argv[3]
        field_name = argv[4]
        dimension = int(argv[5])
        create_default_index(osearch, index_name, field_name, dimension)
        doc_count = int(argv[6])
        bulk_ingest_random_data(osearch, index_name, field_name, doc_count, 300, dimension)
    elif case == "stats":
        r = requests.get(url="http://" + url + "/_opendistro/_knn/stats")
        print(json.dumps(r.json(), indent=2))
    elif case == "train":
        index_name = argv[3]
        field_name = argv[4]
        dimension = int(argv[5])
        model_id = argv[6]
        description = argv[7]
        train(url, index_name, field_name, dimension, model_id, description)
    elif case == "create_model_index":
        index_name = argv[3]
        field_name = argv[4]
        dimension = int(argv[5])
        model_id = argv[6]
        create_index_from_model(osearch, index_name, field_name, model_id)
        doc_count = int(argv[7])
        bulk_ingest_random_data(osearch, index_name, field_name, doc_count, 300, dimension)
    elif case == "search":
        index_name = argv[3]
        field_name = argv[4]
        dimension = int(argv[5])
        k = int(argv[6])
        size = int(argv[7])
        num_queries = int(argv[8])
        run_queries(osearch, index_name, field_name, k, size, num_queries, dimension)
    elif case == "nodes":
        print(osearch.cat.nodes())
    elif case == "indices":
        print(osearch.cat.indices())
    elif case == "mapping":
        pass
    elif case == "settings":
        pass
    elif case == "get_model":
        model_id = argv[3]
        r = requests.get(url="http://" + url + "/_plugins/_knn/models/" + model_id)
        print(json.dumps(r.json(), indent=2))
    elif case == "delete_model":
        model_id = argv[3]
        r = requests.delete(url="http://" + url + "/_plugins/_knn/models/" + model_id)
        print(json.dumps(r.json(), indent=2))


    # TODO: Add delete index, get mapping,

if __name__ == "__main__":
    main(sys.argv)
