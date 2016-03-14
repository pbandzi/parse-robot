import urllib3
import json
http = urllib3.PoolManager()


def publish_json(json_ojb, output_destination):
    json_dump = json.dumps(json_ojb)
    if output_destination == 'stdout':
        print json_dump
    else:
        http.request('POST', output_destination, body=json_dump)


def _get_nr_of_hits(elastic_json):
    return elastic_json['hits']['total']


def get_elastic_data(elastic_url, body):
    # 1. get the number of results
    elastic_json = json.loads(http.request('GET', elastic_url + '/_search?size=0', body=body).data)
    nr_of_hits = _get_nr_of_hits(elastic_json)

    # 2. get all results
    elastic_json = json.loads(http.request('GET', elastic_url + '/_search?size={}'.format(nr_of_hits), body=body).data)

    elastic_data = []
    for hit in elastic_json['hits']['hits']:
        elastic_data.append(hit['_source'])
    return elastic_data
