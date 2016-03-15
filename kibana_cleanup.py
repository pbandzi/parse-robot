#! /usr/bin/env python
import logging
import argparse
import shared_utils
import json
import urlparse

logger = logging.getLogger('clear_kibana')
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('/var/log/{}.log'.format(__name__))
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(file_handler)


def delete_all(url):
    ids = shared_utils.get_elastic_data(url, body=None, field='_id')
    for id in ids:
        del_url = '/'.join([url, id])
        shared_utils.delete_request(del_url)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create Kibana dashboards from data in elasticsearch')
    parser.add_argument('-e', '--elasticsearch-url', default='http://localhost:9200',
                        help='the url of elasticsearch, defaults to http://localhost:9200')

    args = parser.parse_args()
    base_elastic_url = args.elasticsearch_url

    urls = (urlparse.urljoin(base_elastic_url, '/.kibana/visualization'),
            urlparse.urljoin(base_elastic_url, '/.kibana/dashboard'),
            urlparse.urljoin(base_elastic_url, '/.kibana/search'))

    for url in urls:
        delete_all(url)
