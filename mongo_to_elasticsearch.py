#! /usr/bin/env python
import logging
import argparse
import json
import urllib3
import urlparse

logger = logging.getLogger('mongo_to_elasticsearch')
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('/var/log/mongo2elastic.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
logger.addHandler(file_handler)


def _get_dicts_from_list(dict_list, keys):
    dicts = []
    for dictionary in dict_list:
        # iterate over dictionaries in input list
        if keys == set(dictionary.keys()):
            # check the dictionary structure
            dicts.append(dictionary)
    return dicts


def _get_results_from_list_of_dicts(list_of_dict_statuses, dict_indexes, expected_results=None):
    test_results = {}
    for test_status in list_of_dict_statuses:
        status = test_status
        for index in dict_indexes:
            status = status[index]
        if status in test_results:
            test_results[status] += 1
        else:
            test_results[status] = 1

    if expected_results is not None:
        for expected_result in expected_results:
            if expected_result not in test_results:
                test_results[expected_result] = 0

    return test_results


def _convert_duration(duration_string):
    hours, minutes, seconds = duration_string.split(":")
    int_duration = 3600 * int(hours) + 60 * int(minutes) + float(seconds)
    return int_duration


def modify_functest_vims(testcase):
    """
    Structure:
        details.sig_test.result.[{result}]
        details.sig_test.duration
        details.vIMS.duration
        details.orchestrator.duration

    Find data for these fields
        -> details.sig_test.duration
        -> details.sig_test.tests
        -> details.sig_test.failures
        -> details.sig_test.passed
        -> details.sig_test.skipped
        -> details.vIMS.duration
        -> details.orchestrator.duration
    """
    testcase_details = testcase['details']
    sig_test_results = _get_dicts_from_list(testcase_details['sig_test']['result'],
                                            {'duration', 'result', 'name', 'error'})
    if len(sig_test_results) < 1:
        logger.info("No 'result' from 'sig_test' found in vIMS details, skipping")
        return False
    else:
        test_results = _get_results_from_list_of_dicts(sig_test_results, ('result',), ('Passed', 'Skipped', 'Failed'))
        passed = test_results['Passed']
        skipped = test_results['Skipped']
        failures = test_results['Failed']
        all_tests = passed + skipped + failures
        testcase['details'] = {
            'sig_test': {
                'duration': testcase_details['sig_test']['duration'],
                'tests': all_tests,
                'failures': failures,
                'passed': passed,
                'skipped': skipped
            },
            'vIMS': {
                'duration': testcase_details['vIMS']['duration']
            },
            'orchestrator': {
                'duration': testcase_details['orchestrator']['duration']
            }
        }
        return True


def modify_functest_onos(testcase):
    """
    Structure:
        details.FUNCvirNet.duration
        details.FUNCvirNet.status.[{Case result}]
        details.FUNCvirNetL3.duration
        details.FUNCvirNetL3.status.[{Case result}]

    Find data for these fields
        -> details.FUNCvirNet.duration
        -> details.FUNCvirNet.tests
        -> details.FUNCvirNet.failures
        -> details.FUNCvirNetL3.duration
        -> details.FUNCvirNetL3.tests
        -> details.FUNCvirNetL3.failures
    """
    testcase_details = testcase['details']

    funcvirnet_details = testcase_details['FUNCvirNet']['status']
    funcvirnet_statuses = _get_dicts_from_list(funcvirnet_details, {'Case result', 'Case name:'})

    funcvirnetl3_details = testcase_details['FUNCvirNetL3']['status']
    funcvirnetl3_statuses = _get_dicts_from_list(funcvirnetl3_details, {'Case result', 'Case name:'})

    if len(funcvirnet_statuses) < 0:
        logger.info("No results found in 'FUNCvirNet' part of ONOS results")
        return False
    elif len(funcvirnetl3_statuses) < 0:
        logger.info("No results found in 'FUNCvirNetL3' part of ONOS results")
        return False
    else:
        funcvirnet_results = _get_results_from_list_of_dicts(funcvirnet_statuses,
                                                             ('Case result',), ('PASS', 'FAIL'))
        funcvirnetl3_results = _get_results_from_list_of_dicts(funcvirnetl3_statuses,
                                                               ('Case result',), ('PASS', 'FAIL'))

        funcvirnet_passed = funcvirnet_results['PASS']
        funcvirnet_failed = funcvirnet_results['FAIL']
        funcvirnet_all = funcvirnet_passed + funcvirnet_failed

        funcvirnetl3_passed = funcvirnetl3_results['PASS']
        funcvirnetl3_failed = funcvirnetl3_results['FAIL']
        funcvirnetl3_all = funcvirnetl3_passed + funcvirnetl3_failed

        testcase_details['FUNCvirNet'] = {
            'duration': _convert_duration(testcase_details['FUNCvirNet']['duration']),
            'tests': funcvirnet_all,
            'failures': funcvirnet_failed
        }

        testcase_details['FUNCvirNetL3'] = {
            'duration': _convert_duration(testcase_details['FUNCvirNetL3']['duration']),
            'tests': funcvirnetl3_all,
            'failures': funcvirnetl3_failed
        }

        return True


def modify_functest_rally(testcase):
    """
    Structure:
        details.[{summary.duration}]
        details.[{summary.nb success}]
        details.[{summary.nb tests}]

    Find data for these fields
        -> details.duration
        -> details.tests
        -> details.success_percentage
    """
    summaries = _get_dicts_from_list(testcase['details'], {'summary'})

    if len(summaries) != 1:
        logger.info("Found zero or more than one 'summaries' in Rally details, skipping")
        return False
    else:
        summary = summaries[0]['summary']
        testcase['details'] = {
            'duration': summary['duration'],
            'tests': summary['nb tests'],
            'success_percentage': summary['nb success']
        }
        return True


def modify_functest_odl(testcase):
    """
    Structure:
        details.details.[{test_status.@status}]

    Find data for these fields
        -> details.tests
        -> details.failures
        -> details.success_percentage?
    """
    test_statuses = _get_dicts_from_list(testcase['details']['details'], {'test_status', 'test_doc', 'test_name'})
    if len(test_statuses) < 1:
        logger.info("No 'test_status' found in ODL details, skipping")
        return False
    else:
        test_results = _get_results_from_list_of_dicts(test_statuses, ('test_status', '@status'), ('PASS', 'FAIL'))

        passed_tests = test_results['PASS']
        failed_tests = test_results['FAIL']
        all_tests = passed_tests + failed_tests

        testcase['details'] = {
            'tests': all_tests,
            'failures': failed_tests,
            'success_percentage': 100 * passed_tests / float(all_tests)
        }
        return True


def modify_default_entry(testcase):
    """
    Look for these and leave any of those:
        details.duration
        details.tests
        details.failures

    If none are present, then return False
    """
    found = False
    testcase_details = testcase['details']
    fields = ['duration', 'tests', 'failures']
    if isinstance(testcase_details, dict):
        for key, value in testcase_details.items():
            if key in fields:
                found = True
            else:
                del testcase_details[key]

    return found


def verify_mongo_entry(testcase):
    """
    Mandatory fields:
        installer
        pod_name
        version
        case_name
        date
        project
        details

        these fields must be present and must NOT be None

    Optional fields:
        description

        these fields will be preserved if the are NOT None
    """
    mandatory_fields = ['installer',
                        'pod_name',
                        'version',
                        'case_name',
                        'project_name',
                        'details']
    mandatory_fields_to_modify = {'creation_date': lambda x: x.replace(' ', 'T')}
    if '_id' in testcase:
        mongo_id = testcase['_id']
    else:
        mongo_id = None
    optional_fields = ['description']
    for key, value in testcase.items():
        if key in mandatory_fields:
            if value is None:
                # empty mandatory field, invalid input
                logger.info("Skipping testcase with mongo _id '{}' because the testcase was missing value"
                            " for mandatory field '{}'".format(mongo_id, key))
                return False
            else:
                mandatory_fields.remove(key)
        elif key in mandatory_fields_to_modify:
            if value is None:
                # empty mandatory field, invalid input
                logger.info("Skipping testcase with mongo _id '{}' because the testcase was missing value"
                            " for mandatory field '{}'".format(mongo_id, key))
                return False
            else:
                testcase[key] = mandatory_fields_to_modify[key](value)
                del mandatory_fields_to_modify[key]
        elif key in optional_fields:
            if value is None:
                # empty optional field, remove
                del testcase[key]
            optional_fields.remove(key)
        else:
            # unknown field
            del testcase[key]

    if len(mandatory_fields) > 0:
        # some mandatory fields are missing
        logger.info("Skipping testcase with mongo _id '{}' because the testcase was missing"
                    " mandatory field(s) '{}'".format(mongo_id, mandatory_fields))
        return False
    else:
        return True


def modify_mongo_entry(testcase):
    # 1. verify and identify the testcase
    # 2. if modification is implemented, then use that
    # 3. if not, try to use default
    # 4. if 2 or 3 is successful, return True, otherwise return False
    if verify_mongo_entry(testcase):
        project = testcase['project_name']
        case_name = testcase['case_name']
        if project == 'functest':
            if case_name == 'Rally':
                return modify_functest_rally(testcase)
            elif case_name == 'ODL':
                return modify_functest_odl(testcase)
            elif case_name == 'ONOS':
                return modify_functest_onos(testcase)
            elif case_name == 'vIMS':
                return modify_functest_vims(testcase)
        return modify_default_entry(testcase)
    else:
        return False


def get_mongo_data(mongo_url, days, http):
    mongo_json = json.loads(http.request('GET', urlparse.urljoin(mongo_url, '?period={}'.format(days))).data)

    mongo_data = []
    for test_result in mongo_json['test_results']:
        if modify_mongo_entry(test_result):
            # if the modification could be applied, append the modified result
            mongo_data.append(test_result)
    return mongo_data


def get_elastic_data(elastic_url, days, http):
    body = '''{{
    "query" : {{
        "range" : {{
            "creation_date" : {{
                "gte" : "now-{}d"
            }}
        }}
    }}
}}'''.format(days)

    # 1. get the number of results
    elastic_json = json.loads(http.request('GET', elastic_url + '/_search?size=1', body=body).data)
    nr_of_hits = elastic_json['hits']['total']

    # 2. get all results
    elastic_json = json.loads(http.request('GET', elastic_url + '/_search?size={}'.format(nr_of_hits), body=body).data)

    elastic_data = []
    for hit in elastic_json['hits']['hits']:
        elastic_data.append(hit['_source'])
    return elastic_data


def get_difference(mongo_data, elastic_data):
    for elastic_entry in elastic_data:
        if elastic_entry in mongo_data:
            mongo_data.remove(elastic_entry)
    return mongo_data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Modify and filter mongo json data for elasticsearch')
    parser.add_argument('-od', '--output-destination',
                        default='elasticsearch',
                        choices=('elasticsearch', 'stdout'),
                        help='defaults to elasticsearch')

    parser.add_argument('-u', '--update', default=0, type=int, metavar='N',
                        help='get entries old at most N days from mongodb and'
                             ' parse those that are not already in elasticsearch.'
                             ' If not present, will get everything from mongodb, which is the default')

    parser.add_argument('-e', '--elasticsearch-url', default='http://localhost:9200',
                        help='the url of elasticsearch, defaults to http://localhost:9200')

    parser.add_argument('-m', '--mongodb-url', default='http://localhost:8082',
                        help='the url of mongodb, defaults to http://localhost:8082')

    args = parser.parse_args()
    base_elastic_url = urlparse.urljoin(args.elasticsearch_url, '/test_results/mongo2elastic')
    base_mongodb_url = urlparse.urljoin(args.mongodb_url, '/results')
    output_destination = args.output_destination
    update = args.update

    http = urllib3.PoolManager()

    # parsed_test_results will be printed/sent to elasticsearch
    parsed_test_results = []
    if update == 0:
        # TODO get everything from mongo
        pass
    elif update > 0:
        elastic_data = get_elastic_data(base_elastic_url, update, http)
        mongo_data = get_mongo_data(base_mongodb_url, update, http)
        parsed_test_results = get_difference(mongo_data, elastic_data)
    else:
        raise Exception('Update must be non-negative')

    logger.info('number of parsed test results: {}'.format(len(parsed_test_results)))

    for parsed_test_result in parsed_test_results:
        json_dump = json.dumps(parsed_test_result)
        if output_destination == 'stdout':
            print json_dump
        else:
            http.request('POST', base_elastic_url, body=json_dump)
