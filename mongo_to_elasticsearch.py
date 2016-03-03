#! /usr/bin/env python
import logging
import argparse
import json
import urllib3


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
        logging.info("No 'result' from 'sig_test' found in vIMS details, skipping")
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
        logging.info("No results found in 'FUNCvirNet' part of ONOS results")
        return False
    elif len(funcvirnetl3_statuses) < 0:
        logging.info("No results found in 'FUNCvirNetL3' part of ONOS results")
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
            'duration': testcase_details['FUNCvirNet']['duration'],
            'tests': funcvirnet_all,
            'failures': funcvirnet_failed
        }

        testcase_details['FUNCvirNetL3'] = {
            'duration': testcase_details['FUNCvirNetL3']['duration'],
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
        logging.info("Found zero or more than one 'summaries' in Rally details, skipping")
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
        logging.info("No 'test_status' found in ODL details, skipping")
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
                logging.info("Skipping testcase with mongo _id '{}' because the testcase was missing value"
                             " for mandatory field '{}'".format(mongo_id, key))
                return False
            else:
                mandatory_fields.remove(key)
        elif key in mandatory_fields_to_modify:
            if value is None:
                # empty mandatory field, invalid input
                logging.info("Skipping testcase with mongo _id '{}' because the testcase was missing value"
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
        logging.info("Skipping testcase with mongo _id '{}' because the testcase was missing"
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


if __name__ == '__main__':
    logging.basicConfig(filename='/var/log/mongo2elastic.log', format='%(asctime)s %(levelname)s: %(message)s',
                        level=logging.DEBUG)
    parser = argparse.ArgumentParser(description='Modify and filter mongo json data for elasticsearch')
    parser.add_argument('input',
                        help='Input json file to modify')

    parser.add_argument('-od', '--output_destination',
                        default='elasticsearch',
                        help='Supported destinations are stdout and elasticsearch, defaults to elasticsearch')

    args = parser.parse_args()
    input_json_path = args.input
    output_destination = args.output_destination

    with open(input_json_path) as input_json_fdesc:
        input_json = json.load(input_json_fdesc)

    test_results_section = input_json['test_results']

    parsed_test_results = []

    for test_result in test_results_section:
        if modify_mongo_entry(test_result):
            # if the modification could be applied, append the modified result
            parsed_test_results.append(test_result)

    http = urllib3.PoolManager()
    for parsed_test_result in parsed_test_results:
        json_dump = json.dumps(parsed_test_result)
        if output_destination == 'stdout':
            print json_dump
        else:
            http.request('POST', 'http://localhost:9200/test_results/mongo2elastic/', body=json_dump)
