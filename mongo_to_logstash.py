#! /usr/bin/env python
import logging
import argparse
import json
import uuid
import copy

logging.basicConfig(filename='/var/log/mongo2elk.log', format='%(asctime)s %(levelname)s: %(message)s', level=logging.DEBUG)

conflicting_fiels = {'_id', '_type', '_index', '_score', '_source'}


def rename_conflicting_fields(json_obj):
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            if key in conflicting_fiels:
                # rename
                prefix = 'mongo' + key
                new_key = 'mongo' + key
                while new_key in json_obj:
                    new_key = prefix + str(uuid.uuid4())

                json_obj[new_key] = value
                del json_obj[key]
            rename_conflicting_fields(value)
    elif isinstance(json_obj, list):
        for json_section in json_obj:
            rename_conflicting_fields(json_section)


def analyze_testcases(test_results_section):
    case_names = {}
    result_types = []
    result_fields = {}
    for test_result in test_results_section:
        case_name = test_result['case_name']
        if case_name in case_names:
            case_names[case_name] += 1
        else:
            case_names[case_name] = 1
            result_types.append(test_result)

    for test_result in result_types:
        for field in test_result.iterkeys():
            if field in result_fields:
                result_fields[field] += 1
            else:
                result_fields[field] = 1

    logging.info("Number of different case names: {}\n".format(len(case_names)))
    for case_name, occurrences in case_names.iteritems():
        logging.info("Case name '{}' occurred {} times".format(case_name, occurrences))
    logging.info('')
    for field, occurrences in result_fields.iteritems():
        logging.info("Field '{}' occurred {} times".format(field, occurrences))
    logging.info('')


def split_testcases(test_result):
    """
    1. search for all lists in test_result
    2. ignore 0 length lists, flatten 1 length lists
    3. split longer lists, create new json for each list entry and repeat for each json

    :param test_result: json object
    :return:
    """
    list_of_split_test_results = []
    def process_lists(test_result):
        long_lists = []
        get_lists_from_children((test_result, 'details'), test_result['details'], long_lists)
        remaining_lists = []

        # leave 0 length lists, flatten 1 length lists and process >2 length lists
        for (parent, key) in long_lists:
            long_list = parent[key]
            if len(long_list) == 1:
                # flatten
                parent[key] = {}
                long_list_value = long_list[0]
                if isinstance(long_list_value, dict):
                    for single_list_key, single_list_value in long_list_value.iteritems():
                        parent[key][single_list_key] = single_list_value
                else:
                    parent[key] = long_list_value
            elif len(long_list) > 1:
                remaining_lists.append((parent, key))

        return remaining_lists

    def get_lists_from_children(parent, json_obj, stored_lists):
        if isinstance(json_obj, dict):
            for key, value in json_obj.iteritems():
                get_lists_from_children((json_obj, key), value, stored_lists)
        elif isinstance(json_obj, list):
            stored_lists.append(parent)
            for list_child in json_obj:
                get_lists_from_children((json_obj, json_obj.index(list_child)), list_child, stored_lists)

    def split_list_into_categories(list_of_dictionaries):
        # return a list of lists of dictionaries, each list represents one category
        categories = []
        for dictionary in list_of_dictionaries:
            categorized = False
            for category in categories:
                # if a match is found, append to category, which is a list
                category_representative = category[0]
                if set(category_representative.keys()) == set(dictionary.keys()):
                    category.append(dictionary)
                    categorized = True
                    break

            # if a match is not found, create a new category, which is a list
            if not categorized:
                categories.append([dictionary])

        return categories

    # this list of tuples contains parents with child lists with length > 1
    remaining_lists = process_lists(test_result)

    if len(remaining_lists) == 0:
        list_of_split_test_results.append(test_result)
        # print json.dumps(test_result)
    else:
        remaining_list_parent, remaining_list_key = remaining_lists.pop(0)
        # split entries into categories by looking at their dictionary keys
        # only one category should contain more than one entry
        # split this category and append the entries from other categories (somehow, if possible)
        remaining_list = remaining_list_parent[remaining_list_key]
        for remaining_list_entry in remaining_list:
            remaining_list_parent[remaining_list_key] = remaining_list_entry
            t = split_testcases(copy.deepcopy(test_result))
            list_of_split_test_results.extend(t)

    return list_of_split_test_results

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Modify mongo json dump for logstash')
    parser.add_argument('input', help='Input json file to modify')
    args = parser.parse_args()
    input_json_path = args.input

    with open(input_json_path) as input_json_fdesc:
        input_json = json.load(input_json_fdesc)

    test_results_section = input_json['test_results']

    analyze_testcases(test_results_section)

    parsed_test_results = []

    for test_result in test_results_section:
        rename_conflicting_fields(test_result)

        new_test_results = split_testcases(test_result)
        parsed_test_results.extend(new_test_results)

    for parsed_test_result in parsed_test_results:
        print json.dumps(parsed_test_result, indent=2)
