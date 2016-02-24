#! /usr/bin/env python
import logging
import argparse
import json
import uuid
import copy


conflicting_fields = {'_id', '_type', '_index', '_score', '_source'}


def rename_conflicting_fields(json_obj):
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            if key in conflicting_fields:
                # rename
                prefix = 'mongo' + key
                new_key = prefix
                while new_key in json_obj:
                    new_key = prefix + str(uuid.uuid4())

                json_obj[new_key] = value
                del json_obj[key]
            elif '.' in key:
                prefix = key.replace('.', ':')
                new_key = prefix
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


def _process_lists(parent, child_key):
    # leave 0 length lists, flatten 1 length lists and process >2 length lists
    processed_list = parent[child_key]
    if len(processed_list) == 1:
        # flatten
        parent[child_key] = {}
        processed_list_value = processed_list[0]
        if isinstance(processed_list_value, dict):
            # move one level up
            for single_list_key, single_list_value in processed_list_value.iteritems():
                parent[child_key][single_list_key] = single_list_value
        else:
            # either a list or string
            parent[child_key] = processed_list_value


def _get_list_from_children(parent, child_key):
    processed_child = parent[child_key]
    if isinstance(processed_child, dict):
        for key, value in processed_child.iteritems():
            returned_list = _get_list_from_children(processed_child, key)
            if returned_list is not None:
                return returned_list
    elif isinstance(processed_child, list):
        if len(processed_child) < 2:
            _process_lists(parent, child_key)
        else:
            return parent, child_key


def _shallow_dict_copy(dictionary):
    if isinstance(dictionary, dict):
        copied_dict = copy.copy(dictionary)
        for key, value in copied_dict.iteritems():
            copied_dict[key] = _shallow_dict_copy(value)
    else:
        copied_dict = dictionary

    return copied_dict


def split_testcases(test_result):
    """
    1. search for all lists in test_result
    2. ignore 0 length lists, flatten 1 length lists
    3. split longer lists, create new json for each list entry and repeat for each json

    :param test_result: json object
    :return:
    """
    # def split_list_into_categories(list_of_dictionaries):
    #     # return a list of lists of dictionaries, each list represents one category
    #     categories = []
    #     for dictionary in list_of_dictionaries:
    #         categorized = False
    #         for category in categories:
    #             # if a match is found, append to category, which is a list
    #             category_representative = category[0]
    #             if set(category_representative.keys()) == set(dictionary.keys()):
    #                 category.append(dictionary)
    #                 categorized = True
    #                 break
    #
    #         # if a match is not found, create a new category, which is a list
    #         if not categorized:
    #             categories.append([dictionary])
    #
    #     return categories

    list_of_split_test_results = []
    remaining_list = _get_list_from_children(test_result, 'details')
    if remaining_list is None:
        # no lists that need splitting were found
        list_of_split_test_results.append(_shallow_dict_copy(test_result))
    else:
        # this list of tuples contains parents with child lists with length > 1
        (remaining_list_parent, remaining_list_key) = remaining_list
        # split entries into categories by looking at their dictionary keys
        # only one category should contain more than one entry
        # split this category and append the entries from other categories (somehow, if possible)
        remaining_list = remaining_list_parent[remaining_list_key]
        for remaining_list_entry in remaining_list:
            remaining_list_parent[remaining_list_key] = remaining_list_entry
            t = split_testcases(test_result)
            list_of_split_test_results.extend(t)

    return list_of_split_test_results

if __name__ == '__main__':
    logging.basicConfig(filename='/var/log/mongo2elk.log', format='%(asctime)s %(levelname)s: %(message)s', level=logging.DEBUG)
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
        print json.dumps(parsed_test_result)
