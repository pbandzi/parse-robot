import json
import sys
#import time

data = json.load(sys.stdin)

for test_result in data["test_results"]:

  if test_result["case_name"] == "Rally":
    tmp_result = test_result
    for detail in  test_result["details"]:
      tmp_result["details"] = {"sla": detail["sla"][0], "name": detail["key"]["name"]}
      print(json.dumps(tmp_result))
    continue

  if test_result["case_name"] == "vIMS":
    tmp_result = test_result
    for ims_case in test_result["details"]["sig_test"]["result"]:
      tmp_result["details"] = ims_case
      print(json.dumps(tmp_result))
    continue

  if test_result["case_name"] == "ODL":
    tmp_result = test_result
    for detail in test_result["details"]:
      tmp_result["details"] = detail
      print(json.dumps(tmp_result))
    continue

  if test_result["project_name"] == "yardstick":
    tmp_result = test_result
    for detail in test_result["details"]:
      tmp_result["details"] = detail
      print(json.dumps(tmp_result))
    continue

  print(json.dumps(test_result))
