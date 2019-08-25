import helpers
import os

def perform_mapping(input=None,root_path=None):
    """mapping list of snomed codes to icd-10 codes
        :param input(dict)
        :return result (obj) - json with mapping results"""

    output = helpers.get_empty_result_dict()
    prefs_filepath = os.path.join(root_path, 'prefs.json')
    prefs = helpers.read_json_file(prefs_filepath)
    if input == None:
        input = helpers.read_json_file(prefs['default_input_filepath'])
    db_connector, db_cursor = helpers.connect_to_db(host=prefs['mysql_host'],
                                                    user=prefs['mysql_user'],
                                                    passwd=prefs['mysql_pass'])

    # query & evaluating rules for each input code
    # first rule evaluating to true accepted as an answer for that code
    idx = 1
    for el in input['snomed_codes']:
        # query rules
        values = '({0},{1})'.format(str(idx), el['snomed_code'])
        mapping_rules=helpers.query_mapping(db_name=prefs['mysql_db_name'],
                                        cursor=db_cursor,
                                        values=values)

        if len(mapping_rules) == 1 and mapping_rules[0][3] == None: # snomed code not found in mapping table
            result = {"num" : idx-1,
                      "mapping_result": "snomed code not found",
                      "snomed_code" : el['snomed_code'],
                      "snomed_code_description" : None,
                      "icd_10_code" : None,
                      "icd_10_code_description" : None,
                      "mapping_rule_evaluated_to_true": None,
                      "all_mapping_rules_for_snomed_code" : []}
            output['results'].append(result)
            continue

        if 'age' in el: # convert age (dict) into days (int)
            el['age_in_days'] = helpers.age_in_days(el['age'])

        # evaluating each rule
        appropriate_rule = None

        for rule in mapping_rules:
            if helpers.evaluate_mapping_rule(rule=rule,input=el):
                appropriate_rule = rule
                break

        formatted_rule = helpers.mapping_rule_tuple_to_dict_conversion(appropriate_rule)
        if appropriate_rule != None and len(appropriate_rule[8]) != 0: # matching code found
            result = {"num": idx-1,
                      "mapping_result": "mapped correctly",
                      "snomed_code": el['snomed_code'],
                      "snomed_code_description": formatted_rule['snomed_code_description'],
                      "icd_10_code": formatted_rule['icd_10_code'],
                      "icd_10_code_description": formatted_rule['icd_10_code_description'],
                      "mapping_rule_evaluated_to_true": formatted_rule,
                      "all_mapping_rules_for_snomed_code": helpers.mapping_rules_list_conversion(mapping_rules)}
        elif appropriate_rule != None: # empty mapping
            result = {"num": idx-1,
                      "mapping_result": "rule mapped to empty code",
                      "snomed_code": el['snomed_code'],
                      "snomed_code_description": formatted_rule['snomed_code_description'],
                      "icd_10_code": None,
                      "icd_10_code_description": None,
                      "mapping_rule_evaluated_to_true": formatted_rule,
                      "all_mapping_rules_for_snomed_code": helpers.mapping_rules_list_conversion(mapping_rules)}
        else: # need additional info
            result = {"num": idx-1,
                      "mapping_result": "additional info needed",
                      "snomed_code": el['snomed_code'],
                      "snomed_code_description": formatted_rule['snomed_code_description'],
                      "icd_10_code": None,
                      "icd_10_code_description": None,
                      "mapping_rule_evaluated_to_true": None,
                      "all_mapping_rules_for_snomed_code": helpers.mapping_rules_list_conversion(mapping_rules)}
        output['results'].append(result)
        idx += 1

    if 'output' in input and input['output'] == 'short':
        short_output = {"results":[]}
        for entry in output["results"]:
            short_output["results"].append({"num": entry["num"],
                                            "snomed_code": entry["snomed_code"],
                                            "snomed_code_description": entry["snomed_code_description"],
                                            "icd_10_code": entry["icd_10_code"],
                                            "icd_10_code_description": entry["icd_10_code_description"]})
        return short_output
    else:
        return output
    # print(str(mapping_rules))

if __name__ == '__main__':
    perform_mapping()