from json import loads as jload
import re
def read_json_file(filepath):
    """load json object from file
       :param filepath (str) - json file
       :return json - json list"""
    raw_data = open(filepath).read()
    json_obj = jload(raw_data)

    return json_obj

def connect_to_db(host, user, passwd):
    """connect to mapping rules database
        parameters are self-explanatory
        :returns connector, cursor"""

    import mysql.connector
    connector = mysql.connector.connect(
        host=host,
        user=user,
        passwd=passwd
    )
    cursor = connector.cursor()
    return connector, cursor

def age_in_days(age):
    """self-explanatory. Age evaluated roughly, only for purpose of mapping rules evaluation
    :param age (dict) - age in form {"years" : int, "months" : int, "days" : int}
    :return (int)"""

    if 'years' in age: years = age['years']
    else: years = 0

    if 'months' in age: months = age['months']
    else: months = 0

    if 'days' in age: days = age['days']
    else: days = 0

    return years*365 + months*30 + days

def get_empty_result_dict():
    """:return output dict template"""
    return {"results" : []}

def mapping_rule_tuple_to_dict_conversion(mapping_rule):
    """self-explanatory"""

    return {"snomed_code" : mapping_rule[2],
            "snomed_code_description" : mapping_rule[3],
            "map_group": mapping_rule[4],
            "map_priority": mapping_rule[5],
            "map_advice" : mapping_rule[7],
            "icd_10_code" : mapping_rule[8],
            "icd_10_code_description" : mapping_rule[9],
            "rule_category" : mapping_rule[10]}

def mapping_rules_list_conversion(mapping_rules):
    """self-explanatory"""

    result = []
    for rule in mapping_rules:
        result.append(mapping_rule_tuple_to_dict_conversion(rule))
    return result

    """self-explanatory"""
def query_mapping(db_name, cursor, values):
    """perfom sql query to database
        :param db_name (str) - mysql database name on server
        :param cursor - mysql_db cursor
        :param values (str) - string with values that are ready to insert into sql query text
        (see extract_sql_values_from_input() for details)
        :return rules - list of mapping rules for given snomed codes"""

    cursor.execute("use {};".format(db_name))
    cursor.execute("drop temporary table if exists input;")
    cursor.execute("create temporary table input(num INTEGER, referencedComponentId BIGINT);")
    cursor.execute("insert into input(num, referencedComponentId) values {};".format(values))
    cursor.execute("drop temporary table if exists rules_count;")
    cursor.execute("create temporary table rules_count(referencedComponentId BIGINT, count integer);")
    cursor.execute("""insert into rules_count
                        (select
                        input.referencedComponentId,
                        sum(1 - isnull(mapping_rules.mapGroup))
                        from input
                        left join {}.mapping_rules on input.referencedComponentId = mapping_rules.referencedComponentId
                        group by
                         input.referencedComponentId);""".format(db_name))
    cursor.execute("""SELECT 
                        num,
                        rules_count.count,
                        input.referencedComponentId,
                        referencedComponentName,
                        mapGroup,
                        mapPriority,
                        mapRule,
                        mapAdvice,
                        mapTarget,
                        mapTargetName,
                        mapCategoryName
                        FROM
                            input
                                LEFT JOIN
                            {}.mapping_rules ON input.referencedComponentId = mapping_rules.referencedComponentId
                            inner join rules_count on input.referencedComponentId = rules_count.referencedComponentId
                        ORDER BY num ASC , mapGroup ASC, mapPriority ASC;""".format(db_name))

    queryresult = []
    for x in cursor:
        queryresult.append(x)

    return queryresult

def evaluate_mapping_rule(rule, input):
    """evaluate mapping rule for the list of input parameters
        :param rule (list) - mapping rule queried from db
        :param input (dict) - snomed code, possible with additional parameters, like symptoms and age
        :return (boolean)  - result of rule evaluation"""

    symptom_regex = re.compile('^IFA [0-9]+')

    map_rule = rule[6]

    # divide rule into pieces
    sections = map_rule.split('|')

    if len(sections) == 1 and sections[0].find('TRUE') > -1: # simple one-to-one rule, always true
        return True

    # extract conditions list from the sections
    idx = 0
    conditions = []
    it_is_sympthom = False
    it_is_age = False
    while idx < len(sections):
        section = sections[idx]

        if not len(section): # remove empty section
            sections.pop(idx)
            continue

        if ' AND ' in section: # maybe it's logical 'AND', but it's can only be the part of symptom description also
            subsections = section.split(' AND ')
            if 'IFA' in subsections[-1]: # this 'AND' is definitely a logical operator
                if conditions[-1]['type'] == 'age':
                    conditions[-1]['condition'] += subsections[0].lower().strip() # all before this 'AND' is age condition
                elif conditions[-1]['type'] == 'symptom': # all before this 'AND' is age condition
                    conditions[-1]['symptom_description'] += subsections[0].strip()
                section = subsections[-1]

        if 'IFA 445518008' in section: # it is the start of age-specific condition
            conditions.append({'type' : 'age', 'condition' : ''})
            it_is_age = True
            idx += 2 # there're no need to parse next section
            # it's always 'Age at onset of clinical finding (observable entity)'
        elif 'IFA 248152002' in section:  # it is rule for females
            conditions.append({'type': 'sex', 'condition': 'female'})
            idx += 2  # there're no need to parse next section
            # it's always 'Female (finding)'
        elif 'IFA 248153007' in section:  # it is rule for males
            conditions.append({'type': 'sex', 'condition': 'male'})
            idx += 2  # there're no need to parse next section
            # it's always 'Male (finding)'
        elif symptom_regex.match(section): # it is the start of symptom description
            conditions.append({'type': 'symptom', 'symptom_code': section.replace('IFA ', '').strip(), 'symptom_description' : ''})
            it_is_sympthom = True
            idx += 1
        elif it_is_sympthom: # it is description of symptom code
            conditions[-1]['symptom_description'] = section.strip()
            it_is_sympthom = False
            idx += 1
        elif it_is_age: # it is age condition
            conditions[-1]['condition'] = section.lower().strip()
            it_is_age = False
            idx += 1

    for condition in conditions:
        if not evaluate_condition(condition, input):
            return False # rule evaluated to true only if all of rule's conditions met

    return True # all conditions met

def evaluate_condition(condition, input):
    """evaluate single condition in mapping rule for the list of input parameters
        :param condition (dict) - single condition in mapping rule. see evaluate_mapping_rule() for details
        :param input (dict) - snomed code, possible with additional parameters, like symptoms and age
        :return (boolean)  - result of condition evaluation"""

    if condition['type'] == 'age':
        if not 'age_in_days' in input: # no age data provided, condition evaluated to false
            return False
        else:

            # evaluate age condition against input age
            greater_than_pos = condition['condition'].find('>')
            less_than_pos = condition['condition'].find('<')
            equal_pos = condition['condition'].find('<')
            number = float(re.findall('\d+\.\d+',condition['condition'])[0])
            years_pos = condition['condition'].find('years')
            days_pos = condition['condition'].find('days')

            if years_pos > -1:
                condition_in_days = number * 365
            elif days_pos > -1:
                condition_in_days = number
            else: # something went wrong
                return False

            if greater_than_pos > -1:
                if equal_pos > -1:
                    if input['age_in_days'] >= condition_in_days:
                        return True
                    else:
                        return False
                else:
                    if input['age_in_days'] > condition_in_days:
                        return True
                    else:
                        return False

            elif less_than_pos > -1:
                if equal_pos > -1:
                    if input['age_in_days'] <= condition_in_days:
                        return True
                    else:
                        return False
                else:
                    if input['age_in_days'] < condition_in_days:
                        return True
                    else:
                        return False


    elif condition['type'] == 'symptom':
        if not 'symptoms' in input or not len(input['symptoms']): # no symptoms provided
            return False
        elif condition['symptom_code'] in input['symptoms']: # symptoms match
            return True
        else: # the sympthoms doesn't match
            return False

    elif condition['type'] == 'sex':
        if not 'sex' in input: # no sex info provided
            return False
        elif condition['condition'] == input['sex']:
            return True
        else:
            return False

if __name__ == '__main__':
    print(str(read_json_file('prefs.json')))
    print(str(read_json_file('input.json')))