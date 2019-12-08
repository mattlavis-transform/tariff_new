import sys
from classes.condition import condition


class condition_profile(object):
    def __init__(self):
        self.profile = ""
        self.conditions = []

    def create_condition(self, index, condition_string):
        condition_object = condition_string.split(",")
        item_count = len(condition_object)
        if item_count == 2:
            condition_code = condition_object[0]
            action_code = condition_object[1]
            certificate_type_code = ""
            certificate_code = ""

        elif item_count == 4:
            condition_code = condition_object[0]
            action_code = condition_object[1]
            certificate_type_code = condition_object[2]
            certificate_code = condition_object[3]

        else:
            print("Error", item_count, condition_string)
            sys.exit()

        condition_object = condition(condition_code, action_code, certificate_type_code, certificate_code)
        self.conditions.append(condition_object)
