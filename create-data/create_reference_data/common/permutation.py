import psycopg2
import sys
import re
import common.functions as fn
import common.globals as g


class permutation(object):
    def __init__(self, measure_sid, duty_expression_id):
        self.measure_sid = measure_sid
        self.duty_expression_id = duty_expression_id
        self.expression_list = []