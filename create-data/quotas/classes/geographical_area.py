import classes.functions as f
import classes.globals as g
import datetime
import sys


class geographical_area(object):
    def __init__(self, country_description, geographical_area_id, measure_generating_regulation_id):
        self.country_description = country_description
        self.geographical_area_id = geographical_area_id
        self.measure_generating_regulation_id = measure_generating_regulation_id
