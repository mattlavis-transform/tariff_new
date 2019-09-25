# This is now deprecated - do not use
import classes.globals as g
import os
import sys

from classes.application import application

app = g.app
app.get_templates()
app.get_envelope()
app.get_quota_order_numbers()
app.get_quota_definitions()
app.get_quota_balances()
app.assign_quota_balances()
sys.exit()
app.get_associations()

"""
app.get_measures() 
app.get_measure_components()
app.get_measure_conditions()
app.get_measure_condition_components()
app.get_measure_geographical_exclusions()
app.get_measure_footnotes()
app.get_measure_partial_temporary_stops()

app.terminate_measures()
"""
app.terminate_definitions() 

app.write_content()
app.validate(app.output_filename)
