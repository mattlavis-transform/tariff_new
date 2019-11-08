import classes.globals as g
import os
import sys

app = g.app
app.get_templates()
app.get_envelope()

app.get_mfns()
app.get_measures()
app.get_measure_conditions()
app.get_measure_condition_components()
app.get_measure_components()
app.get_measure_geographical_exclusions()
app.get_measure_footnotes()
app.get_measure_partial_temporary_stops()

app.terminate_measures()
app.restart_measures()
app.append_seasonal_goods()

app.write_content()
app.validate(app.output_filename)
#app.generate_xml_report()
#app.check_business_rules()
app.copy_to_custom_import_folder()
app.set_config()