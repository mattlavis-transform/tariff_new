import classes.globals as g
import os
import sys

g.app.get_geographical_areas()
#g.app.get_exclusions()
g.app.get_measures_from_csv()
g.app.get_nomenclature_dates()
#g.app.associate_exclusions()

#sys.exit()

g.app.write_xml()
g.app.validate()
g.app.set_config()