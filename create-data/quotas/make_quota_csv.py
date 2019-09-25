import classes.globals as g
import os
import sys

#g.app.get_measures_from_csv()
g.app.get_all_origins_from_db()
g.app.get_quota_definitions_from_csv()
g.app.get_quota_order_numbers_from_csv()
g.app.get_commodities_from_db()
g.app.compare_order_number_origins()
#sys.exit()
g.app.write_xml()
g.app.validate()
