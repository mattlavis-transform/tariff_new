# Import custom libraries
import sys
import common.globals as g

app = g.app
app.get_config()
app.dbase = "tariff_eu"
app.connect()
app.write_classification_trees()
