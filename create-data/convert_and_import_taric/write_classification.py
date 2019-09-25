# Import custom libraries
import sys
import common.globals as g

app = g.app
app.dbase = "tariff_eu"
app.write_classification_trees()
