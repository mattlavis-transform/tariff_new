import classes.globals as g
import os
import sys

app = g.app
app.vat_excise = True
app.vat = False
app.excise = True

app.get_filename()

app.get_regulations()
app.get_measure_types()
app.get_footnote_types()
app.get_footnotes()
app.get_measures()
app.get_measure_components()
app.get_measure_footnotes()

app.write_footnote_types()
app.write_measure_types()
app.write_footnotes()
app.write_regulations()
app.write_measures()

app.validate()