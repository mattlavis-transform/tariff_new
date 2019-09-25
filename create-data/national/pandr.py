import classes.globals as g
import os
import sys

app = g.app
app.vat_excise = False
app.get_filename()

app.get_footnote_types()
app.get_certificate_types()
app.get_regulation_groups()
app.get_certificates()
app.get_measure_types()
app.get_regulations()
app.get_geographical_areas()
app.get_geographical_area_memberships()

app.get_footnotes()
app.get_measures()
app.get_measure_components()
app.get_measure_conditions()
app.get_measure_footnotes()
app.get_measure_excluded_geographical_areas()

app.write_footnote_types()                  # Only produce for facsimile UK P&R, and facsimile VAT and excise
app.write_certificate_types()               # Always produce for P&R; not required for VAT - there are no certificate types for VAT & excise
app.write_regulation_groups()
app.write_certificates()
app.write_measure_types()
app.write_regulations()
app.write_geographical_areas()
app.write_geographical_area_memberships()
app.write_footnotes()
app.write_measures()

app.validate()