# Import standard modules
import sys
import glob as g
import functions as f
from commodity import commodity

# Set up
app = g.app
app.get_mfn_rates()

# The function below creates a set of boilerplate texts that are appended to the commodity
# code descriptions that are used on the authorised use document - these are duplicated
# on every commodity code that uses the specific relief in question
app.get_special_paragraphs()

app.get_DavidOwen_list()
app.getSectionsChapters()
app.get_authorised_use_commodities()
app.write_file()
app.shutDown()
print ("\nPROCESS COMPLETE\n")