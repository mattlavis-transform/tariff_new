# Import external libraries
import xml.etree.ElementTree as ET
import csv
import sys
import os

# Import custom libraries
from application import application

app = application()
for filename in os.listdir(app.XML_IN_DIR):
    if filename.endswith(".xml"):
        app.end_date_EU_measures(filename)
        app.generate_metadata()
        #print (filename)
        #sys.exit()
