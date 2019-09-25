# Import external libraries
import os

# Import custom libraries
from application import application

app = application()
for filename in os.listdir(app.XML_IN_DIR):
    if filename.endswith(".xml"):
        app.import_xml(filename)
