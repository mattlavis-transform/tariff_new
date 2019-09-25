import sys
import common.globals as g

if len(sys.argv) > 1:
    xml_file = sys.argv[1]
else:
    print ("Please specify a file against which to create metadata - please use the DIT export format e.g. DIT190xxx.xml")
    sys.exit()

g.app.output_filename = xml_file
g.app.metadata_filename = xml_file.replace(".xml", "_metadata.xml")    # "DIT190163_metadata.xml"
g.app.xml_file_out = "/Users/matt.admin/projects/tariffs/create-data/convert_and_import_taric/xml_out/" + xml_file

g.app.generate_metadata()