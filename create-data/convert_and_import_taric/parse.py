import xml.etree.ElementTree as ET
filename = "/Users/matt.admin/projects/tariffs/create-data/convert_and_import_taric/xml_in/TGB19130.xml"
tree = ET.parse(filename)
root = tree.getroot()

ns = {'env': 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0', 'oub': 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0'}
obj = root.findall('env:transaction', ns)
print (len(obj))
"""
for transaction in root.findall('env:transaction', ns):
    name = transaction.get('id')
    print(name)
"""