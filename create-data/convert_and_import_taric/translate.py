"""
import sys
from lxml import etree

data = open('C:\\projects\\tariff\create-data\\xslt\\test.xsl')
xslt_content = data.read()

p = etree.XMLParser(recover = True, encoding="UTF-8")
xslt_root = etree.XML(xslt_content, parser=p)
#sys.exit()
dom = etree.parse("C:\\projects\\tariff\\create-data\\convert_and_import_taric\\xml_in\\TGB19026.xml")
result = etree.transform(dom)
f = open("C:\\projects\\tariff\\create-data\\convert_and_import_taric\\translations\\TGB19026.xml")
f.write(str(result))
f.close()
"""

import sys
import lxml.html
from lxml import etree

xslt_filename	= "C:\\projects\\tariff\\create-data\\xslt\\test.xsl"
xml_filename	= "C:\\projects\\tariff\\create-data\\convert_and_import_taric\\xml_in\\TGB19026.xml"
csv_filename	= "C:\\projects\\tariff\\create-data\\convert_and_import_taric\\translations\\TGB19026.csv"

xslt_doc = etree.parse(xslt_filename)
xslt_transformer = etree.XSLT(xslt_doc)

source_doc = etree.parse(xml_filename)
output_doc = xslt_transformer(source_doc)

print(str(output_doc))
output_doc.write(csv_filename) # , pretty_print=True)
#pass
