# Import external libraries
import xml.etree.ElementTree as ET
import os
import sys
import enum
import datetime

import footnote_type							# 10000
import footnote_type_description				# 10005

import certificate_type							# 11000
import certificate_type_description				# 11005

import additional_code_type						# 12000
import additional_code_type_description			# 12005

import regulation_group							# 15000
import regulation_group_description				# 15005

import footnote									# 20000
import footnote_description_period				# 20005
import footnote_description						# 20010

import certificate								# 20500
import certificate_description_period			# 20505
import certificate_description					# 20510

import measure_type								# 23500
import measure_type_description					# 23505

import additional_code_type_measure_type		# 24000
import additional_code							# 24500
import additional_code_description_period		# 24505
import additional_code_description				# 24510

import geographical_area						# 25000
import geographical_area_description_period		# 25005
import geographical_area_description			# 25010
import geographical_area_membership				# 25015

import base_regulation							# 28500

import quota_order_number						# 36000
import quota_order_number_origin				# 36010
import quota_order_number_origin_exclusions		# 36015

import quota_definition							# 37000
import quota_association						# 37005
import quota_suspension_period					# 37015

import quota_unsuspension_event					# 37525
import quota_closed_and_transferred_event		# 37530

import goods_nomenclature						# 40000
import goods_nomenclature_indents				# 40005
import goods_nomenclature_description_period	# 40010
import goods_nomenclature_description			# 40015
import footnote_association_goods_nomenclature	# 40020
import goods_nomenclature_origin				# 40035
import goods_nomenclature_successor				# 40040

import measure									# 43000
import measure_component						# 43005
import measure_condition						# 43010
import measure_condition_component				# 43011
import measure_excluded_geographical_area		# 43015
import footnote_association_measure				# 43020
import measure_partial_temporary_stop			# 43025

global iCount
global dictRecordCount

namespaces = {'oub': 'urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0', 'env': 'urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0', } # add more as needed
# sDivider = "|"
sDivider = ""

class datatype(enum.Enum):
    string = 1
    date = 2
    currency = 3

def doConvert(sXMLFile):
	global iCount, 	dictRecordCount, dictSequence
	print ("Creating EDIFACT file for " + sXMLFile)

	pathname = os.path.dirname(os.path.abspath(__file__)) + "\\xml\\"
	sXMLFile = pathname + sXMLFile
	sEdifact = sXMLFile.replace(".xml", ".txt")
	sEdifact = sEdifact.replace("\\xml", "\\edifact")

	# Load file
	tree = ET.parse(sXMLFile)
	# tree.encode("utf-8")
	root = tree.getroot()

	# open a file for writing
	oEdifact = open(sEdifact, 'w')

	# Get the date and time for the header and footer
	now = datetime.datetime.now()
	sDate = str(now.day).zfill(2)
	sDate += str(now.month).zfill(2)
	sDate += str(now.year).zfill(4)
	sDate += ":"
	sDate += str(now.hour).zfill(2)
	sDate += str(now.minute).zfill(2)

	# Get the envelope details
	sIdentifier = root.attrib["id"]

	# Write the header
	oEdifact.write("UNB+XXIB:1+CEC/XXI/B5+               GB+" + sDate + "+TARIC'\n")
	oEdifact.write("UNH+TGBGB" + sIdentifier + "+" + sIdentifier[1:] + "'\n")
	iCount = 3
	dictRecordCount = {}
	dictSequence = {}

	# find the relevant objects
	"""
	oEdifact.write(footnote_type.convert(root))                         	# 10000
	oEdifact.write(footnote_type_description.convert(root))             	# 10005

	oEdifact.write(certificate_type.convert(root))                      	# 11000
	oEdifact.write(certificate_type_description.convert(root))          	# 11005

	oEdifact.write(additional_code_type.convert(root))                  	# 12000
	oEdifact.write(additional_code_type_description.convert(root))      	# 12005

	oEdifact.write(regulation_group.convert(root))                      	# 15000
	oEdifact.write(regulation_group_description.convert(root))          	# 15005

	oEdifact.write(footnote.convert(root))                              	# 20000
	oEdifact.write(footnote_description_period.convert(root))           	# 20005
	oEdifact.write(footnote_description.convert(root))                  	# 20010

	oEdifact.write(certificate.convert(root))                           	# 20500
	oEdifact.write(certificate_description_period.convert(root))        	# 20505
	oEdifact.write(certificate_description.convert(root))               	# 20510

	oEdifact.write(measure_type.convert(root))                          	# 23500
	oEdifact.write(measure_type_description.convert(root))              	# 23505

	oEdifact.write(additional_code_type_measure_type.convert(root))     	# 24000

	oEdifact.write(additional_code.convert(root))                       	# 24500
	oEdifact.write(additional_code_description_period.convert(root))    	# 24505
	oEdifact.write(additional_code_description.convert(root))           	# 24510

	oEdifact.write(geographical_area.convert(root))                     	# 25000
	oEdifact.write(geographical_area_description_period.convert(root))  	# 25005
	oEdifact.write(geographical_area_description.convert(root))         	# 25010
	oEdifact.write(geographical_area_membership.convert(root))          	# 25015

	oEdifact.write(base_regulation.convert(root))                       	# 28500

	oEdifact.write(quota_order_number.convert(root))                    	# 36000
	oEdifact.write(quota_order_number_origin.convert(root))             	# 36010
	oEdifact.write(quota_order_number_origin_exclusions.convert(root))  	# 36015

	oEdifact.write(quota_definition.convert(root))                      	# 37000
	oEdifact.write(quota_association.convert(root))                     	# 37005
	oEdifact.write(quota_suspension_period.convert(root))               	# 37015

	oEdifact.write(quota_unsuspension_event.convert(root))              	# 37525
	oEdifact.write(quota_closed_and_transferred_event.convert(root))    	# 37530

	oEdifact.write(goods_nomenclature.convert(root))                    	# 40000
	oEdifact.write(goods_nomenclature_indents.convert(root))            	# 40005
	oEdifact.write(goods_nomenclature_description_period.convert(root)) 	# 40010
	oEdifact.write(goods_nomenclature_description.convert(root))        	# 40015
	oEdifact.write(footnote_association_goods_nomenclature.convert(root))	# 40020
	oEdifact.write(goods_nomenclature_origin.convert(root))					# 40035
	oEdifact.write(goods_nomenclature_successor.convert(root))				# 40040

	"""
	oEdifact.write(measure.convert(root))                               	# 43000
	"""
	oEdifact.write(footnote_association_measure.convert(root))          	# 43020
	oEdifact.write(measure_component.convert(root))                     	# 43005
	oEdifact.write(measure_condition.convert(root))                     	# 43010
	oEdifact.write(measure_condition_component.convert(root))           	# 43011
	oEdifact.write(measure_excluded_geographical_area.convert(root))    	# 43015
	oEdifact.write(measure_partial_temporary_stop.convert(root))          	# 43025
	"""

	# Write the special control record
	sRecordCounts = ""
	for k in dictRecordCount:
		sRecordCounts += k + str(dictRecordCount[k]).zfill(8)

	oEdifact.write("999+" + str(iCount - 1).zfill(8) + "+" + sRecordCounts + "'\n")


	# Write the footer
	oEdifact.write("UNT+" + str(iCount).zfill(8) + "+TGBGB" + sIdentifier + "'\n")
	oEdifact.write("UNZ+01+TARIC'\n")

	# close the file object
	oEdifact.close()


def tofloat(s):
	try:
		return float(s)
	except ValueError:
		return 0

def remove_non_ascii_1(text):
    return ''.join(i for i in text if ord(i)<128)
		
def todate(s):
	t = s[8:] + s[5:7] + s[0:4]	
	return (t)

def edi(oNode, sFind, length, defaultChar, fieldType, direction="right"):
	global namespaces

	try:
		oX = oNode.find(sFind, namespaces)
		value = oX.text
		value = remove_non_ascii_1(value)

		if (fieldType == datatype.currency):
			v2 = int(tofloat(value) * 1000)
			value = str(v2)
		elif (fieldType == datatype.date):
			value = todate(value)
		else:
			value = value.replace("\n", "<P>")

		if (length != "-1"):
			if (direction == "right"):
				value = value.rjust(length, defaultChar)
			else:
				value = value.ljust(length, defaultChar)

	except:
		if (length != "-1"):
			value = defaultChar * length
		else:
			value = ""

	return (value)
	
	
def updateRecordCount(key, value):
	global dictRecordCount
	if key != "":
		if key in dictRecordCount:
			dictRecordCount[key] += value
		else:
			dictRecordCount[key] = value

