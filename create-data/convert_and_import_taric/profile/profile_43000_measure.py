import psycopg2, sys
import common.globals as g
from common.classification import classification
from datetime import datetime

class profile_43000_measure(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                      = app.getTimestamp()
		measure_sid							= app.getNumberValue(oMessage, ".//oub:measure.sid", True)
		measure_type						= app.getValue(oMessage, ".//oub:measure.type", True)
		geographical_area					= app.getValue(oMessage, ".//oub:geographical.area", True)
		goods_nomenclature_item_id			= app.getValue(oMessage, ".//oub:goods.nomenclature.item.id", True)
		additional_code_type				= app.getValue(oMessage, ".//oub:additional.code.type", True)
		additional_code						= app.getValue(oMessage, ".//oub:additional.code", True)
		ordernumber							= app.getValue(oMessage, ".//oub:ordernumber", True)
		reduction_indicator					= app.getNumberValue(oMessage, ".//oub:reduction.indicator", True)
		validity_start_date					= app.getDateValue(oMessage, ".//oub:validity.start.date", True)
		validity_start_date_string			= app.getValue(oMessage, ".//oub:validity.start.date", True)
		measure_generating_regulation_role	= app.getValue(oMessage, ".//oub:measure.generating.regulation.role", True)
		measure_generating_regulation_id	= app.getValue(oMessage, ".//oub:measure.generating.regulation.id", True)
		regulation_code						= measure_generating_regulation_id
		validity_end_date					= app.getDateValue(oMessage, ".//oub:validity.end.date", True)
		validity_end_date_string			= app.getValue(oMessage, ".//oub:validity.end.date", True)
		justification_regulation_role		= app.getValue(oMessage, ".//oub:justification.regulation.role", True)
		justification_regulation_id			= app.getValue(oMessage, ".//oub:justification.regulation.id", True)
		stopped_flag						= app.getValue(oMessage, ".//oub:stopped.flag", True)
		geographical_area_sid				= app.getNumberValue(oMessage, ".//oub:geographical.area.sid", True)
		goods_nomenclature_sid				= app.getNumberValue(oMessage, ".//oub:goods.nomenclature.sid", True)
		additional_code_sid					= app.getNumberValue(oMessage, ".//oub:additional.code.sid", True)
		export_refund_nomenclature_sid		= app.getNumberValue(oMessage, ".//oub:export.refund.nomenclature.sid", True)

		# Add to a global list of measures, so that this can be validated at the end that there are
		# components associated with it
		if measure_type in g.app.measure_types_that_require_components_list:
			g.app.duty_measure_list.append(measure_sid)

		if g.app.perform_taric_validation == True:
			# Not a Taric error, but just simple referential integrity
			# Check if a delete is being made, and if so, check that the measure exists before actually deleting
			if int(update_type) == 2:
				sql = "select measure_sid from measures where measure_sid = " + str(measure_sid)
				cur = g.app.conn.cursor()
				cur.execute(sql)
				rows = cur.fetchall()
				if len(rows) == 0:
					g.app.add_load_error("DBFK error on measures - Attempt to delete measure SID that does not exist. " + str(measure_sid))

			# Check for ME34 error
			if validity_end_date != None:
				if justification_regulation_id == None or justification_regulation_role == None:
					g.app.add_load_error("ME34 - A justification regulation must be entered if the measure end date is filled in, " \
					"when loading measure with SID " + str(measure_sid))

			# Check for ME86 error
			if int(measure_generating_regulation_role) > 4:
				g.app.add_load_error("ME86 - The role of the entered regulation must be a Base, a Modification, " \
				"a Provisional AntiDumping, a Definitive Anti-Dumping, when loading measure with SID " + str(measure_sid))

			# Check for ME6 / ME7 error
			if (goods_nomenclature_item_id not in g.app.goods_nomenclatures) and (goods_nomenclature_item_id != None):
				g.app.add_load_error("ME6 - The goods code (" + str(goods_nomenclature_item_id) + ") must exist, when loading measure with SID " + str(measure_sid))

			# Check for ME4 error
			geographical_areas = g.app.get_all_geographical_areas()
			if geographical_area not in geographical_areas:
				g.app.add_load_error("ME4 - The geographical area must exist, when loading measure with SID " + str(measure_sid))

			# Check for ME2, ME3 and ME10 error
			measure_types = g.app.get_measure_types()
			if measure_type not in measure_types:
				g.app.add_load_error("ME2 - The measure type must exist, when loading measure with SID " + str(measure_sid))
			else:
				ME3_Error = False
				for item in g.app.all_measure_types:
					measure_type_id2 			= item[0]
					validity_start_date2 		= item[1]
					validity_end_date2 			= item[2]
					order_number_capture_code	= item[3]
					
					if measure_type_id2 == measure_type:
						# first check for ME10 (order_number_capture_code)
						if (order_number_capture_code == 2 and ordernumber != None) or (order_number_capture_code == 1 and ordernumber == None) :
								g.app.add_load_error("ME10 - The order number must be specified if the 'order number flag' " \
								"(specified in the measure type record) has the value 'mandatory'. If the flag is set " \
								"to 'not permitted' then the field cannot be entered, when loading measure with SID " + str(measure_sid))
						if validity_end_date == None:
							if validity_end_date2 == None:
								if validity_start_date < validity_start_date2:
									ME3_Error = True
							else:
								if validity_start_date > validity_end_date2:
									ME3_Error = True
						else:
							if validity_end_date2 == None:
								if validity_start_date < validity_start_date2:
									ME3_Error = True
							else:
								if validity_start_date < validity_start_date2 or validity_end_date > validity_end_date2:
									ME3_Error = True
						break
				if ME3_Error == True:
					g.app.add_load_error("ME3 - The validity period of the measure type must span the validity period of the measure, " \
					"when loading measure with SID " + str(measure_sid))

			# Check for ME25 error
			if validity_end_date != None:
				if validity_end_date < validity_start_date:
					g.app.add_load_error("ME25 - If the measure’s end date is specified (implicitly or explicitly) then " \
					"the start date of the measure must be less than or equal to the end date, when loading measure with SID " + str(measure_sid))

			"""
			if measure_type in ("490", "489", "488"):
				return
			"""
			if int(update_type) in (3,1):
				if ordernumber != None:
					found = False
					for item in g.app.all_quota_order_numbers:
						quota_order_number_id2 = item[0]
						quota_order_number_sid2 = item[1]
						validity_start_date2 = item[2]
						validity_end_date2 = item[3]
						if ordernumber == quota_order_number_id2:
							found = True
							break
					
					if found == False:
						if ordernumber[0:3] != "094":
							g.app.add_load_error("ME116 - Non-existent order number referenced " + ordernumber + " in measure with SID. " + str(measure_sid))
					else:
						ON9_error = False
						if validity_end_date2 == None:
							if validity_start_date < validity_start_date2:
								ON9_error = True
						else:
							if validity_end_date > validity_end_date2 or validity_start_date < validity_start_date2:
								ON9_error = True
						
						if ON9_error == True:
							if validity_start_date > datetime.strptime("2007-12-31", "%Y-%m-%d"):
								g.app.add_load_error("ON9 - When a quota order number is used in a measure then the validity period " \
								"of the quota order number must span the validity period of the measure. This rule is only applicable for " \
								"measure with start date after 31/12/2007, when loading measure with SID " + str(measure_sid) + " for order number " + ordernumber + ".")

			# run ME24 check - check that there is a supporting regulation
			if regulation_code not in g.app.all_regulations:
				g.app.add_load_error("ME24 - Regulation " + measure_generating_regulation_id + " does not exist when dealing with measure with SID " + str(measure_sid))

			# Run ROIMB8 check - that dates of measures fall within dates of regulation
			my_regulation = g.app.get_my_regulation(regulation_code)
			if my_regulation != None:
				regulation_start_date = my_regulation[1]
				regulation_end_date = my_regulation[2]

				if validity_end_date == None:
					# Step 1 - check for when the measure end date is not specified
					if validity_start_date < regulation_start_date: 
						g.app.add_load_error("ROIMB8(a) - Explicit dates of related measures must be within the " \
						"validity period of the base regulation, when dealing with measure SID " + str(measure_sid))
				else:
					# Step 2 - check for when the measure end date is specified
					if regulation_end_date == None:
						if validity_start_date < regulation_start_date: 
							g.app.add_load_error("ROIMB8(b) - Explicit dates of related measures must be within the " \
							"validity period of the base regulation, when dealing with measure SID " + str(measure_sid))
						
					else:
						pass
						"""
						if validity_start_date < regulation_start_date or validity_end_date > regulation_end_date: 
							g.app.add_load_error("ROIMB8(c) - Explicit dates of related measures must be within the " \
							"validity period of the base regulation (" + measure_generating_regulation_id + "), when dealing with measure SID " + str(measure_sid))
						"""

			# Not a business rule, but a major flaw in Taric - this prevents a SID being inserted
			# if it already exists
			if int(update_type) == 3:
				sql = "select measure_sid from measures_oplog where measure_sid = " + str(measure_sid)
				cur = g.app.conn.cursor()
				cur.execute(sql)
				rows = cur.fetchall()
				if len(rows) > 0:
					g.app.add_load_error("ML1 - Measure SID already exists on insert operation. " + str(measure_sid) + " already exists. Please roll back.")
			
			# run ME32 check - get relations of this commodity code up and down the tree
			if int(update_type) == 3:
				my_node = g.app.find_node(goods_nomenclature_item_id)
				relation_string = "'" + goods_nomenclature_item_id + "', "
				for relation in my_node.relations:
					relation_string += "'" + relation + "', "
				relation_string = relation_string.strip(", ")
				
				sql = "select measure_sid from ml.measures_real_end_dates m \n" \
				"where \n" \
				"(\n" \
				"	measure_type_id = '" + measure_type + "' \n"  \
				"	and goods_nomenclature_item_id in (" + relation_string + ") \n"
				

				if geographical_area == None:
					sql += "	and geographical_area_id is Null \n"
				else:
					sql += "	and geographical_area_id = '" + geographical_area + "' \n"

				if ordernumber == None:
					sql += "	and ordernumber is Null \n"
				else:
					sql += "	and ordernumber = '" + ordernumber + "' \n"

				if reduction_indicator == None:
					sql += "	and reduction_indicator is Null \n"
				else:
					sql += "	and reduction_indicator = '" + str(reduction_indicator) + "' \n"

				if additional_code_type == None:
					sql += "	and additional_code_type_id is Null \n"
				else:
					sql += "	and additional_code_type_id = '" + additional_code_type + "' \n"

				if additional_code == None:
					sql += "	and additional_code_id is Null \n"
				else:
					sql += "	and additional_code_id = '" + additional_code + "' \n"

				sql += ")\nand\n(\n"

				if validity_end_date == None:
					# The news measure does not have an end date
					sql += """  (validity_end_date is null or (validity_start_date <= '""" + validity_start_date_string + """'
					and validity_end_date >= '""" + validity_start_date_string + """')))"""
				else:
					# The new measure has an end date
					sql += """
	(
		validity_end_date is not Null and
		(
			('""" + validity_start_date_string + """' <= validity_start_date and '""" + validity_end_date_string + """' >= validity_start_date)
			or
			('""" + validity_start_date_string + """' <= validity_end_date and '""" + validity_end_date_string + """' >= validity_end_date)
		)
	)
	or
	(
		validity_end_date is Null and
		(
			'""" + validity_start_date_string + """' >= validity_start_date or '""" + validity_end_date_string + """' >= validity_start_date
		)
	)
"""

					sql += ")\n"


				cur = g.app.conn.cursor()
				cur.execute(sql)
				rows = cur.fetchall()
				if len(rows) > 0:
					g.app.add_load_error("ME32 conflict caused - please revert database.  Conflict caused on measure " \
					+ str(measure_sid) + " on commodity " +  goods_nomenclature_item_id + " (order number - " + str(ordernumber) + ")")
			
		tariff_measure_number = goods_nomenclature_item_id
		if goods_nomenclature_item_id != None:
			if tariff_measure_number[-2:] == "00":
				tariff_measure_number = tariff_measure_number[:-2]

		if measure_sid < 0:
			national = True
		else:
			national = None
			
		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating measure " + str(measure_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting measure " + str(measure_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating measure " + str(measure_sid))

		cur = app.conn.cursor()
		cur.execute("""INSERT INTO measures_oplog (measure_sid, measure_type_id, geographical_area_id,
		goods_nomenclature_item_id, additional_code_type_id, additional_code_id,
		ordernumber, reduction_indicator, validity_start_date,
		measure_generating_regulation_role, measure_generating_regulation_id, validity_end_date,
		justification_regulation_role, justification_regulation_id, stopped_flag,
		geographical_area_sid, goods_nomenclature_sid, additional_code_sid,
		export_refund_nomenclature_sid, operation, operation_date, national, tariff_measure_number)
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
		(measure_sid, measure_type, geographical_area,
		goods_nomenclature_item_id, additional_code_type, additional_code,
		ordernumber, reduction_indicator, validity_start_date,
		measure_generating_regulation_role, measure_generating_regulation_id, validity_end_date,
		justification_regulation_role, justification_regulation_id, stopped_flag,
		geographical_area_sid, goods_nomenclature_sid, additional_code_sid,
		export_refund_nomenclature_sid, operation, operation_date, national, tariff_measure_number))
		app.conn.commit()

		cur.close()