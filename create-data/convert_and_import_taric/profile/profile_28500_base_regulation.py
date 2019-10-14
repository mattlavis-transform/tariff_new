import psycopg2
import common.globals as g

class profile_28500_base_regulation(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date						= app.getTimestamp()
		base_regulation_role				= app.get_value(oMessage, ".//oub:base.regulation.role", True)
		base_regulation_id					= app.get_value(oMessage, ".//oub:base.regulation.id", True)
		code								= base_regulation_role + base_regulation_id
		published_date		        		= app.get_date_value(oMessage, ".//oub:published.date", True)
		officialjournal_number			    = app.get_value(oMessage, ".//oub:officialjournal.number", True)
		officialjournal_page	            = app.get_value(oMessage, ".//oub:officialjournal.page", True)
		validity_start_date					= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date					= app.get_date_value(oMessage, ".//oub:validity.end.date", True)
		effective_end_date					= app.get_date_value(oMessage, ".//oub:effective.end.date", True)
		community_code						= app.get_value(oMessage, ".//oub:community.code", True)
		regulation_group_id					= app.get_value(oMessage, ".//oub:regulation.group.id", True)
		antidumping_regulation_role			= app.get_value(oMessage, ".//oub:antidumping.regulation.role", True)
		related_antidumping_regulation_id	= app.get_value(oMessage, ".//oub:related.antidumping.regulation.id", True)
		complete_abrogation_regulation_role	= app.get_value(oMessage, ".//oub:complete.abrogation.regulation.role", True)
		complete_abrogation_regulation_id	= app.get_value(oMessage, ".//oub:complete.abrogation.regulation.id", True)
		explicit_abrogation_regulation_role	= app.get_value(oMessage, ".//oub:explicit.abrogation.regulation.role", True)
		explicit_abrogation_regulation_id	= app.get_value(oMessage, ".//oub:explicit.abrogation.regulation.id", True)
		replacement_indicator               = app.get_value(oMessage, ".//oub:replacement.indicator", True)
		stopped_flag                  		= app.get_value(oMessage, ".//oub:stopped.flag", True)
		information_text                  	= app.get_value(oMessage, ".//oub:information.text", True)
		approved_flag                  		= app.get_value(oMessage, ".//oub:approved.flag", True)

		if g.app.perform_taric_validation == True:
			base_regulations = g.app.get_base_regulations()
			regulation_groups = g.app.get_regulation_groups()
				
			if validity_end_date != None:
				if validity_end_date < validity_start_date:
					g.app.add_load_error("ROIMB3 - The start date of the base regulation must be less than or equal to the end date, when loading base regulation " + str(base_regulation_id))

			if regulation_group_id not in regulation_groups:
				g.app.add_load_error("ROIMB4 - The referenced regulation group must exist, when loading base regulation " + str(base_regulation_id))

		
		national = None

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating base regulation " + str(base_regulation_id))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting base regulation " + str(base_regulation_id))

		else:					# INSERT
			if g.app.perform_taric_validation == True:
				if code in base_regulations:
					g.app.add_load_error("ROIMB1 - The (regulation id + role id) must be unique, when loading base regulation " + base_regulation_id)

			operation = "C"
			app.doprint ("Creating base regulation " + str(base_regulation_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO base_regulations_oplog (base_regulation_role,
			base_regulation_id, published_date, officialjournal_number, officialjournal_page, 
			validity_start_date, validity_end_date, effective_end_date, 
			community_code, regulation_group_id, antidumping_regulation_role, 
			related_antidumping_regulation_id, complete_abrogation_regulation_role, complete_abrogation_regulation_id,
			explicit_abrogation_regulation_role, explicit_abrogation_regulation_id, 
			replacement_indicator, stopped_flag, information_text, approved_flag,
			operation, operation_date, national)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
			(base_regulation_role,
			base_regulation_id, published_date, officialjournal_number, officialjournal_page, 
			validity_start_date, validity_end_date, effective_end_date, 
			community_code, regulation_group_id, antidumping_regulation_role, 
			related_antidumping_regulation_id, complete_abrogation_regulation_role, complete_abrogation_regulation_id,
			explicit_abrogation_regulation_role, explicit_abrogation_regulation_id, 
			replacement_indicator, stopped_flag, information_text, approved_flag,
			operation, operation_date, national))
			app.conn.commit()
		except:
			g.app.log_error("base regulation", operation, None, base_regulation_id, transaction_id, message_id)
		cur.close()

		if g.app.perform_taric_validation == True:
			g.app.all_regulations = g.app.get_all_regulations()
