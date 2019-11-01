import psycopg2
from datetime import datetime
import common.globals as g

class profile_37005_quota_association(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date              = app.getTimestamp()
		main_quota_definition_sid	= app.get_number_value(oMessage, ".//oub:main.quota.definition.sid", True)
		sub_quota_definition_sid	= app.get_number_value(oMessage, ".//oub:sub.quota.definition.sid", True)
		relation_type				= app.get_value(oMessage, ".//oub:relation.type", True)
		coefficient				    = app.get_value(oMessage, ".//oub:coefficient", True)

		sql = "select quota_definition_sid from quota_definitions where quota_definition_sid = %s"
		params = [
			str(main_quota_definition_sid)
		]
		cur = g.app.conn.cursor()
		cur.execute(sql, params)
		rows = cur.fetchall()
		if len(rows) == 0:
			g.app.add_load_error("DBFK - On loading quota association, cannt find quota definition ID " + str(main_quota_definition_sid))

		sql = "select quota_definition_sid from quota_definitions where quota_definition_sid = %s"
		params = [
			str(sub_quota_definition_sid)
		]
		cur = g.app.conn.cursor()
		cur.execute(sql, params)
		rows = cur.fetchall()
		if len(rows) == 0:
			g.app.add_load_error("DBFK - On loading quota association, cannt find quota definition ID " + str(sub_quota_definition_sid))

		if update_type == "1":		# Update
			operation = "U"
			app.doprint ("Updating quota association " + str(main_quota_definition_sid))
		elif update_type == "2":	# Delete
			operation = "D"
			app.doprint ("Deleting quota association " + str(main_quota_definition_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating quota association " + str(main_quota_definition_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO quota_associations (main_quota_definition_sid,
			sub_quota_definition_sid, relation_type, coefficient, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s)""", 
			(main_quota_definition_sid,
			sub_quota_definition_sid, relation_type, coefficient, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("quota association", operation, main_quota_definition_sid, None, transaction_id, message_id)
		cur.close()
