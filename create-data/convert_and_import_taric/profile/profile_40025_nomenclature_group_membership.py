import common.globals as g

class profile_40025_nomenclature_group_membership(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date               	= app.getTimestamp()
		goods_nomenclature_sid			= app.get_number_value(oMessage, ".//oub:goods.nomenclature.sid", True)
		goods_nomenclature_group_type	= app.get_value(oMessage, ".//oub:goods.nomenclature.group.type", True)
		goods_nomenclature_group_id		= app.get_value(oMessage, ".//oub:goods.nomenclature.group.id", True)
		validity_start_date				= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date				= app.get_date_value(oMessage, ".//oub:validity.end.date", True)
		goods_nomenclature_item_id		= app.get_value(oMessage, ".//oub:goods.nomenclature.item.id", True)
		productline_suffix				= app.get_value(oMessage, ".//oub:productline.suffix", True)

		if update_type == "1":		# UPDATE
			operation = "U"
			app.doprint ("Updating nomenclature group membership on group " + str(goods_nomenclature_group_id))

		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting nomenclature group membership on group " + str(goods_nomenclature_group_id))

		else:					# INSERT
			operation = "C"
			app.doprint ("Creating nomenclature group membership on group " + str(goods_nomenclature_group_id))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO nomenclature_group_memberships_oplog (goods_nomenclature_sid,
			goods_nomenclature_group_type, goods_nomenclature_group_id, validity_start_date, validity_end_date,
			goods_nomenclature_item_id, productline_suffix, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
			(goods_nomenclature_sid,
			goods_nomenclature_group_type, goods_nomenclature_group_id, validity_start_date, validity_end_date,
			goods_nomenclature_item_id, productline_suffix, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("Nomenclature group membership", operation, goods_nomenclature_group_id, transaction_id, message_id)
		cur.close()

