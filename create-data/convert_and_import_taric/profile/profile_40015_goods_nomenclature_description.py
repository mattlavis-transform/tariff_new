import psycopg2
import common.globals as g
import sys

class profile_40015_goods_nomenclature_description(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                  			= app.getTimestamp()
		goods_nomenclature_description_period_sid	= app.get_number_value(oMessage, ".//oub:goods.nomenclature.description.period.sid", True)
		language_id									= app.get_value(oMessage, ".//oub:language.id", True)
		goods_nomenclature_sid						= app.get_number_value(oMessage, ".//oub:goods.nomenclature.sid", True)
		goods_nomenclature_item_id		            = app.get_value(oMessage, ".//oub:goods.nomenclature.item.id", True)
		productline_suffix			                = app.get_value(oMessage, ".//oub:productline.suffix", True)
		description									= app.get_value(oMessage, ".//oub:description", True)

		if update_type == "1":	# UPDATE
			operation = "U"
			app.doprint ("Updating goods nomenclature description for period " + str(goods_nomenclature_description_period_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting goods nomenclature description for period " + str(goods_nomenclature_description_period_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating goods nomenclature description for period " + str(goods_nomenclature_description_period_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO goods_nomenclature_descriptions_oplog (goods_nomenclature_description_period_sid,
			language_id, goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix,
			description, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
			(goods_nomenclature_description_period_sid,
			language_id, goods_nomenclature_sid, goods_nomenclature_item_id, productline_suffix,
			description, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("goods nomenclature description", operation, goods_nomenclature_description_period_sid, goods_nomenclature_item_id, transaction_id, message_id)
		cur.close()
