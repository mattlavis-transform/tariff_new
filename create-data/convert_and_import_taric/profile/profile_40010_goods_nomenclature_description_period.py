import psycopg2
import common.globals as g

class profile_40010_goods_nomenclature_description_period(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date				                = app.getTimestamp()
		goods_nomenclature_description_period_sid   = app.getNumberValue(oMessage, ".//oub:goods.nomenclature.description.period.sid", True)
		goods_nomenclature_sid                      = app.getNumberValue(oMessage, ".//oub:goods.nomenclature.sid", True)
		validity_start_date			                = app.getDateValue(oMessage, ".//oub:validity.start.date", True)
		goods_nomenclature_item_id		            = app.getValue(oMessage, ".//oub:goods.nomenclature.item.id", True)
		productline_suffix			                = app.getValue(oMessage, ".//oub:productline.suffix", True)

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating goods nomenclature description period " + str(goods_nomenclature_description_period_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting goods nomenclature description period " + str(goods_nomenclature_description_period_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating goods nomenclature description period " + str(goods_nomenclature_description_period_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO goods_nomenclature_description_periods_oplog (goods_nomenclature_description_period_sid,
			goods_nomenclature_sid, validity_start_date, goods_nomenclature_item_id,
			productline_suffix, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
			(goods_nomenclature_description_period_sid,
			goods_nomenclature_sid, validity_start_date, goods_nomenclature_item_id,
			productline_suffix, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("goods nomenclature description period", operation, goods_nomenclature_description_period_sid, goods_nomenclature_item_id, transaction_id, message_id)
		cur.close()
