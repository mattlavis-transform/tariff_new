import psycopg2
from datetime import datetime
import common.globals as g

class profile_40000_goods_nomenclature(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                      = app.getTimestamp()
		goods_nomenclature_sid				= app.get_number_value(oMessage, ".//oub:goods.nomenclature.sid", True)
		goods_nomenclature_item_id			= app.get_value(oMessage, ".//oub:goods.nomenclature.item.id", True)
		producline_suffix			        = app.get_value(oMessage, ".//oub:producline.suffix", True)
		validity_start_date					= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		validity_end_date					= app.get_date_value(oMessage, ".//oub:validity.end.date", True)
		statistical_indicator				= app.get_value(oMessage, ".//oub:statistical.indicator", True)

		# Check the SID exists
		"""
		sql = "select * from goods_nomenclatures where goods_nomenclature_sid = " + str(goods_nomenclature_sid)
		cur = g.app.conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		if len(rows) == 0:
			g.app.log_error("goods nomenclature SID does not exist " + str(goods_nomenclature_sid), operation, goods_nomenclature_sid, goods_nomenclature_item_id, transaction_id, message_id)
		"""

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating goods nomenclature " + str(goods_nomenclature_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting goods nomenclature " + str(goods_nomenclature_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating goods nomenclature " + str(goods_nomenclature_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO goods_nomenclatures_oplog (goods_nomenclature_sid,
			goods_nomenclature_item_id, producline_suffix,
			validity_start_date, validity_end_date, statistical_indicator, operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
			(goods_nomenclature_sid,
			goods_nomenclature_item_id, producline_suffix,
			validity_start_date, validity_end_date, statistical_indicator, operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("goods nomenclature", operation, goods_nomenclature_sid, goods_nomenclature_item_id, transaction_id, message_id)
		cur.close()

		g.app.goods_nomenclatures	= g.app.get_all_goods_nomenclatures()
