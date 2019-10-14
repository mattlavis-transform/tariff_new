import psycopg2
from datetime import datetime
import common.globals as g

class profile_40005_goods_nomenclature_indent(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date                  = app.getTimestamp()
		goods_nomenclature_indent_sid	= app.get_number_value(oMessage, ".//oub:goods.nomenclature.indent.sid", True)
		goods_nomenclature_sid			= app.get_number_value(oMessage, ".//oub:goods.nomenclature.sid", True)
		validity_start_date				= app.get_date_value(oMessage, ".//oub:validity.start.date", True)
		number_indents		            = app.get_value(oMessage, ".//oub:number.indents", True)
		goods_nomenclature_item_id		= app.get_value(oMessage, ".//oub:goods.nomenclature.item.id", True)
		productline_suffix			    = app.get_value(oMessage, ".//oub:productline.suffix", True)

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating goods nomenclature indent " + str(goods_nomenclature_indent_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting goods nomenclature indent " + str(goods_nomenclature_indent_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating goods nomenclature indent " + str(goods_nomenclature_indent_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO goods_nomenclature_indents_oplog (goods_nomenclature_indent_sid, 
			goods_nomenclature_sid, validity_start_date, number_indents, 
			goods_nomenclature_item_id, productline_suffix,
			operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", 
			(goods_nomenclature_indent_sid, 
			goods_nomenclature_sid, validity_start_date, number_indents, 
			goods_nomenclature_item_id, productline_suffix,
			operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("goods nomenclature indent", operation, goods_nomenclature_indent_sid, goods_nomenclature_item_id, transaction_id, message_id)
		cur.close()
