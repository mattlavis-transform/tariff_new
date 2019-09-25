import psycopg2
import common.globals as g

class profile_40040_goods_nomenclature_successor(object):
	def import_xml(self, app, update_type, oMessage, transaction_id, message_id):
		g.app.message_count += 1
		operation_date				        = app.getTimestamp()
		goods_nomenclature_sid		        = app.getNumberValue(oMessage, ".//oub:goods.nomenclature.sid", True)
		absorbed_goods_nomenclature_item_id	= app.getValue(oMessage, ".//oub:absorbed.goods.nomenclature.item.id", True)
		absorbed_productline_suffix			= app.getValue(oMessage, ".//oub:absorbed.productline.suffix", True)
		goods_nomenclature_item_id	        = app.getValue(oMessage, ".//oub:goods.nomenclature.item.id", True)
		productline_suffix			        = app.getValue(oMessage, ".//oub:productline.suffix", True)

		if update_type == "1":	# Update
			operation = "U"
			app.doprint ("Updating goods nomenclature successor " + str(goods_nomenclature_sid))
		elif update_type == "2":	# DELETE
			operation = "D"
			app.doprint ("Deleting goods nomenclature successor " + str(goods_nomenclature_sid))
		else:					# INSERT
			operation = "C"
			app.doprint ("Creating goods nomenclature successor " + str(goods_nomenclature_sid))

		cur = app.conn.cursor()
		try:
			cur.execute("""INSERT INTO goods_nomenclature_successors_oplog (goods_nomenclature_sid,
			absorbed_goods_nomenclature_item_id, absorbed_productline_suffix,
			goods_nomenclature_item_id, productline_suffix,
			operation, operation_date)
			VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
			(goods_nomenclature_sid,
			absorbed_goods_nomenclature_item_id, absorbed_productline_suffix,
			goods_nomenclature_item_id, productline_suffix,
			operation, operation_date))
			app.conn.commit()
		except:
			g.app.log_error("goods nomenclature successor", operation, goods_nomenclature_sid, goods_nomenclature_item_id, transaction_id, message_id)
		cur.close()
