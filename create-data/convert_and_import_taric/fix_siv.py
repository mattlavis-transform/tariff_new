# Import custom libraries
import sys
from datetime import datetime
import common.globals as g

app = g.app

template = """
    <env:transaction id="[TRANSACTION_ID]">
        <env:app.message id="[MESSAGE_ID]">
            <oub:transmission xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0" xmlns:oub="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0">
                <oub:record>
                    <oub:transaction.id>[TRANSACTION_ID]</oub:transaction.id>
                    <oub:record.code>430</oub:record.code>
                    <oub:subrecord.code>00</oub:subrecord.code>
                    <oub:record.sequence.number>[MESSAGE_ID]</oub:record.sequence.number>
                    <oub:update.type>1</oub:update.type>
                    <oub:measure>
                        <oub:measure.sid>[MEASURE_SID]</oub:measure.sid>
                        <oub:measure.type>490</oub:measure.type>
                        <oub:geographical.area>[GEOGRAPHICAL_AREA_ID]</oub:geographical.area>
                        <oub:goods.nomenclature.item.id>[GOODS_NOMENCLATURE_ITEM_ID]</oub:goods.nomenclature.item.id>
                        <oub:validity.start.date>[VALIDITY_START_DATE]</oub:validity.start.date>
                        <oub:measure.generating.regulation.role>[MEASURE_GENERATING_REGULATION_ROLE]</oub:measure.generating.regulation.role>
                        <oub:measure.generating.regulation.id>[MEASURE_GENERATING_REGULATION_ID]</oub:measure.generating.regulation.id>
                        <oub:validity.end.date>[VALIDITY_START_DATE]</oub:validity.end.date>
                        <oub:justification.regulation.role>[MEASURE_GENERATING_REGULATION_ROLE]</oub:justification.regulation.role>
                        <oub:justification.regulation.id>[MEASURE_GENERATING_REGULATION_ID]</oub:justification.regulation.id>
                        <oub:stopped.flag>0</oub:stopped.flag>
                        <oub:geographical.area.sid>[GEOGRAPHICAL_AREA_SID]</oub:geographical.area.sid>
                        <oub:goods.nomenclature.sid>[GOODS_NOMENCLATURE_SID]</oub:goods.nomenclature.sid>
                    </oub:measure>
                </oub:record>
            </oub:transmission>
        </env:app.message>
    </env:transaction>"""

sql = """
select measure_sid, measure_type_id, geographical_area_id, goods_nomenclature_item_id,
validity_start_date, validity_end_date, measure_generating_regulation_role, measure_generating_regulation_id,
justification_regulation_role, justification_regulation_id, stopped_flag, geographical_area_sid,
goods_nomenclature_sid
from measures
where measure_type_id = '490'
and validity_end_date = '2019-10-31' order by measure_sid
"""
cur = g.app.conn.cursor()
cur.execute(sql)
rows = cur.fetchall()
out = '''<?xml version="1.0" encoding="UTF-8"?>
<env:envelope xmlns="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0" id="190186">
'''
cnt = 1000000
if len(rows) != 0:
    for row in rows:
        measure_sid                         = str(row[0])
        measure_type_id                     = str(row[1])
        geographical_area_id                = str(row[2])
        goods_nomenclature_item_id          = str(row[3])
        validity_start_date                 = datetime.strftime(row[4], "%Y-%m-%d")
        validity_end_date                   = str(row[5])
        measure_generating_regulation_role  = str(row[6])
        measure_generating_regulation_id    = str(row[7])
        justification_regulation_role       = str(row[8])
        justification_regulation_id         = str(row[9])
        stopped_flag                        = str(row[10])
        geographical_area_sid               = str(row[11])
        goods_nomenclature_sid              = str(row[12])

        s = template
        s = s.replace("[MEASURE_SID]", measure_sid)
        s = s.replace("[GEOGRAPHICAL_AREA_ID]", geographical_area_id)
        s = s.replace("[GOODS_NOMENCLATURE_ITEM_ID]", goods_nomenclature_item_id)
        s = s.replace("[VALIDITY_START_DATE]", validity_start_date)
        s = s.replace("[MEASURE_GENERATING_REGULATION_ROLE]", measure_generating_regulation_role)
        s = s.replace("[MEASURE_GENERATING_REGULATION_ID]", measure_generating_regulation_id)
        s = s.replace("[GEOGRAPHICAL_AREA_SID]", geographical_area_sid)
        s = s.replace("[GOODS_NOMENCLATURE_SID]", goods_nomenclature_sid)
        s = s.replace("[TRANSACTION_ID]", str(cnt))
        s = s.replace("[MESSAGE_ID]", str(cnt))
        cnt += 1

        out += s
    
    out += "\n</env:envelope>"
    f = open("guru99.txt","w+")
    f.write(out)
    f.close()
