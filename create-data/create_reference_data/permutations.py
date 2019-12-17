import sys
import common.globals as g
from common.permutation import permutation
from common.application import application


app = g.app

sql = """select mc.measure_sid, mc.duty_expression_id
from measure_components mc, ml.measures_real_end_dates m
where m.validity_end_date is null
and m.measure_sid > 0
-- and m.goods_nomenclature_item_id like '1704903000%'
and measure_type_id in ('142', '103')
and m.measure_sid = mc.measure_sid
order by mc.measure_sid, mc.duty_expression_id limit 100000;"""
cur = g.app.conn.cursor()
cur.execute(sql)
rows = cur.fetchall()
permutations = []
if len(rows) > 0:
    for row in rows:
        measure_sid = str(row[0])
        duty_expression_id = str(row[1])
        p = permutation(measure_sid, duty_expression_id)
        permutations.append(p)

measures = []
print(len(permutations))
# sys.exit()
index = 1
for p in permutations:
    print(index)
    index += 1
    added = False
    for m in measures:
        if m.measure_sid == p.measure_sid:
            m.expression_list.append(p.duty_expression_id)
            added = True
            break

    if added is False:
        m2 = permutation(p.measure_sid, None)
        m2.expression_list.append(p.duty_expression_id)
        measures.append(m2)

expressions = []
for m in measures:
    expressions.append(', '.join(m.expression_list))

unique_expressions = set(expressions)
for e in unique_expressions:
    print(e)
