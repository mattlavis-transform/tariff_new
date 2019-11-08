import classes.globals as g
import os
import sys

sql = "SELECT * from base_regulations where regulation_group_id IN %s"

cur = g.app.conn.cursor()
cur.execute(sql, [("DIV", "xDUM")])
rows = cur.fetchall()
print (len(rows))