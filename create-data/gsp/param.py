import classes.globals as g
import os
import sys

sql = "SELECT * from base_regulations where regulation_group_id = %s and base_regulation_role = %s"

cur = g.app.conn.cursor()
cur.execute(sql, ["DIV", 2])
rows = cur.fetchall()
print (len(rows))