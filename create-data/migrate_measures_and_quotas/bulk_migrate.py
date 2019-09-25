import classes.globals as g
import os
import sys
import csv
import classes.functions as fn


app = g.app

migrations_file		= "migrations_" + app.bulk_migration_profile + ".csv"
migrations_file2	= os.path.join(app.MIGRATION_PROFILE_DIR, migrations_file)

print ("Commencing bulk migration using profile '", app.bulk_migration_profile, "'")
print ("This will read data from the CSV file", migrations_file, "\n")

ret = fn.yes_or_no("Do you want to continue?")
if not (ret):
	sys.exit()

app.bulk_log_delete()

app.override_prompt = True
with open(migrations_file2) as csv_file:
	csv_reader = csv.reader(csv_file, delimiter = ",")
	for row in csv_reader:
		if len(row) > 0:
			regulation_from = row[0]
			if regulation_from != "EU Regulation":
				try:
					regulation_to = row[1]
				except:
					regulation_to = ""

				#if len(regulation_from) == 7:
				if 1 > 0:
					if regulation_to != "":
						os.system("python3 migrate_measures.py rb r " + regulation_from + " " + regulation_to)
					else:
						os.system("python3 migrate_measures.py rb t " + regulation_from)

app.bulk_recompile()

sys.exit()
