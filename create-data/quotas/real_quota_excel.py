import xlrd, sys
from datetime import datetime, date
import classes.globals as g
from classes.fta_quota import fta_quota
from classes.new_quota import new_quota
from classes.quota_order_number import quota_order_number


sheet_name              = "TRQ_database_inward_agreed"
sheet_name_new_quotas   = "New quotas"
sheet_name_special      = "Quota special instructions"

g.app.clear
g.app.get_measurement_units()
g.app.get_quota_associations()
g.app.new_quota_associations = []
g.app.association_count = 0
fname = "measure_components.txt"
f1 = open (fname, "w+")
f1.close()

# The workbook in which the data is stored
g.app.d("Opening quota source document", False)
g.app.d("Using source file " + g.app.input_file)
workbook = xlrd.open_workbook(g.app.input_file)


g.app.fta_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
g.app.fta_content += '<env:envelope xmlns="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0" id="ENV">\n'

# Get special instructions from the Special instructions worksheet
out = ""
"""
if sheet_name_special in workbook.sheet_names():
    ws          = workbook.sheet_by_name(sheet_name_special)
    row_count   = ws.nrows

    out = ""

    for i in range(1, row_count):
        item        = ws.cell(i, 0).value
        instance    = ws.cell(i, 1).value
        action      = ws.cell(i, 2).value
        date_tuple  = xlrd.xldate_as_tuple(ws.cell(i, 3).value, workbook.datemode)
        change      = date(year = date_tuple[0], month = date_tuple[1], day = date_tuple[2])
        change      = datetime.strftime(change, '%Y-%m-%d')
        omit        = ws.cell(i, 4).value

        if omit != "Y":
            if item == "Quota order number":
                if action == "Terminate":
                    q = quota_order_number()
                    q.quota_order_number_id = instance
                    q.get_data_from_id()
                    q.terminate(change)
                    g.app.fta_content += q.xml()
            
                elif action == "Start":
                    q = quota_order_number()
                    q.quota_order_number_id = instance
                    q.get_next_sid()
                    q.start(change)
                    g.app.fta_content += q.xml()
"""
# Get details of new quotas, rates and commodity codes
# Read these from the new quotas sheet
g.app.d("Getting details of new quotas", True)
wb_new_quotas  = workbook.sheet_by_name(sheet_name_new_quotas)
row_count = wb_new_quotas.nrows
g.app.new_quotas    = []
g.app.origins_added = []

for row in range(1, row_count):
    quota_order_number_id               = wb_new_quotas.cell(row, 0).value
    goods_nomenclature_item_id          = wb_new_quotas.cell(row, 1).value
    duty_rate                           = wb_new_quotas.cell(row, 2).value
    omit                                = wb_new_quotas.cell(row, 3).value
    omit_quotar_order_number_creation   = wb_new_quotas.cell(row, 6).value

    if omit != "Y":
        obj = new_quota(quota_order_number_id, goods_nomenclature_item_id, duty_rate)
        obj.omit_quotar_order_number_creation = omit_quotar_order_number_creation
        g.app.new_quotas.append (obj)

# Get a single list just containing the new quota order numbers

g.app.d("Getting full list of quotas", True)

g.app.new_quota_ids = []
for item in g.app.new_quotas:
    g.app.new_quota_ids.append (item.quota_order_number_id)

g.app.new_quota_ids_set = set(g.app.new_quota_ids)
g.app.new_quota_ids = []
for item in g.app.new_quota_ids_set:
    g.app.new_quota_ids.append (item)
g.app.new_quota_ids = sorted(g.app.new_quota_ids)

g.app.quota_list = []

# Create FTA quota objects for each of the new quotas
# This just creates a stub entry, which is later matched and populated properly (hopefully)
for item in g.app.new_quota_ids:
    """for nq in g.app.new_quotas:
        if nq.omit_quotar_order_number_creation == "Y":"""
    g.app.insert_quota_order_number(item)


# Now run through the full list of quotas with all their dates and implicit definitions
worksheet = workbook.sheet_by_name(sheet_name)
sheet_count = workbook.nsheets
col_count = worksheet.ncols 
row_count = worksheet.nrows

g.app.get_quota_descriptions()
g.app.get_geographical_areas()

f = open(g.app.output_file, "w")
f.close()

for row in range(1, row_count):
    country_name            = worksheet.cell(row, 1).value.strip()
    quota_order_number_id   = worksheet.cell(row, 3).value.strip()
    measure_type_id         = worksheet.cell(row, 2).value.strip()
    annual_volume           = worksheet.cell(row, 4).value
    increment               = worksheet.cell(row, 5).value
    is_valid = True

    try:
        date_tuple              = xlrd.xldate_as_tuple(worksheet.cell(row, 6).value, workbook.datemode)
        eu_period_starts        = date(year = date_tuple[0], month = date_tuple[1], day = date_tuple[2])
        date_tuple              = xlrd.xldate_as_tuple(worksheet.cell(row, 7).value, workbook.datemode)
        eu_period_ends          = date(year = date_tuple[0], month = date_tuple[1], day = date_tuple[2])
    except:
        print ("Date error on quota", quota_order_number_id)
        sys.exit()
        is_valid = False
        a = 1


    interim_volume          = worksheet.cell(row, 12).value
    units                   = worksheet.cell(row, 13).value
    omit_string             = worksheet.cell(row, 19).value
    preferential            = worksheet.cell(row, 20).value
    include_interim_period  = worksheet.cell(row, 21).value

    if omit_string != "Y":
        # Check to see if the item has already been added via the new quota process
        found = False
        for item in g.app.quota_list:
            if item.quota_order_number_id.strip() == quota_order_number_id.strip():
                #if item.country_name == country_name:
                print ("Found quota", quota_order_number_id, "in the list of new quotas")
                obj_quota = item
                found = True
                break


        if found == False:
            # Standard create quota functions
            print ("Creating a new quota object", quota_order_number_id, "for", country_name)
            obj_quota = fta_quota(country_name, measure_type_id, quota_order_number_id, annual_volume, increment, \
            eu_period_starts, eu_period_ends, interim_volume, units, preferential, include_interim_period)
            obj_quota.is_valid = is_valid
            g.app.quota_list.append (obj_quota)
        else:
            # Create objects for quotas that are new
            obj_quota.country_name      = country_name
            obj_quota.annual_volume     = annual_volume
            obj_quota.increment         = increment
            obj_quota.eu_period_starts  = eu_period_starts
            obj_quota.eu_period_ends    = eu_period_ends
            obj_quota.interim_volume    = interim_volume
            obj_quota.include_interim_period    = include_interim_period
            obj_quota.units             = units
            obj_quota.preferential      = preferential

            obj_quota.common_elements()

        a = 1


# Now write the XML
g.app.d("\nWriting XML\n", True)
"""
g.app.fta_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
g.app.fta_content += '<env:envelope xmlns="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0" id="ENV">\n'
"""

for quota in g.app.quota_list:
    if quota.is_new == True:
        g.app.fta_content += "<!-- Beginning qon XML for quota " + quota.quota_order_number_id_formatted() + " //-->\n"
        g.app.fta_content += quota.quota_order_number_xml()
        g.app.fta_content += "<!-- Ending qon XML for quota " + quota.quota_order_number_id_formatted() + " //-->\n"
        g.app.fta_content += "<!-- Beginning origin XML for quota " + quota.quota_order_number_id_formatted() + " //-->\n"
        g.app.fta_content += quota.origin_xml
        g.app.fta_content += "<!-- Ending origin XML for quota " + quota.quota_order_number_id_formatted() + " //-->\n"

    if quota.is_valid == True:
        g.app.fta_content += "<!-- Beginning definition XML for quota " + quota.quota_order_number_id_formatted() + " //-->\n"
        for qd in quota.quota_definition_list:
            g.app.fta_content += qd.xml()
        g.app.fta_content += "<!-- Ending definition XML for quota " + quota.quota_order_number_id_formatted() + " //-->\n"

    g.app.fta_content += "<!-- Beginning measure_xml for quota " + quota.quota_order_number_id_formatted() + " //-->\n"
    g.app.fta_content += quota.measure_xml()
    g.app.fta_content += "<!-- Ending measure_xml for quota " + quota.quota_order_number_id_formatted() + " //-->\n"


# Do the associations
g.app.d("Getting quota associations", True)
for quota in g.app.quota_list:
    quota.get_quota_associations()


g.app.d("Writing quota associations", True)
print ("There are", str(len(g.app.new_quota_associations)), "quota associations")
for qa in g.app.new_quota_associations:
    g.app.fta_content += qa.xml()
        


g.app.fta_content += "</env:envelope>"

f = open(g.app.output_file, "a")
f.write (g.app.fta_content)
f.close()
g.app.output_filename = g.app.output_file
g.app.validate()
g.app.set_config()