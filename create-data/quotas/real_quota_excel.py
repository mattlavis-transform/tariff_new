import xlrd, sys
from datetime import datetime, date
import classes.globals as g
from classes.fta_quota import fta_quota
from classes.new_quota import new_quota


sheet_name              = "TRQ_database_inward_agreed"
sheet_name_new_quotas   = "New quotas"

g.app.clear

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

# Get details of new quotas, rates and commodity codes
# Read these from the new quotas sheet
g.app.d("Getting details of new quotas", True)
wb_new_quotas  = workbook.sheet_by_name(sheet_name_new_quotas)
row_count = wb_new_quotas.nrows
g.app.new_quotas    = []
g.app.origins_added = []

for row in range(1, row_count):
    quota_order_number_id       = wb_new_quotas.cell(row, 0).value
    goods_nomenclature_item_id  = wb_new_quotas.cell(row, 1).value
    duty_rate                   = wb_new_quotas.cell(row, 2).value
    omit                        = wb_new_quotas.cell(row, 3).value

    if omit != "Y":
        obj = new_quota(quota_order_number_id, goods_nomenclature_item_id, duty_rate)
        g.app.new_quotas.append (obj)

# Get a single list just containing the new quota order numbers

g.app.d("Getting full list of quotas", True)

g.app.new_quota_ids = []
for item in g.app.new_quotas:
    g.app.new_quota_ids.append (item.quota_order_number_id)
g.app.new_quota_ids = set(g.app.new_quota_ids)
g.app.quota_list = []

# Create new FTA quota objects for each of the new quotas 
for item in g.app.new_quota_ids:
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
    #print ("Works on ", quota_order_number_id)
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
        #print ("Date error on quota", quota_order_number_id)
        is_valid = False


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
                #print ("Found quota", quota_order_number_id, "in the list of new quotas")
                obj_quota = item
                found = True
                break


        if found == False:
            # Standard create quota functions
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
            obj_quota.units             = units
            obj_quota.preferential      = preferential

            obj_quota.common_elements()

        a = 1


# Now write the XML
g.app.d("Writing XML", True)
g.app.fta_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
g.app.fta_content += '<env:envelope xmlns="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0" id="ENV">\n'

for quota in g.app.quota_list:
    if quota.is_new == True:
        g.app.fta_content += quota.quota_order_number_xml()
        g.app.fta_content += quota.origin_xml

    if quota.is_valid == True:
        for qd in quota.quota_definition_list:
            if quota.primary_origin != "LI":
                g.app.fta_content += qd.xml()

    g.app.fta_content += quota.measure_xml()

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