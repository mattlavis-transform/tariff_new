# Standard imports
import xlrd
import sys
from datetime import datetime, date

# Custom imports
import classes.globals as g
from classes.quota_order_number import quota_order_number
from classes.new_quota import new_quota

# Names of sheets
sheet_name = "TRQ_database_inward_agreed"
sheet_name_new_quotas = "New quotas"
sheet_name_special = "Quota special instructions"

# Get measurement units, get associations from database
g.app.clear
g.app.get_measurement_units()
g.app.get_quota_associations()
g.app.new_quota_associations = []
g.app.association_count = 0

# Open the workbook in which the data is stored - use XLRD
g.app.d("Opening quota source document", False)
g.app.d("Using source file " + g.app.input_file)
workbook = xlrd.open_workbook(g.app.input_file)

# Start the XML extract string
g.app.fta_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
g.app.fta_content += '<env:envelope xmlns="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0" id="ENV">\n'

# Get details of new quotas, rates and commodity codes - Read these from the "New quotas" sheet
g.app.d("Getting details of new quotas", True)
wb_new_quotas = workbook.sheet_by_name(sheet_name_new_quotas)
new_quota_count = wb_new_quotas.nrows
g.app.new_quotas = []
g.app.origins_added = []

for row in range(1, new_quota_count):
    quota_order_number_id = wb_new_quotas.cell(row, 0).value
    goods_nomenclature_item_id = wb_new_quotas.cell(row, 1).value
    duty_rate = wb_new_quotas.cell(row, 2).value
    omit = wb_new_quotas.cell(row, 3).value
    measure_type_id = int(wb_new_quotas.cell(row, 4).value)
    measure_type_id = "{:.0f}".format(measure_type_id)
    omit_quotar_order_number_creation = wb_new_quotas.cell(row, 6).value

    if omit != "Y":
        # Create temporary objects of type "new_quota" to house these quotas until incorporated into main "quota_order_number" corpus
        obj_quota = new_quota(quota_order_number_id, goods_nomenclature_item_id, duty_rate, measure_type_id)
        obj_quota.omit_quotar_order_number_creation = omit_quotar_order_number_creation
        g.app.new_quotas.append(obj_quota)


# Get a single list just containing the new quota order number IDs only
g.app.new_quota_ids = []
for item in g.app.new_quotas:
    g.app.new_quota_ids.append(item.quota_order_number_id)

g.app.new_quota_ids_set = set(g.app.new_quota_ids)
g.app.new_quota_ids = []
for item in g.app.new_quota_ids_set:
    g.app.new_quota_ids.append(item)
g.app.new_quota_ids = sorted(g.app.new_quota_ids)

g.app.quota_list = []

# Create FTA quota objects for each of the new quotas - This just creates a stub entry, which is later matched and populated properly
for item in g.app.new_quota_ids:
    # Get the measure type ID from the new quota order number list
    measure_type_id = ""
    for item2 in g.app.new_quotas:
        if item2.quota_order_number_id == item:
            measure_type_id = item2.measure_type_id
            break

    g.app.insert_quota_order_number(item, measure_type_id)

# Now run through the full list of quotas with all their dates and implicit definitions (from sheet "TRQ_database_inward_agreed")
worksheet = workbook.sheet_by_name(sheet_name)
row_count = worksheet.nrows

# Get all the quota descriptions from the database - these are all stored in the EU database
g.app.get_quota_descriptions()


# Get all the geographical areas, used to find the SIDs later on (e.g. for origins and exclusions)
g.app.get_geographical_areas()


# Cycle through the main sheet "TRQ_database_inward_agreed" to find the quotas to insert
for row in range(1, row_count):
    country_name = worksheet.cell(row, 1).value.strip()
    quota_order_number_id = worksheet.cell(row, 3).value.strip()
    measure_type_id = worksheet.cell(row, 2).value.strip()
    annual_volume = worksheet.cell(row, 4).value
    increment = worksheet.cell(row, 5).value

    is_valid = True

    try:
        date_tuple = xlrd.xldate_as_tuple(worksheet.cell(row, 6).value, workbook.datemode)
        eu_period_starts = date(year=date_tuple[0], month=date_tuple[1], day=date_tuple[2])
        date_tuple = xlrd.xldate_as_tuple(worksheet.cell(row, 7).value, workbook.datemode)
        eu_period_ends = date(year=date_tuple[0], month=date_tuple[1], day=date_tuple[2])
    except:
        print("Date error on quota", quota_order_number_id)
        is_valid = False
        sys.exit()

    interim_volume = worksheet.cell(row, 12).value
    units = worksheet.cell(row, 13).value
    omit_string = worksheet.cell(row, 19).value
    preferential = worksheet.cell(row, 20).value
    include_interim_period = worksheet.cell(row, 21).value
    exclusions = worksheet.cell(row, 23).value.strip()

    if quota_order_number_id == "098603":
        a = 1

    if omit_string != "Y":
        # Check to see if the item has already been added via the new quota process
        found_in_new_quota_list = False
        for item in g.app.quota_list:
            if item.quota_order_number_id.strip() == quota_order_number_id.strip():
                # print("Found quota", quota_order_number_id, "in the list of new quotas")
                obj_quota = item
                found_in_new_quota_list = True
                break

        if found_in_new_quota_list is False:
            # Standard create quota functions - this is not marked as a new quota (i.e. not in the list on the "New quotas" sheet)
            print("Creating a new quota object", quota_order_number_id, "for", country_name)
            obj_quota = quota_order_number(country_name, measure_type_id, quota_order_number_id, annual_volume, increment,
            eu_period_starts, eu_period_ends, interim_volume, units, preferential, include_interim_period, exclusions)
            obj_quota.is_valid = is_valid
            g.app.quota_list.append(obj_quota)
        else:
            # This is a 'new' quota - perform new quota tasks
            print(exclusions)
            # Create objects for quotas that are new
            obj_quota.country_name = country_name
            obj_quota.annual_volume = annual_volume
            obj_quota.increment = increment
            obj_quota.eu_period_starts = eu_period_starts
            obj_quota.eu_period_ends = eu_period_ends
            obj_quota.interim_volume = interim_volume
            obj_quota.include_interim_period = include_interim_period
            obj_quota.units = units
            obj_quota.preferential = preferential
            obj_quota.exclusions = exclusions

            obj_quota.common_elements()

# Now write the XML
g.app.d("\nWriting XML\n", True)


closed_list = []
for quota in g.app.quota_list:
    if g.app.is_safeguard and quota.quota_order_number_id not in closed_list:
        closed_list.append(quota.quota_order_number_id)
        quota.create_closure_xml()

    if quota.is_new is True:
        g.app.fta_content += "<!-- Beginning quota order number XML for quota " + quota.quota_order_number_id_formatted() + " //-->\n"
        g.app.fta_content += quota.quota_order_number_xml()
        g.app.fta_content += "<!-- Ending quota order number XML for quota " + quota.quota_order_number_id_formatted() + " //-->\n"
        g.app.fta_content += "<!-- Beginning origin XML for quota " + quota.quota_order_number_id_formatted() + " //-->\n"
        g.app.fta_content += quota.origin_xml
        g.app.fta_content += "<!-- Ending origin XML for quota " + quota.quota_order_number_id_formatted() + " //-->\n"

    if quota.is_valid is True:
        if quota.method == "FCFS":
            g.app.fta_content += "<!-- Beginning definition XML for quota " + quota.quota_order_number_id_formatted() + " //-->\n"

        for qd in quota.quota_definition_list:
            g.app.fta_content += qd.xml()

        if quota.method == "FCFS":
            g.app.fta_content += "<!-- Ending definition XML for quota " + quota.quota_order_number_id_formatted() + " //-->\n"

    g.app.fta_content += "<!-- Beginning measure xml for quota " + quota.quota_order_number_id_formatted() + " //-->\n"
    g.app.fta_content += quota.measure_xml()
    g.app.fta_content += "<!-- Ending measure xml for quota " + quota.quota_order_number_id_formatted() + " //-->\n"


# Do the associations
g.app.d("Getting quota associations", True)
for quota in g.app.quota_list:
    quota.get_quota_associations()


g.app.d("Writing quota associations", True)
print("There are", str(len(g.app.new_quota_associations)), "quota associations")
for qa in g.app.new_quota_associations:
    g.app.fta_content += qa.xml()

g.app.fta_content += "</env:envelope>"

f = open(g.app.output_file, "w+")
f.write(g.app.fta_content)
f.close()
g.app.output_filename = g.app.output_file
g.app.validate()
g.app.set_config()
