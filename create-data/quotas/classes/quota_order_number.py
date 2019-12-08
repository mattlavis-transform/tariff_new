import datetime
import sys
from dateutil.relativedelta import relativedelta
from datetime import datetime, date

import classes.functions as f
import classes.globals as g
from classes.quota_definition import quota_definition
from classes.measure import measure
from classes.measure_component import measure_component
from classes.quota_order_number_origin import quota_order_number_origin
from classes.quota_order_number_origin_exclusion import quota_order_number_origin_exclusion
from classes.quota_association import quota_association


class quota_order_number(object):
    def __init__(self, country_name, measure_type_id, quota_order_number_id, annual_volume, increment,
    eu_period_starts, eu_period_ends, interim_volume, unaits, preferential, include_interim_period,
    quota_order_number_sid=-1, exclusions=[]):
        self.country_name = country_name
        self.measure_type_id = measure_type_id
        self.quota_order_number_id = quota_order_number_id
        self.annual_volume = annual_volume
        self.increment = increment
        self.eu_period_starts = eu_period_starts
        self.eu_period_ends = eu_period_ends
        self.interim_volume = interim_volume
        self.units = units
        self.measure_generating_regulation_id = ""
        self.override_origins = False
        self.is_new = False
        self.quota_definition_list = []
        self.origin_list = []
        self.origin_objects = []
        self.origin_xml = ""
        self.preferential = preferential
        self.include_interim_period = include_interim_period
        self.origin_exclusions = []
        self.exclusions = exclusions

        if self.quota_order_number_id[0:3] == "094":
            # print("Found a licensed quota", self.quota_order_number_id)
            self.licensed = True
            self.method = "Licensed"
        else:
            self.licensed = False
            self.method = "FCFS"

        # Only process the quota if the order number starts with "09"
        self.is_valid = True
        if self.quota_order_number_id[0:2] != "09":
            self.is_valid = False
            return

        # Check to see if this is a new quota - this is temporary (and poor) code to deal with safeguards
        self.check_exists()
        if self.quota_order_number_id[0:3] == "098":
            self.is_new = True

        if self.is_new is True:
            if self.suppress_quota_order_number_creation is False:
                self.quota_order_number_sid = g.app.last_quota_order_number_sid
                g.app.last_quota_order_number_sid += 1

            else:
                # In this instance, I need to get the order number of the existing quota order number
                sql = """select quota_order_number_sid from quota_order_numbers qon
                where qon.quota_order_number_id = '""" + self.quota_order_number_id + """'
                and validity_end_date is null
                order by 1 desc;"""
                cur = g.app.conn.cursor()
                cur.execute(sql)
                rows = cur.fetchall()
                count = len(rows)
                if count == 0:
                    print("Problem - system thinks a quota exists, but it does not")
                    sys.exit()
                else:
                    row = rows[0]
                    self.quota_order_number_sid = row[0]
                    print("Retrieved real SID for quota order number", self.quota_order_number_id, "which is", str(self.quota_order_number_sid))

            self.measure_list = []

            for m in g.app.new_quotas:
                if m.quota_order_number_id == self.quota_order_number_id:
                    goods_nomenclature_item_id = self.format_comm_code(m.goods_nomenclature_item_id)
                    duty_amount = -1
                    monetary_unit_code = ""
                    measurement_unit_code = ""
                    measurement_unit_qualifier_code = ""

                    validity_start_date = ""
                    validity_end_date = ""
                    measure_sid = -1

                    m = measure(goods_nomenclature_item_id, self.quota_order_number_id, "", duty_amount,
                    monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code, measure_type_id,
                    validity_start_date, validity_end_date, measure_sid)

                    mc = measure_component(-1, "01", 0, "", "", "")
                    m.measure_component_list.append(mc)

                    self.measure_list.append(m)

        else:
            self.get_sid()
            self.common_elements()
            self.get_measure_components()
            self.get_existing_measures()
            self.assign_measure_components()

    def format_comm_code(self, s):
        s = str(s)
        s = s.replace(" ", "")
        if len(s) < 10:
            s += "0" * (10 - len(s))
        return s

    def common_elements(self):
        self.get_origins()
        self.standardise_numbers()
        self.measurement_unit_code = ""
        if self.units != "":
            self.get_unit()

        self.get_definitions()

    def get_origins(self):
        # Get country origins
        # self.origin_list = []
        self.geographical_area_id = ""

        if self.is_new:
            # for new quotas, get the matching regulation and then create the origin
            if self.country_name != "":
                self.get_matching_regulation()
                self.create_origin_for_new_quota()
        else:
            if self.country_name[0:4] == "Only":
                self.override_origins = True
            if self.country_name != "":
                self.get_matching_regulation()

            if self.override_origins is False:
                self.get_origins_from_db()
                self.get_origin_exclusions_from_db()
            else:
                obj = list()
                obj.append(-1)
                obj.append(-1)
                obj.append("1011")
                obj.append(400)

                self.origin_list.append(obj)

            if self.licensed is False:
                try:
                    self.primary_origin = self.origin_list[0][2]
                except:
                    self.is_new = True
            else:
                self.primary_origin = self.geographical_area_id

    def create_origin_for_new_quota(self):
        validity_start_date = g.app.critical_date_plus_one.strftime("%Y-%m-%d")
        self.primary_origin = self.geographical_area_id

        obj = quota_order_number_origin(self.quota_order_number_sid, self.geographical_area_id, validity_start_date)
        self.origin_objects.append(obj)

        if self.quota_order_number_id not in g.app.origins_added:
            for obj in self.origin_objects:
                self.origin_xml += obj.xml()
                print("Creating a new origin for quota", self.quota_order_number_id, "which should have SID of", str(self.quota_order_number_sid))

            origin_object = list()
            primary_quota_order_number_origin_sid = obj.quota_order_number_origin_sid
            origin_object.append(obj.quota_order_number_origin_sid)
            origin_object.append(obj.quota_order_number_sid)
            origin_object.append(obj.geographical_area_id)
            origin_object.append(obj.geographical_area_sid)

            self.origin_list.append(origin_object)

            # Now add in origin exclusions as appropriate
            ex_string = ""
            exclusions = self.exclusions.replace(" ", "").strip()
            exclusion_list = []
            if exclusions != "":
                # sys.exit()
                exclusion_list = exclusions.split(",")
                for excluded_geographical_area_id in exclusion_list:

                    quota_order_number_origin_sid = primary_quota_order_number_origin_sid
                    excluded_geographical_area_sid = self.get_geographical_area_sid(excluded_geographical_area_id)

                    obj = list()
                    obj.append(quota_order_number_origin_sid)
                    obj.append(excluded_geographical_area_sid)
                    obj.append(excluded_geographical_area_id)
                    self.origin_exclusions.append(obj)

            g.app.origins_added.append(self.quota_order_number_id)

            if len(exclusion_list) > 0:
                self.origin_xml += '\t<env:transaction id="' + str(g.app.transaction_id) + '">\n'
                for excluded_geographical_area_id in exclusion_list:
                    ex = quota_order_number_origin_exclusion(self.quota_order_number_sid, excluded_geographical_area_id)
                    ex.quota_order_number_origin_sid = primary_quota_order_number_origin_sid
                    self.origin_xml += ex.xml()
                self.origin_xml += '\n</env:transaction>\n'
                g.app.transaction_id += 1

    def get_geographical_area_sid(self, geographical_area_id):
        sql = "select geographical_area_sid from geographical_areas where geographical_area_id = '" + geographical_area_id + \
            "' order by validity_start_date desc limit 1"
        cur = g.app.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        count = len(rows)
        if count > 0:
            rw = rows[0]
            return rw[0]
        else:
            print("Error - excluded area cannot be found")
            sys.exit()

    def check_exists(self):
        # print("Checking existence of quota", self.quota_order_number_id)
        # Exceptions because Defra have made a mess of selecting quota order numbers
        self.suppress_quota_order_number_creation = False
        if self.quota_order_number_id in ('092018', '092021', '092023'):  # These are for Bosnia and Macedonia
            self.suppress_quota_order_number_creation = True
            self.is_new = True
            print("Suppressing QON for quota", self.quota_order_number_id)
            return

        # For licensed quotas
        if self.quota_order_number_id[0:3] == "094":
            sql = "select * from ml.measures_real_end_dates where ordernumber = '" + self.quota_order_number_id.strip() + "' order by validity_start_date desc limit 1"
            cur = g.app.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            count = len(rows)
            if count == 0:
                self.is_new = True
                # print("Found a new licensed quota in function check_exists - quota", self.quota_order_number_id)
            else:
                self.is_new = False

        # For FCFS quotas
        else:
            sql = "select * from quota_order_numbers where validity_end_date is null and quota_order_number_id = '" + self.quota_order_number_id.strip() + "'"
            cur = g.app.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            count = len(rows)
            if count == 0:
                self.is_new = True
                # print("Found a new FCFS quota in function check_exists - quota", self.quota_order_number_id)
            else:
                self.is_new = False

    def assign_measure_components(self):
        for m in self.measure_list:
            m.measure_component_list = []
            for mc in self.measure_component_list:
                if mc.measure_sid == m.measure_sid:
                    m.measure_component_list.append(mc)

        for m in self.measure_list:
            component_count = len(m.measure_component_list)
            for i in range(component_count - 1, -1, -1):
                mc = m.measure_component_list[i]
                if mc.duty_expression_id in ('12', '14', '21', '25', '27', '29', '99'):
                    del m.measure_component_list[i]

        # Do a check to see that all measures have got components assigned to then: if they have not, then add a zero duty rate to them (this is cheating)
        for m in self.measure_list:
            component_count = len(m.measure_component_list)
            if component_count == 0:
                # print("Commodity code", m.goods_nomenclature_item_id, "on order number", self.quota_order_number_id, "has no components")
                mc = measure_component(m.measure_sid, "01", 0, "", "", "")
                m.measure_component_list.append(mc)

        commodity_list = []
        for m in self.measure_list:
            commodity_list.append(m.goods_nomenclature_item_id)

    def get_sid(self):
        self.quota_order_number_sid = -1
        sql = """
        select distinct on (qon.quota_order_number_id)
        qon.quota_order_number_sid
        from quota_order_numbers qon
        where qon.quota_order_number_id = '""" + self.quota_order_number_id + """'
        order by qon.quota_order_number_id, qon.validity_start_date desc limit 1"""
        cur = g.app.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            self.quota_order_number_sid = row[0]

    def standardise_numbers(self):
        self.annual_volume = self.standardise(self.annual_volume)
        self.increment = self.standardise(self.increment)
        self.interim_volume = self.standardise(self.interim_volume)

    def standardise(self, var):
        if var in ("", "n/a"):
            var = 0
        var = str(var)
        var = var.replace(",", "")
        try:
            var = float(var)
        except:
            print("Standardisation failure on", self.quota_order_number_id)
            sys.exit()

        try:
            var = int(var)
        except:
            print("Standardisation failure on", self.quota_order_number_id)
            sys.exit()

        return (var)

    def add_origin_exclusions(self, exclusions):
        if exclusions == "":
            return
        exclusions_list = exclusions.split(",")

        for geographical_area_id in exclusions_list:
            sql = "select * from geographical_areas where geographical_area_id = '" + geographical_area_id + "' order by validity_start_date desc limit 1"
            cur = g.app.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            if len(rows) > 0:
                rw = rows[0]
                geographical_area_sid = rw[0]

                obj = list()
                obj.append(-1)
                obj.append(-1)
                obj.append(geographical_area_id)
                obj.append(geographical_area_sid)

                self.origin_exclusions.append(obj)

    def get_matching_regulation(self):
        self.country_name = self.country_name.replace("Only ", "")
        for item in g.app.geographical_areas:
            if item.country_description == self.country_name:
                self.geographical_area_id = item.geographical_area_id
                self.measure_generating_regulation_id = item.measure_generating_regulation_id
                break
        a = 1

    def get_origins_from_db(self):
        if self.override_origins is True:
            # Needs completing
            self.geographical_area_id = self.country_name.replace("Only ", "")
            sql = """
            select geographical_area_sid
            from geographical_areas where geographical_area_id = '""" + self.geographical_area_id + """'
            order by validity_start_date desc
            limit 1
            """
            geographical_area_sid = -1
            cur = g.app.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            if len(rows) > 0:
                rw = rows[0]
                geographical_area_sid = rw[0]

            if geographical_area_sid == -1:
                print(self.quota_order_number_id, self.geographical_area_id)
                sys.exit()

            sql = """
            select quota_order_number_origin_sid, qon.quota_order_number_sid
            from quota_order_number_origins qono, quota_order_numbers qon
            where qon.quota_order_number_sid = qono.quota_order_number_sid
            and qon.quota_order_number_id = '""" + str(self.quota_order_number_id) + """'
            and geographical_area_id = '""" + str(self.geographical_area_id) + """'
            and qono.validity_end_date is null
            """
            cur = g.app.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            if len(rows) > 0:
                rw = rows[0]
                quota_order_number_origin_sid = rw[0]
                quota_order_number_sid = rw[1]

            self.origin_list = []
            self.origin_exclusions = []
            obj = list()
            obj.append(quota_order_number_origin_sid)
            obj.append(quota_order_number_sid)
            obj.append(self.geographical_area_id)
            obj.append(geographical_area_sid)

            self.origin_list.append(obj)

            # Needs completing
        else:
            if self.licensed is True:
                sql = """
                select distinct -1 as quota_order_number_origin_sid, -1 as quota_order_number_sid,
                geographical_area_id, geographical_area_sid
                from ml.measures_real_end_dates m, goods_nomenclatures g
                where ordernumber = '""" + self.quota_order_number_id + """'
                and g.goods_nomenclature_item_id = m.goods_nomenclature_item_id
                and g.producline_suffix = '80'
                and g.validity_end_date is null
                and (m.validity_end_date >= '2018-01-01' or m.validity_end_date is null)
                """

                self.origin_list = []
                cur = g.app.conn.cursor()
                cur.execute(sql)
                rows = cur.fetchall()
                for row in rows:
                    quota_order_number_origin_sid = row[0]
                    quota_order_number_sid = row[1]
                    geographical_area_id = row[2]
                    geographical_area_sid = row[3]
                    obj = list()
                    obj.append(quota_order_number_origin_sid)
                    obj.append(quota_order_number_sid)
                    obj.append(geographical_area_id)
                    obj.append(geographical_area_sid)
                    self.origin_list.append(obj)

                    # Add Liechtenstein in as another origin when it is a Swiss quota
                    if self.country_name == "Switzerland":
                        obj2 = list()
                        obj2.append(quota_order_number_origin_sid)
                        obj2.append(quota_order_number_sid)
                        obj2.append("LI")
                        obj2.append(286)
                        self.origin_list.append(obj2)
                    """
                    elif self.country_name == "Morocco":
                        obj2 = list()
                        obj2.append(quota_order_number_origin_sid)
                        obj2.append(quota_order_number_sid)
                        obj2.append("EH")
                        obj2.append(461)
                        self.origin_list.append(obj2)
                    """

            else:
                sql = """
                select distinct on (geographical_area_id)
                qono.quota_order_number_origin_sid, qono.quota_order_number_sid, qono.geographical_area_id, g.geographical_area_sid
                from quota_order_number_origins qono, geographical_areas g
                where qono.geographical_area_id = g.geographical_area_id
                and qono.validity_end_date is null
                and quota_order_number_sid = (
                select distinct on (qon.quota_order_number_id)
                qon.quota_order_number_sid
                from quota_order_numbers qon
                where qon.quota_order_number_id = '""" + self.quota_order_number_id + """'
                order by qon.quota_order_number_id, qon.validity_start_date desc
                ) order by geographical_area_id, qono.validity_start_date desc;
                """

                self.origin_list = []
                cur = g.app.conn.cursor()
                cur.execute(sql)
                rows = cur.fetchall()
                for row in rows:
                    quota_order_number_origin_sid = row[0]
                    quota_order_number_sid = row[1]
                    geographical_area_id = row[2]
                    geographical_area_sid = row[3]
                    obj = list()
                    obj.append(quota_order_number_origin_sid)
                    obj.append(quota_order_number_sid)
                    obj.append(geographical_area_id)
                    obj.append(geographical_area_sid)
                    self.origin_list.append(obj)

                    # Add Liechtenstein in as another origin when it is a Swiss quota
                    if self.country_name == "Switzerland":
                        obj2 = list()
                        obj2.append(quota_order_number_origin_sid)
                        obj2.append(quota_order_number_sid)
                        obj2.append("LI")
                        obj2.append(286)
                        self.origin_list.append(obj2)
                    """
                    elif self.country_name == "Morocco":
                        obj2 = list()
                        obj2.append(quota_order_number_origin_sid)
                        obj2.append(quota_order_number_sid)
                        obj2.append("EH")
                        obj2.append(461)
                        self.origin_list.append(obj2)
                    """

    def get_origin_exclusions_from_db(self):
        if self.override_origins is True:
            return
        sql = """
        select qonoe.quota_order_number_origin_sid, qonoe.excluded_geographical_area_sid, ga.geographical_area_id
        from quota_order_number_origin_exclusions qonoe, geographical_areas ga
        where qonoe.excluded_geographical_area_sid = ga.geographical_area_sid
        and quota_order_number_origin_sid in (
        select distinct on (geographical_area_id)
        quota_order_number_origin_sid from quota_order_number_origins qono
        where quota_order_number_sid in (
        select distinct on (qon.quota_order_number_id)
        qon.quota_order_number_sid
        from quota_order_numbers qon
        where qon.quota_order_number_id = '""" + self.quota_order_number_id + """'
        order by qon.quota_order_number_id, qon.validity_start_date desc
        ) order by geographical_area_id, validity_start_date desc)
        """
        self.origin_exclusions = []
        cur = g.app.conn.cursor()
        try:
            cur.execute(sql)
        except:
            print("Error in get_origin_exclusions_from_db", sql)
            sys.exit()
        rows = cur.fetchall()
        for row in rows:
            quota_order_number_origin_sid = row[0]
            excluded_geographical_area_sid = row[1]
            excluded_geographical_area_id = row[2]
            obj = list()
            obj.append(quota_order_number_origin_sid)
            obj.append(excluded_geographical_area_sid)
            obj.append(excluded_geographical_area_id)
            self.origin_exclusions.append(obj)

    def get_unit(self):
        # Called from within the function common elements
        # Gets the actual measurement_unit and measurement_qualifier_unit depending on what is listed in the Excel
        # The measurement units have been read in from the CSV
        self.measurement_unit_code = ""

        for obj in g.app.measurement_units:
            if self.units.strip() == obj.identifier:
                self.measurement_unit_code = obj.measurement_unit_code
                self.measurement_unit_qualifier_code = obj.measurement_unit_qualifier_code
                break

        if self.measurement_unit_code == "":
            print(self.quota_order_number_id, "has no unit")
            sys.exit()

    def get_existing_measures(self):
        year_ago = g.app.critical_date + relativedelta(years=-1)
        sql = """
        select distinct measure_sid, m.goods_nomenclature_item_id, measure_type_id,
        m.validity_start_date, m.validity_end_date
        from ml.measures_real_end_dates m, goods_nomenclatures g
        where m.goods_nomenclature_item_id = g.goods_nomenclature_item_id
        and g.producline_suffix = '80'
        and measure_type_id in ('143', '146', '122', '123')
        and (g.validity_end_date is null or g.validity_end_date > '""" + g.app.critical_date.strftime("%Y-%m-%d") + """')
        and ordernumber = '""" + self.quota_order_number_id + """'
        and (m.validity_end_date is null or m.validity_end_date > '""" + year_ago.strftime("%Y-%m-%d") + """')
        order by 1
        """

        self.measure_list = []
        cur = g.app.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            measure_sid = row[0]
            goods_nomenclature_item_id = row[1]
            measure_type_id = row[2]
            validity_start_date = row[3]
            validity_end_date = row[4]

            duty_amount = -1
            monetary_unit_code = ""
            measurement_unit_code = ""
            measurement_unit_qualifier_code = ""

            m = measure(goods_nomenclature_item_id, self.quota_order_number_id, "", duty_amount,
            monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code, measure_type_id,
            validity_start_date, validity_end_date, measure_sid)
            self.measure_list.append(m)

    def get_measure_components(self):
        year_ago = g.app.critical_date + relativedelta(years=-1)
        sql = """
        select distinct on (m.goods_nomenclature_item_id, m.validity_start_date, mc.duty_expression_id)
        m.measure_sid, m.goods_nomenclature_item_id, mc.duty_expression_id, mc.duty_amount,
        mc.monetary_unit_code, mc.measurement_unit_code, mc.measurement_unit_qualifier_code
        from goods_nomenclatures g, ml.measures_real_end_dates m
        left outer join measure_components mc on mc.measure_sid = m.measure_sid
        where m.goods_nomenclature_item_id = g.goods_nomenclature_item_id
        and m.goods_nomenclature_item_id = g.goods_nomenclature_item_id
        and g.producline_suffix = '80'
        and m.measure_type_id in ('122', '123', '143', '146')
        and (g.validity_end_date is null or g.validity_end_date > '""" + g.app.critical_date.strftime("%Y-%m-%d") + """')
        and ordernumber = '""" + self.quota_order_number_id + """'
        and (m.validity_end_date is null or m.validity_end_date > '""" + year_ago.strftime("%Y-%m-%d") + """')
        order by m.goods_nomenclature_item_id, m.validity_start_date desc, mc.duty_expression_id
        """

        self.measure_component_list = []
        cur = g.app.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            measure_sid = row[0]
            goods_nomenclature_item_id = row[1]
            duty_expression_id = row[2]
            duty_amount = row[3]
            monetary_unit_code = row[4]
            measurement_unit_code = row[5]
            measurement_unit_qualifier_code = row[6]

            mc = measure_component(measure_sid, duty_expression_id, duty_amount, monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code)
            self.measure_component_list.append(mc)

        contains_valid_components = False
        for mc in self.measure_component_list:
            if mc.duty_amount is not None:
                self.measure_component_list
                contains_valid_components = True
                break

        component_count = len(self.measure_component_list)
        for i in range(component_count - 1, -1, -1):
            mc = self.measure_component_list[i]
            if mc.duty_amount is None:
                del self.measure_component_list[i]

        if contains_valid_components is False:
            print("No valid components for", self.quota_order_number_id)
            self.get_measure_condition_components()

        if len(self.measure_component_list) == 0:
            if self.quota_order_number_id[0:3] != "094":
                self.get_measure_condition_components()

    def get_measure_condition_components(self):
        year_ago = g.app.critical_date + relativedelta(years=-1)
        sql = """
        select distinct on (m.measure_sid, mcc.duty_amount)
        m.measure_sid, m.goods_nomenclature_item_id, mcc.duty_amount
        from measure_conditions mc, measure_condition_components mcc, measures m
        where mc.measure_condition_sid = mcc.measure_condition_sid
        and m.measure_sid = mc.measure_sid
        and monetary_unit_code is null
        and mc.measure_sid in
        (
            select distinct
            m.measure_sid
            from goods_nomenclatures g, ml.measures_real_end_dates m
            where m.goods_nomenclature_item_id = g.goods_nomenclature_item_id
            and g.producline_suffix = '80'
            and m.measure_type_id in ('122', '123', '143', '146')
            and (g.validity_end_date is null or g.validity_end_date > '""" + g.app.critical_date.strftime("%Y-%m-%d") + """')
            and ordernumber = '""" + self.quota_order_number_id + """'
            -- and (m.validity_end_date is null or m.validity_end_date > '""" + year_ago.strftime("%Y-%m-%d") + """')
        )
        order by m.measure_sid, mcc.duty_amount, m.validity_start_date desc
        """

        self.measure_condition_component_list = []
        cur = g.app.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            measure_sid = row[0]
            goods_nomenclature_item_id = row[1]
            duty_amount = row[2]
            duty_expression_id = "01"
            monetary_unit_code = ""
            measurement_unit_code = ""
            measurement_unit_qualifier_code = ""

            mc = measure_component(measure_sid, duty_expression_id, duty_amount, monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code)
            self.measure_component_list.append(mc)

    def get_definitions(self):
        d = g.app.critical_date
        d2 = g.app.critical_date_plus_one
        crit2 = date(d.year, d.month, d.day)
        critplusone2 = date(d2.year, d2.month, d2.day)

        # Get the interim period
        """
        print("QON", self.quota_order_number_id)
        print("EU period starts", self.eu_period_starts)
        print("EU period ends", self.eu_period_ends)

        print("self.eu_period_ends =", type(self.eu_period_ends))
        print("crit2 =", type(crit2))
        print("self.eu_period_starts =", type(self.eu_period_starts))
        print("critplusone2 =", type(critplusone2))
        """

        if (self.eu_period_ends <= crit2 or (self.eu_period_starts - critplusone2).days == 0):
            # There is no opening period
            pass
        else:
            if self.include_interim_period != "N":
                # print("Creating an interim period on quota", self.quota_order_number_id, self.include_interim_period)
                validity_start_date = date(critplusone2.year, critplusone2.month, critplusone2.day)
                validity_end_date = self.eu_period_ends
                length = (validity_end_date - validity_start_date).days + 1

                try:
                    qd = quota_definition(self.quota_order_number_id, self.quota_order_number_sid, self.measure_type_id, self.method, validity_start_date,
                    validity_end_date, length, self.interim_volume, self.measurement_unit_code, 3, "N", 90, "",
                    self.measurement_unit_qualifier_code, "", "", self.primary_origin)
                except:
                    print(self.quota_order_number_id, "failure on primary origin")
                    sys.exit()
                self.quota_definition_list.append(qd)

        # Now get two years' worth of additional periods
        for i in range(1, 3):
            opening_balance = (i * int(self.increment)) + int(self.annual_volume)
            if self.eu_period_starts <= crit2:
                validity_start_date = self.eu_period_starts + relativedelta(years=i)
                validity_end_date = self.eu_period_ends + relativedelta(years=i)
            else:
                validity_start_date = self.eu_period_starts + relativedelta(years=(i - 1))
                validity_end_date = self.eu_period_ends + relativedelta(years=(i - 1))
            length = (validity_end_date - validity_start_date).days + 1

            if self.is_new:
                self.critical_status = "Y"
            else:
                self.critical_status = "N"

            qd = quota_definition(self.quota_order_number_id, self.quota_order_number_sid, self.measure_type_id, self.method, validity_start_date,
            validity_end_date, length, opening_balance, self.measurement_unit_code, 3, self.critical_status, 90, "",
            self.measurement_unit_qualifier_code, "", "", self.primary_origin)
            self.quota_definition_list.append(qd)

    def quota_order_number_id_formatted(self):
        return self.quota_order_number_id[0:2] + "." + self.quota_order_number_id[-4:]

    def get_quota_associations(self):
        # Now get the associations
        for qd in self.quota_definition_list:
            for qa in g.app.quota_associations_list:
                if qa.main_quota_order_number_id == qd.quota_order_number_id:
                    main_quota_order_number_id = self.quota_order_number_id
                    sub_quota_order_number_id = qa.sub_quota_order_number_id
                    relation_type = qa.relation_type
                    coefficient = qa.coefficient

                    # Get sub SID
                    sub_quota_definition_sid = None
                    for q in g.app.quota_list:
                        for qd2 in q.quota_definition_list:
                            if qd2.quota_order_number_id == qa.sub_quota_order_number_id:
                                if qd.validity_start_date == qd2.validity_start_date:
                                    sub_quota_definition_sid = qd2.quota_definition_sid

                                    print("sub_quota_definition_sid", str(sub_quota_definition_sid))

                                    obj = quota_association(main_quota_order_number_id, sub_quota_order_number_id, relation_type, coefficient)
                                    obj.main_quota_definition_sid = qd.quota_definition_sid
                                    obj.sub_quota_definition_sid = sub_quota_definition_sid
                                    g.app.new_quota_associations.append(obj)
                                    g.app.association_count += 1
                                    break

    def measure_xml(self):
        # Loop through all order numbers origins (geographies), then each definition, then each commodity code
        s = ""
        i = 1

        for o in self.origin_list:
            for d in self.quota_definition_list:
                comm_list = []
                for m in self.measure_list:
                    if m.goods_nomenclature_item_id not in (comm_list):
                        comm_list.append(m.goods_nomenclature_item_id)
                        m.measure_sid = g.app.last_measure_sid
                        g.app.last_measure_sid += 1
                        m.quota_order_number_id = self.quota_order_number_id
                        m.geographical_area_id = o[2].strip()
                        m.geographical_area_sid = o[3]
                        m.measure_generating_regulation_id = self.measure_generating_regulation_id
                        m.justification_regulation_id = self.measure_generating_regulation_id
                        m.measure_type_id = m.measure_type_id
                        m.validity_start_date = datetime.strftime(d.validity_start_date, "%Y-%m-%d")
                        m.validity_end_date = datetime.strftime(d.validity_end_date, "%Y-%m-%d")

                        m.measure_excluded_geographical_area_list = self.get_measure_exclusion_list(o[0], o[2])
                        if m.goods_nomenclature_item_id in ('0707000510', '0707000520', '0707000590', '0707000599'):  # and m.geographical_area_id in ('IL', 'CH', '1001'):
                            pass
                        else:
                            s += m.xml()
                            g.app.transaction_id += 1
                        i += 1

        return (s)

    def get_measure_exclusion_list(self, quota_order_number_origin_sid, geographical_area_id):
        tmp = []
        for obj in self.origin_exclusions:
            if obj[0] == quota_order_number_origin_sid:
                obj_exclusion = quota_order_number_origin_exclusion(self.quota_order_number_sid, obj[2])
                tmp.append(obj_exclusion)
        return (tmp)

    def quota_order_number_xml(self):
        # Create the XML to support the creation of new quota order number objects
        # Should be suppressed if the QON is not new
        self.validity_start_date = datetime.strftime(g.app.critical_date_plus_one, "%Y-%m-%d")
        if self.quota_order_number_id[0:3] == "094":
            return ("")
        if self.is_new is False or self.suppress_quota_order_number_creation is True:
            # In this instance, I need to get the order number of the existing quota order number
            sql = """select quota_order_number_sid from quota_order_numbers qon
            where qon.quota_order_number_id = '""" + self.quota_order_number_id + """'
            and validity_end_date is null
            order by 1 desc;"""
            cur = g.app.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            count = len(rows)
            if count == 0:
                print("Problem - system thinks a quota exists, but it does not")
                sys.exit()
            else:
                row = rows[0]
                self.quota_order_number_sid = row[0]
                print("Retrieved real SID for quota order number", self.quota_order_number_id, "which is", str(self.quota_order_number_sid))

            return ("")

        g.app.transaction_id += 1
        s = g.app.template_quota_order_number
        s = s.replace("[TRANSACTION_ID]", str(g.app.transaction_id))
        s = s.replace("[MESSAGE_ID]", str(g.app.message_id))
        s = s.replace("[RECORD_SEQUENCE_NUMBER]", str(g.app.message_id))
        s = s.replace("[UPDATE_TYPE]", "3")
        s = s.replace("[QUOTA_ORDER_NUMBER_SID]", str(self.quota_order_number_sid))
        s = s.replace("[QUOTA_ORDER_NUMBER_ID]", self.quota_order_number_id)
        s = s.replace("[VALIDITY_START_DATE]", self.validity_start_date)
        s = s.replace("[VALIDITY_END_DATE]", "")

        s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
        g.app.message_id += 1
        g.app.transaction_id += 1

        return (s)

    def create_closure_xml(self):
        g.app.transaction_id += 1
        sql = """select quota_order_number_sid, quota_order_number_id, validity_start_date
        from quota_order_numbers where quota_order_number_id = '""" + self.quota_order_number_id + """'
        and validity_end_date is null
        order by validity_start_date desc
        limit 1"""
        cur = g.app.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        count = len(rows)
        if count > 0:
            rw = rows[0]
            quota_order_number_sid = rw[0]
            quota_order_number_id = rw[1]
            on_validity_start_date = datetime.strftime(rw[2], "%Y-%m-%d")
            if self.quota_order_number_id == "098626":
                print(on_validity_start_date)

            sql = """select quota_order_number_origin_sid, quota_order_number_sid, geographical_area_id,
            validity_start_date, validity_end_date, geographical_area_sid
            from quota_order_number_origins
            where quota_order_number_sid = """ + str(quota_order_number_sid)
            cur = g.app.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            count = len(rows)
            if count > 0:
                rw = rows[0]
                quota_order_number_origin_sid = rw[0]
                quota_order_number_sid = rw[1]
                geographical_area_id = rw[2]
                validity_start_date = datetime.strftime(rw[3], "%Y-%m-%d")
                if rw[4] is not None:
                    validity_end_date = datetime.strftime(rw[4], "%Y-%m-%d")
                geographical_area_sid = rw[5]

                g.app.transaction_id += 1
                g.app.message_id += 1
                g.app.fta_content += '<!-- Start terminate old quota order number and origin for quota ' + self.quota_order_number_id_formatted() + ' //-->\n'
                g.app.fta_content += '\t<env:transaction id="' + str(g.app.transaction_id) + '">\n'
                g.app.fta_content += '\t\t<env:app.message id="' + str(g.app.message_id) + '">\n'
                g.app.fta_content += '\t\t\t<oub:transmission xmlns:oub="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0">\n'
                g.app.fta_content += '\t\t\t<oub:record>\n'
                g.app.fta_content += '\t\t\t\t<oub:transaction.id>' + str(g.app.transaction_id) + '</oub:transaction.id>\n'
                g.app.fta_content += '\t\t\t\t<oub:record.code>360</oub:record.code>\n'
                g.app.fta_content += '\t\t\t\t<oub:subrecord.code>10</oub:subrecord.code>\n'
                g.app.fta_content += '\t\t\t\t<oub:record.sequence.number>' + str(g.app.message_id) + '</oub:record.sequence.number>\n'
                g.app.fta_content += '\t\t\t\t<oub:update.type>1</oub:update.type>\n'
                g.app.fta_content += '\t\t\t\t\t<oub:quota.order.number.origin>\n'
                g.app.fta_content += '\t\t\t\t\t<oub:quota.order.number.origin.sid>' + str(quota_order_number_origin_sid) + '</oub:quota.order.number.origin.sid>\n'
                g.app.fta_content += '\t\t\t\t\t<oub:quota.order.number.sid>' + str(quota_order_number_sid) + '</oub:quota.order.number.sid>\n'
                g.app.fta_content += '\t\t\t\t\t<oub:geographical.area.id>' + geographical_area_id + '</oub:geographical.area.id>\n'
                g.app.fta_content += '\t\t\t\t\t<oub:validity.start.date>' + validity_start_date + '</oub:validity.start.date>\n'
                g.app.fta_content += '\t\t\t\t\t<oub:validity.end.date>' + self.critical_date_string + '</oub:validity.end.date>\n'
                g.app.fta_content += '\t\t\t\t\t<oub:geographical.area.sid>' + str(geographical_area_sid) + '</oub:geographical.area.sid>\n'
                g.app.fta_content += '\t\t\t\t</oub:quota.order.number.origin>\n'
                g.app.fta_content += '\t\t\t</oub:record>\n'
                g.app.fta_content += '\t\t</oub:transmission>\n'
                g.app.fta_content += '\t\t</env:app.message>\n'

            g.app.message_id += 1
            g.app.fta_content += '\t\t<env:app.message id="' + str(g.app.message_id) + '">\n'
            g.app.fta_content += '\t\t\t<oub:transmission xmlns:oub="urn:publicid:-:DGTAXUD:TARIC:MESSAGE:1.0" xmlns:env="urn:publicid:-:DGTAXUD:GENERAL:ENVELOPE:1.0">\n'
            g.app.fta_content += '\t\t\t\t<oub:record>\n'
            g.app.fta_content += '\t\t\t\t\t<oub:transaction.id>' + str(g.app.transaction_id) + '</oub:transaction.id>\n'
            g.app.fta_content += '\t\t\t\t\t<oub:record.code>360</oub:record.code>\n'
            g.app.fta_content += '\t\t\t\t\t<oub:subrecord.code>00</oub:subrecord.code>\n'
            g.app.fta_content += '\t\t\t\t\t<oub:record.sequence.number>' + str(g.app.message_id) + '</oub:record.sequence.number>\n'
            g.app.fta_content += '\t\t\t\t\t<oub:update.type>1</oub:update.type>\n'
            g.app.fta_content += '\t\t\t\t\t<oub:quota.order.number>\n'
            g.app.fta_content += '\t\t\t\t\t\t<oub:quota.order.number.sid>' + str(quota_order_number_sid) + '</oub:quota.order.number.sid>\n'
            g.app.fta_content += '\t\t\t\t\t\t<oub:quota.order.number.id>' + quota_order_number_id + '</oub:quota.order.number.id>\n'
            g.app.fta_content += '\t\t\t\t\t\t<oub:validity.start.date>' + on_validity_start_date + '</oub:validity.start.date>\n'
            g.app.fta_content += '\t\t\t\t\t\t<oub:validity.end.date>' + self.critical_date_string + '</oub:validity.end.date>\n'
            g.app.fta_content += '\t\t\t\t\t</oub:quota.order.number>\n'
            g.app.fta_content += '\t\t\t\t</oub:record>\n'
            g.app.fta_content += '\t\t\t</oub:transmission>\n'
            g.app.fta_content += '\t\t</env:app.message>\n'
            g.app.fta_content += '\t</env:transaction>\n'
            g.app.fta_content += '<!-- End terminate old quota order number and origin for quota ' + self.quota_order_number_id_formatted() + ' //-->\n'
