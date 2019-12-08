import classes.functions as f
import classes.globals as g
import datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys

from classes.measure_component import measure_component


class measure(object):
    def __init__(self, measure_sid, ordernumber, measure_type_id, validity_start_date, validity_end_date,
    geographical_area_id, goods_nomenclature_item_id, additional_code_type_id, additional_code_id,
    reduction_indicator, measure_generating_regulation_role,
    measure_generating_regulation_id, justification_regulation_role, justification_regulation_id,
    stopped_flag, geographical_area_sid, goods_nomenclature_sid,
    additional_code_sid, export_refund_nomenclature_sid):
        # from parameters
        self.measure_sid = measure_sid
        self.quota_order_number_id = ordernumber
        self.measure_type_id = measure_type_id
        self.validity_start_date = validity_start_date
        self.validity_end_date = validity_end_date
        self.geographical_area_id = geographical_area_id
        self.goods_nomenclature_item_id = goods_nomenclature_item_id
        self.additional_code_type_id = additional_code_type_id
        self.additional_code_id = additional_code_id
        self.reduction_indicator = reduction_indicator
        self.measure_generating_regulation_role = measure_generating_regulation_role
        self.measure_generating_regulation_id = measure_generating_regulation_id
        self.justification_regulation_role = justification_regulation_role
        self.justification_regulation_id = justification_regulation_id
        self.stopped_flag = stopped_flag
        self.geographical_area_sid = geographical_area_sid
        self.goods_nomenclature_sid = goods_nomenclature_sid
        self.additional_code_sid = additional_code_sid
        self.export_refund_nomenclature_sid = export_refund_nomenclature_sid
        self.omit = False

        # Initialised
        self.measure_component_list = []
        self.measure_condition_list = []
        self.measure_condition_component_list = []
        self.measure_excluded_geographical_area_list = []
        self.measure_footnote_list = []
        self.measure_partial_temporary_stop_list = []

        # Derived
        if isinstance(self.validity_start_date, str):
            self.validity_start_date = datetime.strptime(self.validity_start_date, "%Y-%m-%d")
        if isinstance(self.validity_end_date, str):
            self.validity_end_date = datetime.strptime(self.validity_end_date, "%Y-%m-%d")

        self.validity_start_year = self.validity_start_date.year
        self.validity_start_month = self.validity_start_date.month
        self.validity_start_day = self.validity_start_date.day

        if self.validity_end_date is not None:
            self.validity_end_year = self.validity_end_date.year
            self.validity_end_month = self.validity_end_date.month
            self.validity_end_day = self.validity_end_date.day
            self.period_length = (self.validity_end_date - self.validity_start_date).days + 1
        else:
            self.validity_end_year = 2099
            self.validity_end_month = 12
            self.validity_end_day = 31
            self.period_length = 99999

        # Get action required on this measure
        # The action string is specified in the arguments to the script
        app = g.app

        if app.action_string == "terminate":
            if self.validity_start_date < app.critical_date:
                if self.validity_end_date is None:					# Starts before Brexit, but has no end date, therefore needs stopping @ Brexit (and not restarting)
                    self.action = "enddate"
                elif self.validity_end_date <= app.critical_date:  # Ends before Brexit, therefore just ignore it
                    self.action = "ignore"
                else:
                    self.action = "enddate"						# Straddles Brexit, therefore end date it
            else:
                self.action = "delete"							# Starts after Brexit, therefore just delete it

        elif app.action_string == "restart":
            if self.validity_start_date < app.critical_date:
                if self.validity_end_date is None:					# Starts before Brexit, but has no end date, therefore needs stopping and recreating
                    self.action = "enddate"
                elif self.validity_end_date <= app.critical_date:  # Ends before Brexit, therefore just ignore it
                    self.action = "ignore"
                else:
                    self.action = "enddate"						# Straddles Brexit, therefore end date it and recreate it
            else:
                self.action = "recreate"						# Starts after Brexit, therefore delete and recreate it with same dates

        if self.action == "ignore":
            app.ignore_count += 1
        elif self.action == "enddate":
            app.enddate_count += 1
        elif self.action == "delete":
            app.delete_count += 1
        elif self.action == "recreate":
            app.recreate_count += 1

    def xml(self):
        app = g.app
        s = app.template_measure
        s = s.replace("[TRANSACTION_ID]", str(app.transaction_id))
        s = s.replace("[MESSAGE_ID]", str(app.sequence_id))
        s = s.replace("[RECORD_SEQUENCE_NUMBER]", str(app.sequence_id))

        if self.action == "restart":
            list_conditions = []
            # self.validity_end_date = ""
            app.last_measure_sid += 1
            self.measure_sid = app.last_measure_sid
            for obj in self.measure_component_list:
                obj.action = "restart"
                obj.update_type = "3"
                obj.measure_sid = self.measure_sid

            for obj in self.measure_condition_list:
                obj.action = "restart"
                obj.update_type = "3"
                obj.measure_sid = self.measure_sid
                app.last_measure_condition_sid += 1
                obj.measure_condition_sid = app.last_measure_condition_sid
                my_condition = [obj.measure_condition_sid_original, obj.measure_condition_sid]
                list_conditions.append(my_condition)

            for obj in self.measure_condition_component_list:
                obj.action = "restart"
                obj.update_type = "3"
                obj.measure_sid = self.measure_sid

                for cond in list_conditions:
                    if obj.measure_condition_sid == cond[0]:
                        obj.measure_condition_sid = cond[1]
                        break

            for obj in self.measure_footnote_list:
                obj.action = "restart"
                obj.update_type = "3"
                obj.measure_sid = self.measure_sid

            for obj in self.measure_excluded_geographical_area_list:
                obj.action = "restart"
                obj.update_type = "3"
                obj.measure_sid = self.measure_sid

            for obj in self.measure_partial_temporary_stop_list:
                obj.action = "restart"
                obj.update_type = "3"
                obj.measure_sid = self.measure_sid

        # Get the update type
        if self.action == "delete":
            s = s.replace("[UPDATE_TYPE]", "2")  # This is a complete deletion [1]
        elif self.action == "enddate":
            s = s.replace("[UPDATE_TYPE]", "1")  # This is an end date, therefore an update [2]
        elif self.action == "restart":
            s = s.replace("[UPDATE_TYPE]", "3")  # This is a restart, therefore an insert [3]

        # Check if the geographical area is being replaced
        # If so, then we need to relook up the SID in the database
        if g.app.action_verbatim in ("n", "newstart"):
            if g.app.destination_geographical_area_id != "":
                self.geographical_area_id = g.app.destination_geographical_area_id
                self.geographical_area_sid = g.app.destination_geographical_area_sid

        s = s.replace("[MEASURE_SID]", f.mstr(self.measure_sid))
        s = s.replace("[MEASURE_TYPE_ID]", f.mstr(self.measure_type_id))
        s = s.replace("[GEOGRAPHICAL_AREA_ID]", f.mstr(self.geographical_area_id))
        s = s.replace("[GOODS_NOMENCLATURE_ITEM_ID]", f.mstr(self.goods_nomenclature_item_id))

        # Get the measure start date - depends on the action
        if self.action == "restart":
            if self.validity_start_date < app.critical_date_plus_one:
                s = s.replace("[VALIDITY_START_DATE]", f.mdate(app.critical_date_plus_one))
            else:
                s = s.replace("[VALIDITY_START_DATE]", f.mdate(self.validity_start_date))
        else:
            s = s.replace("[VALIDITY_START_DATE]", f.mdate(self.validity_start_date))

        s = s.replace("[MEASURE_GENERATING_REGULATION_ROLE]", f.mstr(self.measure_generating_regulation_role))
        s = s.replace("[MEASURE_GENERATING_REGULATION_ID]", f.mstr(self.measure_generating_regulation_id))

        # Get the measure end date - depends on the action
        if self.action == "enddate":
            end_date_to_display = f.mdate(app.critical_date)
            s = s.replace("[VALIDITY_END_DATE]", end_date_to_display)
        elif self.action in ("restart", "delete"):
            if self.validity_end_date == "":
                end_date_to_display = None
            else:
                end_date_to_display = f.mdate(self.validity_end_date)
            s = s.replace("[VALIDITY_END_DATE]", end_date_to_display)

        if end_date_to_display is None or end_date_to_display == "":
            self.justification_regulation_id = ""
            self.justification_regulation_role = ""
        else:
            if self.action == "restart":
                self.justification_regulation_id = self.measure_generating_regulation_id
                self.justification_regulation_role = self.measure_generating_regulation_role
            else:
                if self.justification_regulation_id is None or self.justification_regulation_id == "":
                    self.justification_regulation_id = self.measure_generating_regulation_id
                    self.justification_regulation_role = self.measure_generating_regulation_role

        s = s.replace("[JUSTIFICATION_REGULATION_ROLE]", f.mstr(self.justification_regulation_role))
        s = s.replace("[JUSTIFICATION_REGULATION_ID]", f.mstr(self.justification_regulation_id))
        s = s.replace("[STOPPED_FLAG]", f.mbool(self.stopped_flag))
        s = s.replace("[GEOGRAPHICAL_AREA_SID]", f.mstr(self.geographical_area_sid))
        s = s.replace("[GOODS_NOMENCLATURE_SID]", f.mstr(self.goods_nomenclature_sid))
        s = s.replace("[ORDERNUMBER]", f.mstr(self.quota_order_number_id))
        s = s.replace("[ADDITIONAL_CODE_TYPE_ID]", f.mstr(self.additional_code_type_id))
        s = s.replace("[ADDITIONAL_CODE_ID]", f.mstr(self.additional_code_id))
        s = s.replace("[ADDITIONAL_CODE_SID]", f.mstr(self.additional_code_sid))
        s = s.replace("[REDUCTION_INDICATOR]", f.mstr(self.reduction_indicator))
        s = s.replace("[EXPORT_REFUND_NOMENCLATURE_SID]", f.mstr(self.export_refund_nomenclature_sid))

        s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:goods.nomenclature.item.id></oub:goods.nomenclature.item.id>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:additional.code.type></oub:additional.code.type>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:additional.code></oub:additional.code>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:ordernumber></oub:ordernumber>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:reduction.indicator></oub:reduction.indicator>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:justification.regulation.role></oub:justification.regulation.role>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:justification.regulation.id></oub:justification.regulation.id>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:geographical.area.sid></oub:geographical.area.sid>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:goods.nomenclature.sid></oub:goods.nomenclature.sid>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:additional.code.sid></oub:additional.code.sid>\n", "")
        s = s.replace("\t\t\t\t\t\t<oub:export.refund.nomenclature.sid></oub:export.refund.nomenclature.sid>\n", "")

        app.sequence_id += 1

        self.component_content = ""
        self.condition_content = ""
        self.condition_component_content = ""
        self.exclusion_content = ""
        self.footnote_content = ""
        self.pts_content = ""

        # The code below is executed when we are converting MFNs, i.e. the duty type is 103s and 105s
        if self.measure_type_id in("103", "105") and self.action == "restart":
            # This is the case of North African wines, which need to have the specific
            # local duties removed, as liberalisation applies in full
            if self.additional_code_type_id != "":
                if self.additional_code_id in ("551", "552", "501"):
                    s = ""
                    return (s)

            self.additional_code_type_id = ""
            self.additional_code_id = ""

            # If for whatever reason there are any measures that do not have ERGA OMNES
            # as their measure type, then we should delete them
            if self.geographical_area_id != "1011":
                # print("No Erga Omnes area found")
                s = ""
                return (s)

            # By default, an MFN measure will have zero duties unless it has been explicitly overridden
            # In the first instance, give it a duty of zero
            duty_expression_id = "01"
            duty_amount = 0
            monetary_unit_code = ""
            measurement_unit_code = ""
            measurement_unit_qualifier_code = ""

            # Then, search through the list of specified MFN duties, and if a match is found,
            # then apply the specified components
            found = False
            for mfn in app.mfn_master_list:
                if mfn.goods_nomenclature_item_id == self.goods_nomenclature_item_id:
                    duty_expression_id = mfn.duty_expression_id
                    duty_amount = mfn.duty_amount
                    if mfn.duty_amount == -1:
                        return ("")
                        break
                    monetary_unit_code = mfn.monetary_unit_code
                    measurement_unit_code = mfn.measurement_unit_code
                    measurement_unit_qualifier_code = mfn.measurement_unit_qualifier_code

                    obj = measure_component(self.measure_sid, duty_expression_id, duty_amount, monetary_unit_code,
                    measurement_unit_code, measurement_unit_qualifier_code)
                    self.component_content += obj.xml()
                    found = True
            if not found:
                obj = measure_component(self.measure_sid, "01", 0, "", "", "")
                self.component_content += obj.xml()

        else:
            # This code is executed when we are not looking at MFNs
            # I think that the net effect of this is that all footnotes and conditions are removed from
            # the third ccountry duties - is this a problem?
            if self.action in ("delete", "restart"):
                for obj in self.measure_component_list:
                    obj.action = self.action
                    self.component_content += obj.xml(self.measure_type_id, self.goods_nomenclature_item_id)

                for obj in self.measure_condition_list:
                    obj.action = self.action
                    self.condition_content += obj.xml()

                for obj in self.measure_condition_component_list:
                    obj.action = self.action
                    self.condition_component_content += obj.xml()

                for obj in self.measure_excluded_geographical_area_list:
                    obj.action = self.action
                    self.exclusion_content += obj.xml()

                for obj in self.measure_footnote_list:
                    obj.action = self.action
                    self.footnote_content += obj.xml()

                for obj in self.measure_partial_temporary_stop_list:
                    obj.action = self.action
                    self.pts_content += obj.xml()
                    # print(self.pts_content)

        s = s.replace("[COMPONENTS]\n", self.component_content)
        s = s.replace("[CONDITIONS]\n", self.condition_content)
        s = s.replace("[CONDITION_COMPONENTS]\n", self.condition_component_content)
        s = s.replace("[EXCLUDED]\n", self.exclusion_content)
        s = s.replace("[FOOTNOTES]\n", self.footnote_content)
        s = s.replace("[PTS]\n", self.pts_content)
        return (s)

    def get_components(self):
        self.component_list = []
        sql = """select duty_expression_id, duty_amount, monetary_unit_code,
        measurement_unit_code, measurement_unit_qualifier_code from measure_components
        where measure_sid = %s;"""
        params = [
            self.measure_sid
        ]
        cur = g.app.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        if len(rows) == 0:
            # This just pulls out the ad valorem part of the EPS duty
            sql = """select duty_amount from measure_condition_components mcc, measure_conditions mc
            where mcc.measure_condition_sid = mc.measure_condition_sid
            and measure_sid = %s and duty_expression_id = '01';"""
            params = [
                self.measure_sid
            ]
            cur = g.app.conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            # print(len(rows))
            duty_list = []
            for row in rows:
                duty_amount = row[0]
                if duty_amount not in duty_list:
                    duty_list.append(duty_amount)
            if len(duty_list) > 1:
                print("We have an EPS problem on measure", str(self.measure_sid))
                sys.exit()
            else:
                mc = measure_component(self.measure_sid, "01", duty_list[0], "", "", "")

                mc.measure_sid = self.measure_sid
                mc.duty_expression_id = "01"
                mc.duty_amount = duty_list[0]
                mc.measurement_unit_code = ""
                mc.measurement_unit_qualifier_code = ""
                mc.monetary_unit_code = ""
                self.measure_component_list.append(mc)
        else:
            # These are just standard measure components, not EPS rates
            for row in rows:
                mc = measure_component(self.measure_sid, row[0], row[1], row[2], row[3], row[4])
                mc.measure_sid = self.measure_sid
                mc.duty_expression_id = row[0]
                mc.duty_amount = row[1]
                mc.monetary_unit_code = row[2]
                mc.measurement_unit_code = row[3]
                mc.measurement_unit_qualifier_code = row[4]
                self.measure_component_list.append(mc)

    def seasonal_xml(self):
        g.app.transaction_id += 1
        g.app.sequence_id += 1
        out = ""
        # How many years worth of data should we create?
        # Initial view is 2 years
        for i in range(0, 3):
            s = g.app.template_measure
            g.app.last_measure_sid += 1
            self.measure_sid = g.app.last_measure_sid
            s = s.replace("[UPDATE_TYPE]", "3")
            s = s.replace("[TRANSACTION_ID]", str(g.app.transaction_id))
            s = s.replace("[MESSAGE_ID]", str(g.app.sequence_id))
            s = s.replace("[RECORD_SEQUENCE_NUMBER]", str(g.app.sequence_id))

            s = s.replace("[MEASURE_SID]", str(self.measure_sid))
            s = s.replace("[MEASURE_TYPE_ID]", f.mstr(self.measure_type_id))
            s = s.replace("[GEOGRAPHICAL_AREA_ID]", f.mstr(self.geographical_area_id))
            s = s.replace("[GOODS_NOMENCLATURE_ITEM_ID]", f.mstr(self.goods_nomenclature_item_id))
            s = s.replace("[ADDITIONAL_CODE_TYPE_ID]", "")
            s = s.replace("[ADDITIONAL_CODE_ID]", "")
            s = s.replace("[REDUCTION_INDICATOR]", "")
            s = s.replace("[ORDERNUMBER]", "")
            s = s.replace("[STOPPED_FLAG]", "0")
            s = s.replace("[ADDITIONAL_CODE_ID]", "")
            s = s.replace("[MEASURE_GENERATING_REGULATION_ID]", self.measure_generating_regulation_id)
            s = s.replace("[MEASURE_GENERATING_REGULATION_ROLE]", "1")
            s = s.replace("[JUSTIFICATION_REGULATION_ID]", self.justification_regulation_id)
            s = s.replace("[JUSTIFICATION_REGULATION_ROLE]", "1")
            s = s.replace("[GEOGRAPHICAL_AREA_SID]", f.mstr(self.geographical_area_sid))
            s = s.replace("[GOODS_NOMENCLATURE_SID]", f.mstr(self.goods_nomenclature_sid))
            s = s.replace("[ADDITIONAL_CODE_SID]", "")
            s = s.replace("[EXPORT_REFUND_NOMENCLATURE_SID]", "")

            # Now do the dates
            start_date = self.validity_start_date
            start_date = start_date + relativedelta(years=i)
            start_date_string = datetime.strftime(start_date, "%Y-%m-%d")
            if start_date_string < g.app.critical_date_plus_one_string:
                start_date_string = g.app.critical_date_plus_one_string

            end_date = self.validity_end_date
            end_date = end_date + relativedelta(years=i)
            end_date_string = datetime.strftime(end_date, "%Y-%m-%d")

            s = s.replace("[VALIDITY_START_DATE]", start_date_string)
            s = s.replace("[VALIDITY_END_DATE]", end_date_string)

            s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
            s = s.replace("\t\t\t\t\t\t<oub:goods.nomenclature.item.id></oub:goods.nomenclature.item.id>\n", "")
            s = s.replace("\t\t\t\t\t\t<oub:additional.code.type></oub:additional.code.type>\n", "")
            s = s.replace("\t\t\t\t\t\t<oub:additional.code></oub:additional.code>\n", "")
            s = s.replace("\t\t\t\t\t\t<oub:ordernumber></oub:ordernumber>\n", "")
            s = s.replace("\t\t\t\t\t\t<oub:reduction.indicator></oub:reduction.indicator>\n", "")
            s = s.replace("\t\t\t\t\t\t<oub:justification.regulation.role></oub:justification.regulation.role>\n", "")
            s = s.replace("\t\t\t\t\t\t<oub:justification.regulation.id></oub:justification.regulation.id>\n", "")
            s = s.replace("\t\t\t\t\t\t<oub:geographical.area.sid></oub:geographical.area.sid>\n", "")
            s = s.replace("\t\t\t\t\t\t<oub:goods.nomenclature.sid></oub:goods.nomenclature.sid>\n", "")
            s = s.replace("\t\t\t\t\t\t<oub:additional.code.sid></oub:additional.code.sid>\n", "")
            s = s.replace("\t\t\t\t\t\t<oub:export.refund.nomenclature.sid></oub:export.refund.nomenclature.sid>\n", "")

            self.component_content = ""
            for obj in self.measure_component_list:
                g.app.sequence_id += 1
                obj.measure_sid = self.measure_sid
                self.component_content += obj.xml(self.measure_type_id, self.goods_nomenclature_item_id)

            s = s.replace("[COMPONENTS]\n", self.component_content)
            s = s.replace("[CONDITIONS]\n", "")
            s = s.replace("[CONDITION_COMPONENTS]\n", "")
            s = s.replace("[EXCLUDED]\n", "")
            s = s.replace("[FOOTNOTES]\n", "")
            s = s.replace("[PTS]\n", "")

            out += s

        return (out)
