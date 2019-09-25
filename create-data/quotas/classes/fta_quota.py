import datetime
import sys
from dateutil.relativedelta import relativedelta

import classes.functions as f
import classes.globals as g
from classes.quota_order_number import quota_order_number
from classes.quota_definition import quota_definition
from classes.measure import measure
from classes.measure_component import measure_component
from classes.quota_order_number_origin import quota_order_number_origin
from classes.quota_order_number_origin_exclusion import quota_order_number_origin_exclusion
from classes.quota_association import quota_association

class fta_quota(object):
	def __init__(self, country_name, quota_order_number_id, annual_volume, increment, \
	eu_period_starts, eu_period_ends, interim_volume, units, preferential, include_interim_period, \
	quota_order_number_sid = -1):
		self.country_name           = country_name
		self.quota_order_number_id  = quota_order_number_id
		self.annual_volume          = annual_volume
		self.increment              = increment
		self.eu_period_starts       = eu_period_starts
		self.eu_period_ends         = eu_period_ends
		self.interim_volume         = interim_volume
		self.units                  = units
		self.measure_generating_regulation_id	= ""
		self.override_origins		= False
		self.is_new					= False
		self.quota_definition_list	= []
		self.origin_list			= []
		self.origin_objects			= []
		self.origin_xml				= ""
		self.preferential			= preferential
		self.include_interim_period	= include_interim_period
		self.origin_exclusions = []
		self.quota_definition_list = []
		
		if self.quota_order_number_id[0:3] == "094":
			self.licensed = True
			self.method = "Licensed"
		else:
			self.licensed = False
			self.method = "FCFS"

		# Only process the quota if the order number starts with "09"
		self.is_valid = True
		if self.quota_order_number_id[0:2] != "09":
			self.is_valid = False
			return

		# Check to see if this is a new quota
		self.check_exists()
		if self.is_new:
			self.quota_order_number_sid = g.app.last_quota_order_number_sid
			g.app.last_quota_order_number_sid += 1

			self.measure_list = []
			for m in g.app.new_quotas:
				if m.quota_order_number_id == self.quota_order_number_id:
					goods_nomenclature_item_id = self.format_comm_code(m.goods_nomenclature_item_id)
					#print ("For new quota", m.quota_order_number_id, "include a new comm code", goods_nomenclature_item_id)

					duty_amount = -1
					monetary_unit_code = ""
					measurement_unit_code = ""
					measurement_unit_qualifier_code = ""

					if self.preferential == "Y":
						measure_type_id = "143"
					else:
						measure_type_id = "122"

					validity_start_date = ""
					validity_end_date = ""
					measure_sid = -1

					if self.quota_order_number_id == "097709":
						a = 1

					m = measure(goods_nomenclature_item_id, self.quota_order_number_id, "", duty_amount,
					monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code, measure_type_id,
					validity_start_date, validity_end_date, measure_sid)

					mc = measure_component(-1, "01", 0, "", "", "")
					m.measure_component_list.append (mc)

					self.measure_list.append (m)
					a = 1
					b = 1
					#print (str(len(self.measure_list)), "in the measure list for quota", self.quota_order_number_id)


		else:
			self.get_sid()
			self.common_elements()
			self.get_measure_components()
			self.get_measures()
			self.assign_measure_components()

	def format_comm_code(self, s):
		s = s.replace(" ", "")
		if len(s) < 10:
			s += "0" * (10 - len(s))
		return s

	def common_elements(self):
		self.get_origins()
		# Get balances and units
		self.standardise_numbers()
		self.measurement_unit = ""
		if self.units != "":
			self.get_unit()

		self.get_definitions()

	def get_origins(self):
		# Get country origins
		self.origin_list = []
		self.geographical_area_id	= ""

		if self.is_new:
			if self.country_name != "":
				self.get_country_name()
				self.create_new_origin()
		else:
			if self.country_name[0:4] == "Only":
				self.override_origins = True
			if self.country_name != "":
				self.get_country_name()


			if self.licensed == False:
				self.get_origins_from_db()
				self.get_origin_exclusions_from_db()
				try:
					self.primary_origin = self.origin_list[0][2]
				except:
					self.is_new = True
			else:
				self.primary_origin = self.geographical_area_id

	def create_new_origin(self):
		validity_start_date = g.app.critical_date_plus_one.strftime("%Y-%m-%d")
		self.primary_origin = self.geographical_area_id

		obj = quota_order_number_origin(self.quota_order_number_sid, self.geographical_area_id, validity_start_date)
		self.origin_objects.append (obj)

		if self.quota_order_number_id not in g.app.origins_added:
			for obj in self.origin_objects:
				self.origin_xml += obj.xml()

		origin_object = list()
		origin_object.append (obj.quota_order_number_origin_sid)
		origin_object.append (obj.quota_order_number_sid)
		origin_object.append (obj.geographical_area_id)
		origin_object.append (obj.geographical_area_sid)

		self.origin_list.append(origin_object)
		
		g.app.origins_added.append(self.quota_order_number_id)


	def check_exists(self):
		if self.quota_order_number_id[0:3] == "094":
			sql = "select * from ml.measures_real_end_dates where ordernumber = '" + self.quota_order_number_id.strip() + "' limit 10"
			cur = g.app.conn.cursor()
			cur.execute(sql)
			rows = cur.fetchall()
			count = len(rows)
			if count == 0:
				self.is_new = True
				#print ("Found a new licensed quota in function check_exists - quota", self.quota_order_number_id)
			else:
				self.is_new = False
		else: 
			sql = "select * from quota_order_numbers where quota_order_number_id = '" + self.quota_order_number_id.strip() + "'"
			cur = g.app.conn.cursor()
			cur.execute(sql)
			rows = cur.fetchall()
			count = len(rows)
			if count == 0:
				self.is_new = True
				#print ("Found a new FCFS quota in function check_exists - quota", self.quota_order_number_id)
			else:
				self.is_new = False


	def assign_measure_components(self):
		for m in self.measure_list:
			m.measure_component_list = []
			for mc in self.measure_component_list:
				if mc.measure_sid == m.measure_sid:
					m.measure_component_list.append (mc)

		for m in self.measure_list:
			component_count = len(m.measure_component_list)
			for i in range(component_count -1, -1, -1):
				mc = m.measure_component_list[i]
				if mc.duty_expression_id in ('12', '14', '21', '25', '27', '29', '99'):
					del m.measure_component_list[i]

		commodity_list = []
		for m in self.measure_list:
			commodity_list.append (m.goods_nomenclature_item_id)


		fname = "measure_components.txt"
		f1 = open (fname, "a+")
		for m in self.measure_list:
			duty_string = ""
			for item in m.duty_list:
				duty_string += item + ", "

			f1.write (str(m.measure_sid) + " : " + m.quota_order_number_id + " : " + duty_string + "\n")
		f1.close()


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
			self.quota_order_number_sid  = row[0]


	def standardise_numbers(self):
		self.annual_volume	= self.standardise(self.annual_volume)
		self.increment		= self.standardise(self.increment)
		self.interim_volume	= self.standardise(self.interim_volume)


	def standardise(self, var):
		if var in ("", "n/a"):
			var = 0
		var = str(var)
		var = var.replace(",", "")
		try:
			var = float(var)
		except:
			print ("Standardisation failure on", self.quota_order_number_id)
			sys.exit()
		
		try:
			var = int(var)
		except:
			print ("Standardisation failure on", self.quota_order_number_id)
			sys.exit()

		return (var)


	def get_country_name(self):
		self.country_name = self.country_name.replace("Only ", "")
		for item in g.app.geographical_areas:
			if item.country_description == self.country_name:
				self.geographical_area_id				= item.geographical_area_id
				self.measure_generating_regulation_id	= item.measure_generating_regulation_id
				break
		a = 1

	def get_origins_from_db(self):
		if self.override_origins == True:
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
				print (self.quota_order_number_id, self.geographical_area_id)
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

			

			self.origin_list		= []
			self.origin_exclusions	= []
			obj = list()
			obj.append (quota_order_number_origin_sid)
			obj.append (quota_order_number_sid)
			obj.append (self.geographical_area_id)
			obj.append (geographical_area_sid)

			self.origin_list.append (obj)
			# Needs completing
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
				quota_order_number_origin_sid	= row[0]
				quota_order_number_sid			= row[1]
				geographical_area_id			= row[2]
				geographical_area_sid			= row[3]
				obj = list()
				obj.append (quota_order_number_origin_sid)
				obj.append (quota_order_number_sid)
				obj.append (geographical_area_id)
				obj.append (geographical_area_sid)
				self.origin_list.append (obj)


	def get_origin_exclusions_from_db(self):
		if self.override_origins == True:
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
			print ("Error in get_origin_exclusions_from_db", sql)
			sys.exit()
		rows = cur.fetchall()
		for row in rows:
			quota_order_number_origin_sid	= row[0]
			excluded_geographical_area_sid	= row[1]
			excluded_geographical_area_id	= row[2]
			obj = list()
			obj.append (quota_order_number_origin_sid)
			obj.append (excluded_geographical_area_sid)
			obj.append (excluded_geographical_area_id)
			self.origin_exclusions.append (obj)


	def get_unit(self):
		list1 = ["Kilograms", "Litres of pure alcohol", "Litres", "Kilograma", "Pieces", "Head", "Hectolitres", "Nar"]
		list2 = ["KGM", "LPA", "LTR", "KGM", "NAR", "HLT", "NAR"]
		for i in range(0, len(list1) - 1):
			item = list1[i]
			if self.units == item:
				self.measurement_unit = list2[i]
				break


	def get_measures(self):
		year_ago = g.app.critical_date + relativedelta(years = -1)
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
			measure_sid					= row[0]
			goods_nomenclature_item_id	= row[1]
			measure_type_id				= row[2]
			validity_start_date			= row[3]
			validity_end_date			= row[4]

			# Check on overrides
			# TO DO
			
			duty_amount = -1
			monetary_unit_code = ""
			measurement_unit_code = ""
			measurement_unit_qualifier_code = ""
			m = measure(goods_nomenclature_item_id, self.quota_order_number_id, "", duty_amount,
			monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code, measure_type_id,
			validity_start_date, validity_end_date, measure_sid)
			self.measure_list.append (m)


	def get_measure_components(self):
		year_ago = g.app.critical_date + relativedelta(years = -1)
		sql = """
		select distinct on (m.goods_nomenclature_item_id, m.validity_start_date, mc.duty_expression_id)
		m.measure_sid, m.goods_nomenclature_item_id, mc.duty_expression_id, mc.duty_amount,
		mc.monetary_unit_code, mc.measurement_unit_code, mc.measurement_unit_qualifier_code
		from goods_nomenclatures g, ml.measures_real_end_dates m
		left outer join measure_components mc on mc.measure_sid = m.measure_sid
		where m.goods_nomenclature_item_id = g.goods_nomenclature_item_id
		and m.goods_nomenclature_item_id = g.goods_nomenclature_item_id
		and g.producline_suffix = '80'
		and m.measure_type_id in ('143', '146')
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
			measure_sid						= row[0]
			goods_nomenclature_item_id		= row[1]
			duty_expression_id				= row[2]
			duty_amount						= row[3]
			monetary_unit_code				= row[4]
			measurement_unit_code			= row[5]
			measurement_unit_qualifier_code	= row[6]

			mc = measure_component(measure_sid, duty_expression_id, duty_amount, monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code)
			self.measure_component_list.append (mc)

		contains_valid_components = False
		for mc in self.measure_component_list:
			if mc.duty_amount != None:
				self.measure_component_list
				contains_valid_components = True
				break

		component_count = len(self.measure_component_list)
		for i in range(component_count - 1, -1, -1):
			mc = self.measure_component_list[i]
			if mc.duty_amount == None:
				del self.measure_component_list[i]
					

		if contains_valid_components == False:
			self.get_measure_condition_components()


		if len(self.measure_component_list) == 0:
			if self.quota_order_number_id[0:3] != "094":
				self.get_measure_condition_components()
		

	def get_measure_condition_components(self):
		year_ago = g.app.critical_date + relativedelta(years = -1)
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
		and m.measure_type_id in ('143', '146')
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
			measure_sid						= row[0]
			goods_nomenclature_item_id		= row[1]
			duty_amount						= row[2]
			duty_expression_id				= "01"
			monetary_unit_code				= ""
			measurement_unit_code			= ""
			measurement_unit_qualifier_code	= ""

			mc = measure_component(measure_sid, duty_expression_id, duty_amount, monetary_unit_code, measurement_unit_code, measurement_unit_qualifier_code)
			self.measure_component_list.append (mc)


	def get_definitions(self):
		d = g.app.critical_date
		d2 = g.app.critical_date_plus_one

		crit2 = datetime.date(d.year, d.month, d.day)
		critplusone2 = datetime.date(d2.year, d2.month, d2.day)

		# Get the interim period
		if (self.eu_period_ends <= crit2 or (self.eu_period_starts - critplusone2).days == 0):
			# There is no opening period
			pass
		else:
			if self.include_interim_period != "N":
				print ("Creating an interim period on quota", self.quota_order_number_id, self.include_interim_period)
				validity_start_date		= datetime.date(critplusone2.year, critplusone2.month, critplusone2.day)
				validity_end_date = self.eu_period_ends
				length = (validity_end_date - validity_start_date).days + 1

				try:
					qd = quota_definition(self.quota_order_number_id, self.quota_order_number_sid, "143", self.method, validity_start_date,
					validity_end_date, length, self.interim_volume, self.measurement_unit, 3, "N", 90, "",
					"", "", "", self.primary_origin)
				except:
					print (self.quota_order_number_id, "failure on primary origin")
					sys.exit()
				self.quota_definition_list.append (qd)

		# Now get two years' worth of additional periods
		for i in range(1, 3):
			opening_balance = (i * int(self.increment)) + int(self.annual_volume)
			if self.eu_period_starts <= crit2:
				validity_start_date = self.eu_period_starts + relativedelta(years = i)
				validity_end_date	= self.eu_period_ends + relativedelta(years = i)
			else:
				validity_start_date = self.eu_period_starts + relativedelta(years = (i - 1))
				validity_end_date	= self.eu_period_ends + relativedelta(years = (i - 1))
			length = (validity_end_date - validity_start_date).days + 1

			if self.is_new:
				self.critical_status = "Y"
			else:
				self.critical_status = "N"
			
			qd = quota_definition(self.quota_order_number_id, self.quota_order_number_sid, "143", self.method, validity_start_date,
			validity_end_date, length, opening_balance, self.measurement_unit, 3, self.critical_status, 90, "",
			"", "", "", self.primary_origin)
			self.quota_definition_list.append (qd)


	def get_quota_associations(self):
		# Now get the associations
		for qd in self.quota_definition_list:
			for qa in g.app.quota_associations_list:
				if qa.main_quota_order_number_id == qd.quota_order_number_id:
					main_quota_order_number_id	= self.quota_order_number_id
					sub_quota_order_number_id	= qa.sub_quota_order_number_id
					relation_type				= qa.relation_type
					coefficient					= qa.coefficient

					# Get sub SID
					sub_quota_definition_sid = None
					for q in g.app.quota_list:
						for qd2 in q.quota_definition_list:
							if qd2.quota_order_number_id == qa.sub_quota_order_number_id:
								if qd.validity_start_date == qd2.validity_start_date:
									sub_quota_definition_sid = qd2.quota_definition_sid

									print ("sub_quota_definition_sid", str(sub_quota_definition_sid))

									obj = quota_association(main_quota_order_number_id, sub_quota_order_number_id, relation_type, coefficient)
									obj.main_quota_definition_sid = qd.quota_definition_sid
									obj.sub_quota_definition_sid = sub_quota_definition_sid
									g.app.new_quota_associations.append(obj)
									g.app.association_count += 1
									#print ("Assoc", str(g.app.association_count))
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
						comm_list.append (m.goods_nomenclature_item_id)
						m.measure_sid = g.app.last_measure_sid
						g.app.last_measure_sid += 1
						m.quota_order_number_id				= self.quota_order_number_id
						m.geographical_area_id				= o[2].strip()
						m.geographical_area_sid				= o[3]
						m.measure_generating_regulation_id	= self.measure_generating_regulation_id
						m.justification_regulation_id		= self.measure_generating_regulation_id					
						m.measure_type_id					= m.measure_type_id
						m.validity_start_date				= datetime.datetime.strftime(d.validity_start_date, "%Y-%m-%d")
						m.validity_end_date					= datetime.datetime.strftime(d.validity_end_date, "%Y-%m-%d")

						m.measure_excluded_geographical_area_list = self.get_measure_exclusion_list(o[0], o[2])
						if m.goods_nomenclature_item_id in ('0707000510', '0707000520', '0707000590', '0707000599'): #  and m.geographical_area_id in ('IL', 'CH', '1001'):
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
				tmp.append (obj_exclusion)
		return (tmp)

	def quota_order_number_xml(self):
		self.validity_start_date = datetime.datetime.strftime(g.app.critical_date_plus_one, "%Y-%m-%d")
		if self.quota_order_number_id[0:3] == "094":
			return ("")

		s = g.app.template_quota_order_number
		s = s.replace("[TRANSACTION_ID]",			str(g.app.transaction_id))
		s = s.replace("[MESSAGE_ID]",				str(g.app.message_id))
		s = s.replace("[RECORD_SEQUENCE_NUMBER]",	str(g.app.message_id))
		s = s.replace("[UPDATE_TYPE]",				"3")
		s = s.replace("[QUOTA_ORDER_NUMBER_SID]",	str(self.quota_order_number_sid))
		s = s.replace("[QUOTA_ORDER_NUMBER_ID]",	self.quota_order_number_id)
		s = s.replace("[VALIDITY_START_DATE]",		self.validity_start_date)
		s = s.replace("[VALIDITY_END_DATE]",		"")

		s = s.replace("\t\t\t\t\t\t<oub:validity.end.date></oub:validity.end.date>\n", "")
		g.app.message_id +=1
		g.app.transaction_id +=1


		"""
		for obj in self.origin_list:
			g.app.last_quota_order_number_origin_sid += 1
			obj.quota_order_number_origin_sid = g.app.last_quota_order_number_origin_sid
			obj.quota_order_number_sid = self.quota_order_number_sid
			obj.description = self.subject
			s += obj.xml()

		for obj in self.quota_definition_list:
			obj.quota_order_number_sid = self.quota_order_number_sid
			obj.description = self.subject
			s += obj.xml()
		"""

		return (s)