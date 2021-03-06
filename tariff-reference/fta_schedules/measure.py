import re
import sys
from datetime import datetime

import functions as f
import glob as g
from duty import duty

class measure(object):
	def __init__(self, measure_sid, commodity_code, quota_order_number_id, validity_start_date, validity_end_date, geographical_area_id, reduction_indicator):
		# Get parameters from instantiator
		self.measure_sid					= measure_sid
		self.commodity_code               	= commodity_code
		self.quota_order_number_id			= quota_order_number_id

		if validity_start_date == None:
			self.validity_start_date	= None
		else:
			if isinstance(validity_start_date, str):
				self.validity_start_date	= datetime.strptime(validity_start_date, "%Y-%m-%d")
			else:
				self.validity_start_date	= validity_start_date
		
		if validity_end_date == None:
			self.validity_end_date	= None
		else:
			if isinstance(validity_end_date, str):
				self.validity_end_date	= datetime.strptime(validity_end_date, "%Y-%m-%d")
			else:
				self.validity_end_date	= validity_end_date

		self.reduction_indicator			= reduction_indicator

		self.validity_start_day		= datetime.strftime(self.validity_start_date, "%d")
		self.validity_start_month	= datetime.strftime(self.validity_start_date, "%m")
		self.validity_start_year	= datetime.strftime(self.validity_start_date, "%Y")
		
		if self.validity_end_date is not None:
			self.extent = (self.validity_end_date - self.validity_start_date).days + 1
			self.validity_end_day		= datetime.strftime(self.validity_end_date, "%d")
			self.validity_end_month		= datetime.strftime(self.validity_end_date, "%m")
			self.validity_end_year		= datetime.strftime(self.validity_end_date, "%Y")
			self.period_start			= str(self.validity_start_day).zfill(2) + "/" + str(self.validity_start_month).zfill(2) 
			self.period_end				= str(self.validity_end_day).zfill(2) + "/" + str(self.validity_end_month).zfill(2)
			self.period					= self.period_start + " to " + self.period_end
		else:
			self.extent = -1
			self.validity_end_day		= 0
			self.validity_end_month		= 0
			self.validity_end_year		= 0
			self.period_start			= ""
			self.period_end				= ""
			self.period					= ""

		self.geographical_area_id			= geographical_area_id

		self.assigned                     	= False
		self.combined_duty          		= ""
		self.combined_duty_rta          	= ""
		self.duty_list              		= []
		self.suppress						= False
		self.marked							= False
		self.measure_count          		= 0
		self.measure_type_count     		= 0
		self.seasonal_list					= []
		self.is_siv							= False


	def xml_without_dates(self):
		return "<w:r><w:t>" + self.combined_duty + "</w:t></w:r>"


	def xml_with_dates(self):
		whitespace = "<w:tab/>"
		s = "<w:r><w:t>"
		s += self.period
		s += "</w:t></w:r><w:r>" + whitespace + "<w:t>" + self.combined_duty + "</w:t></w:r>"
		s += "<w:r><w:br/></w:r>"
		return (s)

	def excel_with_dates(self):
		whitespace = "\t"
		s = self.period + whitespace + self.combined_duty_rta + "\r\n"
		return (s)

	def combine_duties(self):
		self.combined_duty			= ""
		self.combined_duty_rta    	= ""

		self.measure_list         	= []
		self.measure_type_list    	= []
		self.additional_code_list 	= []

		for d in self.duty_list:
			d.geographical_area_id = self.geographical_area_id
			self.measure_type_list.append(d.measure_type_id)
			self.measure_list.append(d.measure_sid)
			self.additional_code_list.append(d.additional_code_id)

		measure_type_list_unique    = set(self.measure_type_list)
		measure_list_unique         = set(self.measure_list)
		additional_code_list_unique = set(self.additional_code_list)

		self.measure_count      	= len(measure_list_unique)
		self.measure_type_count		= len(measure_type_list_unique)
		self.additional_code_count	= len(additional_code_list_unique)
		
		for d in self.duty_list:
			self.combined_duty		+= d.duty_string + " "
			self.combined_duty_rta	+= d.duty_string_rta + " "

		self.combined_duty = self.combined_duty.replace("  ", " ")
		self.combined_duty = self.combined_duty.strip()

		self.combined_duty_rta = self.combined_duty_rta.replace("  ", " ")
		self.combined_duty_rta = self.combined_duty_rta.strip()
		self.combined_duty_rta = self.combined_duty_rta.strip("\n")
		self.combined_duty_rta = self.combined_duty_rta.strip("\r")

		# Now add in the Meursing components
		if "ACR" in self.combined_duty or "SDR" in self.combined_duty or "FDR" in self.combined_duty:
			# print ("Reduction indicator", self.reduction_indicator)
			meursing_percentage = g.app.get_meursing_percentage(self.reduction_indicator, self.geographical_area_id)
			self.combined_duty = "CAD - " + self.combined_duty + ") " + str(meursing_percentage) + "%"
			self.combined_duty = self.combined_duty.replace(" + ", " + (", 1)
			self.combined_duty = self.combined_duty.replace("ACR", "AC")
			self.combined_duty = self.combined_duty.replace("FDR", "FD")
			self.combined_duty = self.combined_duty.replace("SDR", "SD")

		# Now add in the Meursing components
		elif "AC" in self.combined_duty or "SD" in self.combined_duty or "FD" in self.combined_duty:
			self.combined_duty = "CAD - " + self.combined_duty + ") 100%"
			self.combined_duty = self.combined_duty.replace(" + ", " + (", 1)

class period(object):
	def __init__(self, validity_start_day, validity_start_month):
		self.validity_start_day		= validity_start_day
		self.validity_start_month	= validity_start_month
		self.marked					= False
