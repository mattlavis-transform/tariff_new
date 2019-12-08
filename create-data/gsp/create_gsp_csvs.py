import os
import sys
from openpyxl import Workbook
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font, NamedStyle
from classes.load_commodity import load_commodity
import csv


def fmt(s):
    if len(s) == 7:
        s = "0" + s
    if len(s) < 10:
        s = s + "0" * (10 - len(s))
    return (s)


def num(s):
    s = s.strip()
    if s == "":
        return ""
    else:
        s = s.replace("%", "")
        return (s.strip())


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_DIR = os.path.join(BASE_DIR, "source")

filename = os.path.join(SOURCE_DIR, "GSP data.csv")
commodities = []
with open(filename, newline='') as csvfile:
    csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    next(csv_reader)
    for row in csv_reader:
        commodity_code = row[0]
        pref_2020 = num(row[2])
        pref_2020_exclusions = row[3]
        pref_2027 = num(row[4])
        pref_2005 = num(row[5])
        pref_1032 = num(row[6])

        obj = load_commodity(commodity_code, pref_2020, pref_2020_exclusions, pref_2027, pref_2005, pref_1032)
        commodities.append(obj)


data_2020 = ""
data_2027 = ""
data_2005 = ""
data_1032 = ""
data_2020_exclusions = ""

for item in commodities:
    if item.pref_2020 != "":
        data_2020 += "2020," + fmt(item.commodity_code) + "," + item.pref_2020 + "\n"
    if item.pref_2027 != "":
        data_2027 += "2027," + fmt(item.commodity_code) + "," + item.pref_2027 + "\n"
    if item.pref_2005 != "":
        data_2005 += "2005," + fmt(item.commodity_code) + "," + item.pref_2005 + "\n"
    if item.pref_1032 != "":
        data_1032 += "1032," + fmt(item.commodity_code) + "," + item.pref_1032 + "\n"
    if item.pref_2020_exclusions != "":
        data_2020_exclusions += "1032," + fmt(item.commodity_code) + "," + item.pref_2020_exclusions_code + "\n"

f = open(os.path.join(SOURCE_DIR, "gsp_2020.csv"), "w+")
f.write(data_2020)

f = open(os.path.join(SOURCE_DIR, "gsp_2027.csv"), "w+")
f.write(data_2027)

f = open(os.path.join(SOURCE_DIR, "gsp_2005.csv"), "w+")
f.write(data_2005)

f = open(os.path.join(SOURCE_DIR, "gsp_1032.csv"), "w+")
f.write(data_1032)

f = open(os.path.join(SOURCE_DIR, "gsp_2020_exclusions.csv"), "w+")
f.write(data_2020_exclusions)
