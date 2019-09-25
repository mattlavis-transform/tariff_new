# Import libraries
import requests
import urllib.request
import time
from bs4 import BeautifulSoup
import csv, sys

def left(s, amount):
    return s[:amount]

def right(s, amount):
    return s[-amount:]

def mid(s, offset, amount):
    return s[offset:offset+amount]

def fmt(s):
    return left(s, 8)
    """
    if right(s, 8) == "00000000":
        return left(s, 2)
    elif right(s, 6) == "000000":
        return left(s, 4)
    if right(s, 4) == "0000":
        return left(s, 6)
    if right(s, 2) == "00":
        return left(s, 8)
    else:
        return s
    """

def get_significant_digits(s):
    if right(s, 8) == "00000000":
        return 2
    elif right(s, 6) == "000000":
        return 4
    elif right(s, 4) == "0000":
        return 6
    elif right(s, 2) == "00":
        return 8
    else:
        return 10

commodities = []
filename = "/Users/matt.admin/projects/tariffs/tariff-reference/rtac_ods/source/chapters_all.csv"
with open(filename, 'r') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in reader:
        commodity = fmt(row[1])
        productline_suffix = row[2]
        significant_digits = get_significant_digits(commodity)
        if productline_suffix == "80" and significant_digits > 6:
            commodities.append(commodity)

cset = set(commodities)
cset = sorted(cset)
file_object  = open("source/tariffnumber_commodities.csv", "w+")

for commodity in cset:
    url = 'https://www.tariffnumber.com/2019/' + commodity
    #print (url)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    try:
        a = soup.find(attrs={"itemprop" : "description"})
        heading = a.text
        heading = heading.replace("\n", "")
        heading = heading.replace("\r", "")
        heading = heading.replace('"', "'")
        
        file_object.write ('"' + commodity + '","' + heading + '"\n')
        print (commodity)
    except:
        pass
