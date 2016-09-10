#!/usr/bin/python2.7
#
# Assignment3 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
import re
from math import sin, cos, sqrt, atan2, radians

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    businesslist = []
    for document in collection.find({"city": re.compile(cityToSearch, re.IGNORECASE)}):
        record = '%s$%s$%s$%s' % (document['name'], document['full_address'].replace('\n', ' '), document['city'], document['state'])
        businesslist.append(record.upper())

    fw = open(saveLocation1, 'w')
    fw.write("\n".join(businesslist))
    fw.close()

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    businesslist = []
    for document in collection.find({"categories": {"$in": categoriesToSearch}}):
        radius = 3959.0
        my_lat = radians(float(myLocation[0]))
        my_lon = radians(float(myLocation[1]))
        business_lat = radians(float(document['latitude']))
        business_lon = radians(float(document['longitude']))

        diff_lon = business_lon - my_lon
        diff_lat = business_lat - my_lat

        a = sin(diff_lat / 2)**2 + cos(my_lat) * cos(business_lat) * sin(diff_lon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        distance = radius * c
        # print(distance,maxDistance)

        if distance <= maxDistance:
            businesslist.append(document['name'].encode(encoding='UTF-8').upper())

    fw = open(saveLocation2, 'w')
    fw.write("\n".join(businesslist))
    fw.close()

