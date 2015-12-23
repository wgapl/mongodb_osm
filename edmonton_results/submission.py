#! /usr/bin/env python

"""
File : submission.py 

Author : Thomas Odell Wood

Date : 10 Jul 2014

Description: A. Audit the street types
             B. Reshape data elements and save to mongoDB
             C. Answer questions using mongoDB aggregation framework

Parameters : None

"""
import xml.etree.ElementTree as ET
import pprint
import re
import codecs
import json
from collections import defaultdict
import subprocess
import argparse

parser = argparse.ArgumentParser(description="Show how argparse works.")
parser.add_argument("--input", metavar="-i", help="the input osm")
parser.add_argument("--database", metavar="-d", help="the db name")
parser.add_argument("--collection", metavar="-c", help="the collection name")
args = parser.parse_args()

if args.input == None:
    input_osm = "example.osm"
else:
    input_osm = args.input

if args.database == None:
    osm_db = "osm"
else:
    osm_db = args.database

if args.collection == None:
    osm_coll = "edmonton"
else:
    osm_coll = args.collection

print input_osm, osm_db, osm_coll

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)



CREATED = [ "version", "changeset", "timestamp", "user", "uid"]

mapping = { "St": "Street",
            "St.": "Street",
            "ST": "Street",
            "ST.": "Street",
            "STREET": "Street",
            "street": "Street",
            "Ave.": "Avenue",
            "Ave": "Avenue",
            "AVE": "Avenue",
            "AVE.": "Avenue",
            "AVENUE": "Avenue",
            "avenue": "Avenue",
            "Rd.": "Road",
            "Rd": "Road",
            "RD": "Road",
            "RD.": "Road",
            "ROAD": "Road",
            "road": "Road",
            "Blvd": "Boulevard",
            "Blvd.": "Boulevard",
            "blvd" : "Boulevard",
            "blvd.": "Boulevard",
            "BLVD": "Boulevard",
            "BLVD.": "Boulevard",
            "BOULEVARD":"Boulevard",
            "Ct.": "Court",
            "Ct.": "Court",
            "CT": "Court",
            "CT.": "Court",
            "COURT": "Court",
            "Ln.": "Lane",
            "LN.": "Lane",
            "Ln": "Lane",
            "LN": "Lane",
            "LANE": "Lane",
            "Dr.": "Drive",
            "DR.": "Drive",
            "Dr": "Drive",
            "DR": "Drive",
            "DRIVE":"Drive",
            "PKWY": "Parkway",
            "Pkwy": "Parkway",
            "PKWY.": "Parkway",
            "Pkwy.": "Parkway",
            "pkwy": "Parkway",
            "pkwy.": "Parkway",
            "PARKWAY": "Parkway",
            "SQ.": "Square",
            "Sq.": "Square",
            "sq.": "Square",
            "SQ": "Square",
            "Sq": "Square",
            "sq":"Square",
            "SQUARE": "Square",
            "TRL.": "Trail",
            "Trl.": "Trail",
            "trl.": "Trail",
            "TRL": "Trail",
            "Trl": "Trail",
            "trl": "Trail",
            "TRAIL": "Trail",
            "PL.": "Place",
            "Pl.": "Place",
            "pl.": "Place",
            "PL": "Place",
            "Pl": "Place",
            "pl": "Place",
            "WY": "Way",
            "Wy": "Way",
            "WY.": "Way",
            "Wy.": "Way",
            "WAY": "Way"
            }

expected = ["Street", 
            "Avenue", 
            "Boulevard", 
            "Drive", 
            "Court", 
            "Place", 
            "Square", 
            "Lane", 
            "Road", 
            "Trail", 
            "Parkway", 
            "Commons",
            "Way"
            ]

directional = [ "North",
                "South",
                "East",
                "West",
                "N",
                "S",
                "E",
                "W",
                "Northeast",
                "Northwest",
                "Southeast",
                "Southwest",
                "northeast",
                "northwest",
                "southeast",
                "southwest",
                "NE",
                "NW",
                "SE",
                "SW" ]

directional_mapping = { "N": "North",
                        "S": "South",
                        "E": "East",
                        "W": "West",
                        "NE": "Northeast",
                        "NW": "Northwest",
                        "SE": "Southeast",
                        "SW": "Southwest",
                        "northeast": "Northeast",
                        "northwest": "Northwest",
                        "southeast": "Southeast",
                        "southwest": "Southwest",
                        "north": "North",
                        "south": "South",
                        "east": "East",
                        "west": "West"
                        }

# Reshape XML element to prepare to put in mongoDB.
# Where the bulk of the work is done.
def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way" :
        kys = element.keys()
        node["type"] = element.tag
        # special instructions for how to handle the two types
        if element.tag == "node":
            node["pos"] = [ float(element.attrib["lat"]), float(element.attrib["lon"]) ]
        
        elif element.tag == "way":
            nd_refs = [ x.attrib["ref"] for x in element.iter("nd") ]
            node["node_refs"] = nd_refs
        # If any of the keys of element.attrib are in CREATED,
        # then initialize node["created"] to {}
        if any([x in CREATED for x in kys]):
            node["created"] = {}
        
        # Make a list of tags to iterate over
        tag_keys = [ x for x in element.iter("tag") ]
        
        # This step is agnostic to the type.
        if tag_keys != []:
            # Initialize "address" dictionary if there are any tags
            # that start with addr
            if any([ xx.attrib["k"][:4] == "addr" for xx in tag_keys ]):
                   node["address"] = {}
            for x in tag_keys:
                y, z = x.attrib["k"], x.attrib["v"]
                if y[:4] == "addr":
                    yy = y.split(":")
                    if len(yy) <= 2 and (re.match(problemchars,yy[1]) == None):
                        if yy[1] != "street":
                            node["address"][yy[1]] = z # no need to manicure
                        else:
                            node["address"][yy[1]] = update_name(z, mapping)
                # Had problems with {"type":"water"}
                elif (re.match(lower_colon, y) == None) and (y != "type") and (re.match(problemchars,y) == None):
                    node[y] = z
        # Now populate the "created" dictionary of the element
        for x in kys:
            if x in CREATED:
                node["created"][x] = element.attrib[x]
            # No need to  populate lon and lat => part of pos
            elif (x != "lon") and (x != "lat") and ( re.match(problemchars, x) == None):
                node[x] = element.attrib[x]
        # Having problems with p.o.box being a key!
        if any([ '.' in x for x in node.keys()]):
            print "The periods are getting through the steps above."
            return None
        return node
    else:
        return None

# Adapted from code given in Lesson 6.
# Writes straight to the database instead of a JSON file.
def process_map(file_in, db):
    count = 0
    for _, element in ET.iterparse(file_in):
        el = shape_element(element)
        count += 1
        # Give a sense of progress over time.
        # Kept crashing. Seemed like a good idea.
        if ((count % 100000) == 0):
            print count
        if el:
            db[osm_coll].insert(el)

def update_name(name, mapping):
    m = re.search(street_type_re, name)
    if m:
        stype = m.group()
        if stype not in directional:
            # collect the name of the street
            front_matter = name.split(stype)[0]
            if stype in mapping.keys():
                name = front_matter+mapping[stype]
        else:
            # split the street name up if the last name is in the directional
            direction = stype
            x = name.split(" ")
            st_name = x[:-2]
            stype = x[-2]
            # Change the style where we can
            if stype in mapping.keys():
                if direction in directional_mapping.keys():
                    name = " ".join(st_name)+" "+mapping[stype]+" "+directional_mapping[direction]
                else:
                    name = " ".join(st_name)+" "+mapping[stype]+" "+direction
    return name


def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

# Called in the beginning before coding up the audit of the street
# names. Useful function to have when auditing.
def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])

    return street_types

# Just give me the db already.
def get_db(database):
    from pymongo import MongoClient
    client = MongoClient()
    db = client[database]
    return db

# Pipeline to find all possible amenities and their number of
# occurences. 
def amenity_pipeline():
    pipeline = [{"$match":{"amenity":{"$exists":1}}}, 
                {"$group": {"_id": "$amenity", 
                            "count":{"$sum":1}}}, 
                {"$sort": {"count":-1}}]

    return pipeline

# Use uid to find the number of unique users who have entered
# information in to the OSM dataset.
def uid_pipeline():
    pipeline = [ {"$match": {"created.uid":{"$exists":1}}}, 
                 {"$group": {"_id": "$created.uid",
                             "uid": {"$addToSet" :"$created.uid"}}},
                 {"$group": {"_id": "Number of Unique Users",
                             "count":{"$sum":1}}}
    ]

    return pipeline

# Simple aggregate to count the types of the nodes and ways. This was
# useful in making sure only "nodes" and "ways" were assigned to the
# "type" key.
def nodes_ways_pipeline():
    pipeline = [{"$group": {"_id": "$type",
                            "count": {"$sum":1}}}
                ]
    return pipeline

# Hack to determine the size of the file imported.
def size_of_osm(OSMFILE):
    import subprocess, os
    scale_map = { "G": "Gigabytes",
                  "M": "Megabytes" }
    du_string = "du -sh "+OSMFILE+" > __tmp__"
    p = subprocess.Popen(du_string, shell=True)
    p.communicate()
    fh = open("__tmp__","r")
    fsize_info = fh.read().split("\t")[0]
    fh.close()
    os.remove("__tmp__")
    file_size, scale = float(fsize_info[:-1]), fsize_info[-1]
    return {"size": file_size, 
            "scale": scale_map[scale] }

# Show me the audit.
def audit_data(OSMFILE):
    street_types = audit(OSMFILE)
    pprint.pprint(street_types)

# Save answers for Edmonton into JSON files using this helper
# function.
def export_json(obj, fname):
    import json
    fh = open(fname,'w')
    fh.write( json.dumps(obj) )
    fh.close()

# Adds the data contained to mongoDB with the process_map function.
def gather_data(OSMFILE):
    db = get_db("osm")
    process_map(OSMFILE, db)

# Use aggregation pipelines defined above to query the data for
# questions from the rubric.
def collect_answers(OSMFILE):
    db = get_db("osm")

    size_result = size_of_osm(OSMFILE)
    pprint.pprint(size_result)

    g = OSMFILE.split(".")[0]+"_"

    export_json(size_result, g+"size_result.json")

    uid_result = db[osm_coll].aggregate(uid_pipeline())
    pprint.pprint(uid_result)
    export_json(uid_result["result"], g+"uid_result.json")
    
    nodes_ways_result = db[osm_coll].aggregate(nodes_ways_pipeline())
    pprint.pprint(nodes_ways_result)
    export_json(nodes_ways_result["result"], g+"nodes_ways_result.json")

    amenity_result = db[osm_coll].aggregate(amenity_pipeline())
    pprint.pprint(amenity_result)
    export_json(amenity_result["result"], g+"amenity_result.json")

# Gather then query.
def two_step(input_osm):
    gather_data(input_osm)
    collect_answers(input_osm)


if __name__ == "__main__":
    two_step(input_osm)
    pass
