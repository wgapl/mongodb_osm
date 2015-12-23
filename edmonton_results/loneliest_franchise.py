from pymongo import MongoClient
import re
import pprint
import argparse

parser = argparse.ArgumentParser(description="Find loneliest franchise.")
parser.add_argument("--franchise", metavar="-i", help="the input osm")
parser.add_argument("--database", metavar="-d", help="the db name")
parser.add_argument("--collection", metavar="-c", help="the collection name")
args = parser.parse_args()

franch = args.franchise
database = args.database
collection = args.collection
if franch == None:
   franch = "starbuck"
if database == None:
    database = "osm"
if collection == None:
    collection = "edmonton"
 
N = 1000000
M = 10000.

def get_db(db_name):
    client = MongoClient()
    db = client[db_name]
    return db

def find_franchises(franch, db_name, coll_name):
    franch_re = re.compile(r'^'+franch, re.IGNORECASE)
    query = {"name":franch_re}
    db = get_db(db_name)
    all_locs = [ x for x in db[coll_name].find(query) ]
    # Only nodes have [long, lat] geospatial info
    locs = []
    ids = {}
    geospat = []
    for x in all_locs:
        if x["type"] == "node":
            locs.append(x)
            ids[x["id"]] = x["pos"]
    return [ locs, ids ]


def dist(pos1, pos2):
    from math import acos, sin, cos, radians
    long1, lat1 = [radians(x) for x in pos1]
    long2, lat2 = [radians(x) for x in pos2]

    d = acos( sin(lat1)*sin(lat2)+cos(lat1)*cos(lat2)*cos(long1-long2))*6371.
    return d

def lonely_franchise():
    locs, ids = find_franchises(franch, database, collection)
    pairwise = {}
    for x in ids.keys():
        pairwise[x] = {}
        pos1 = ids[x]
        for xx in ids.keys():
            pos2 = ids[xx]
            if x != xx:
                pairwise[x][xx] = dist(pos1,pos2)
            else:
                pairwise[x][xx] = M
    loneliest = {}
    for x in pairwise.keys():
        loneliest[x] = min(pairwise[x].values())

    maxId, maxDist = None, 0.
    for x in loneliest.keys():
        if loneliest[x] > maxDist:
            maxId, maxDist = x, loneliest[x]
    print maxId, maxDist
    solitary = None
    for x in locs:
        if x["id"] == maxId:
            solitary = x

    pprint.pprint(solitary)

if __name__ == "__main__":
    lonely_franchise()

