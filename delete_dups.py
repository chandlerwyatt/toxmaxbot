from mongo import make_mongo_client
from sys import argv
import pymongo


client = make_mongo_client()
db = client.tweets
coll = db[f"{argv[1]}"]
#def del_dupes(coll=coll):
dups = coll.aggregate([
    {"$group" : { "_id": "$id", "count": { "$sum": 1 } } },
    {"$match": {"_id" :{ "$ne" : None } , "count" : {"$gt": 1} } }, 
    {"$project": {"id" : "$_id", "_id" : 0, "count": 1} }
])
dups = list(dups)
print(dups)

print(f"length of dups array: {len(dups)}")

if len(dups) == 0:
    print("No duplicates detected")
for dup in dups:
    print(f"deleting duplicate document with 'id': {dup['id']}")
    count = dup['count']
    for _ in range(count - 1):
        res = coll.delete_one({'id': dup['id']})
        print(res)
