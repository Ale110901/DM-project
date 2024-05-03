import pymongo as py

client = py.MongoClient("mongodb://localhost:27017")
mydb = client["Energy"]

print(mydb)
