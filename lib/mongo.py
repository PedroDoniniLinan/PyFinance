from pymongo import MongoClient
import datetime as dt

MONGO_USER = 'pedroLinan'
MONGO_PSWD = 'crusaderKing3'

def insert(docs, collection):
    connetionString = "mongodb+srv://" + MONGO_USER + ":" + MONGO_PSWD + "@cluster0.jmd9y.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    client = MongoClient(connetionString)
    db = client.myFirstDatabase
    col = db[collection]
    col.insert_many(docs)

def replaceInsert(docs, collection):
    connetionString = "mongodb+srv://" + MONGO_USER + ":" + MONGO_PSWD + "@cluster0.jmd9y.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    client = MongoClient(connetionString)
    db = client.myFirstDatabase
    col = db[collection]
    
    t = dt.datetime.now()
    
    print('Deleting...')
    col.delete_many({})
    
    print((dt.datetime.now()-t).total_seconds())
    t = dt.datetime.now()
    
    print('Inserting...')
    col.insert_many(docs)

    print((dt.datetime.now()-t).total_seconds())
    t = dt.datetime.now()