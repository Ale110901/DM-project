from pymongo import MongoClient
from bson import json_util
from concurrent.futures import ThreadPoolExecutor, as_completed


def task(t_id):
    port = 10004
    db = 'Energy'
    collection_name = 'data'

    client = MongoClient("mongodb://localhost:%d" % port)

    database = client[db]
    collection = database[collection_name]

    trial = collection.find().limit(10)#.explain()
    #print(json.dumps(trial, indent=4, default=json_util.default))

    #for documents in trial:
        #print(json.dumps(documents, indent=4, default=json_util.default))
    
    client.close()
    return f'{t_id} OK'


if __name__ == '__main__':

    with ThreadPoolExecutor(10) as exe:
        futures = [exe.submit(task, i) for i in range(10)]
        
        for future in as_completed(futures):
            result = future.result()
            print(result)

    