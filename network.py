from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor
import time
import matplotlib.pyplot as plt


query_one_shard = {
        "$and": [
            {"Year": {"$gt": 2007}},
            {"Area": "Norway"}
        ]
    }

query_both_shard = {
        "$and": [
            {"Year": {"$lt": 2016}},
            {"Area": "Lao People's Democratic Republic (the)"}
        ]
    }

def control(client):
    try:
        client.admin.command('ping')
        return True
    except Exception:
        return False
    
def connect():
    port = 10006
    db_name = "Energy_db"
    collection_name = "yearly_release"

    client = MongoClient(f"mongodb://localhost:{port}")

    if not control(client):
        client.close()
        return
    else:
        return (client,db_name, collection_name)
        
def task(query, client, db_name, collection_name):
    database = client[db_name]
    collection = database[collection_name]

    try:
        results = collection.find(query)
        for document in results:
            pass
    except Exception as e:
        print(f"Errore task: {e}")
    
def measure_time(query, num_threads):
    connection = connect()
    if connection:
        client, db_name, collection_name = connection

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            init = time.perf_counter()

            futures = [executor.submit(task, query, client, db_name, collection_name) for _ in range(num_threads)]

            for future in futures:
                future.result()

            end = time.perf_counter()

        client.close()
        return end - init
    else:
        print("[+] Connection error.")
        return float('inf')

def main():
    num_threads_list = [500, 1000, 2000, 3000, 4000]
    num_trials = 2

    times_one_shard = []
    times_both_shards = []

    for num_threads in num_threads_list:
        total_time_one_shard = 0
        total_time_both_shards = 0

        for _ in range(num_trials):
            total_time_one_shard += measure_time(query_one_shard, num_threads)
            total_time_both_shards += measure_time(query_both_shard, num_threads)

        avg_time_one_shard = total_time_one_shard / num_trials
        avg_time_both_shards = total_time_both_shards / num_trials

        times_one_shard.append(avg_time_one_shard)
        times_both_shards.append(avg_time_both_shards)

    plt.plot(num_threads_list, times_one_shard, label="Query on One Shard")
    plt.plot(num_threads_list, times_both_shards, label="Query on Both Shards")
    plt.xlabel("Thread number")
    plt.ylabel("Execution time (s)")
    plt.legend()
    plt.grid(True)
    plt.show()

if __name__ == '__main__':
    main()
