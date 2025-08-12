import time
import pymongo
import matplotlib.pyplot as plt
import numpy as np

# setup connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["operation_benchmark_db"]
collection = db["benchmark_collection"]

# sample sizes
sample_sizes = list(range(10_000, 100_001, 10_000))  # 10k to 100k

# result array
# batch
batch_insert_times = []
batch_update_times = []
batch_delete_times = []

# single
single_insert_times = []
single_update_times = []
single_delete_times = []

#sample schema
# reuse content
BASE_DOC = {
    "rating": 5,
    "title": "cute",
    "text": "very cute",
    "asin": "B09DQ5M2BB",
    "parent_asin": "B09DQ5M2BB",
    "user_id": "AFNT6ZJCYQN3WDIKUSWHJDXNND2Q",
    "timestamp": "12:33:48 AM",
    "helpful_vote": 3,
    "verified_purchase": True,
}

def generate_docs(n):
    return [BASE_DOC.copy() for _ in range(n)]

for size in sample_sizes:
    print(f"\n--- Testing with {size} documents ---")

    # batch operation
    collection.drop()

    # insert
    docs = generate_docs(size)
    start_time = time.time()
    collection.insert_many(docs)
    insert_duration = time.time() - start_time
    batch_insert_times.append(insert_duration)
    print(f"[Batch] Insert time: {insert_duration:.4f} s")

    # update
    start_time = time.time()
    collection.update_many({}, {"$set": {"helpful_vote": 10}})
    update_duration = time.time() - start_time
    batch_update_times.append(update_duration)
    print(f"[Batch] Update time: {update_duration:.4f} s")

    # delete
    start_time = time.time()
    collection.delete_many({})
    delete_duration = time.time() - start_time
    batch_delete_times.append(delete_duration)
    print(f"[Batch] Delete time: {delete_duration:.4f} s")

    # =single operation
    collection.drop()

    # insert
    start_time = time.time()
    for _ in range(size):
        collection.insert_one(BASE_DOC.copy())
    insert_duration = time.time() - start_time
    single_insert_times.append(insert_duration)
    print(f"[Single] Insert time: {insert_duration:.4f} s")

    # update
    ids_cursor = collection.find({}, {"_id": 1})
    start_time = time.time()
    for d in ids_cursor:
        collection.update_one({"_id": d["_id"]}, {"$set": {"helpful_vote": 10}})
    update_duration = time.time() - start_time
    single_update_times.append(update_duration)
    print(f"[Single] Update time: {update_duration:.4f} s")

    # delete
    ids_cursor = collection.find({}, {"_id": 1})
    start_time = time.time()
    for d in ids_cursor:
        collection.delete_one({"_id": d["_id"]})
    delete_duration = time.time() - start_time
    single_delete_times.append(delete_duration)
    print(f"[Single] Delete time: {delete_duration:.4f} s")

# clean up
client.drop_database("operation_benchmark_db")

# plot
labels = [f"{s//1000}K" for s in sample_sizes]
x = np.arange(len(sample_sizes))
width = 0.25

# batch operation plot
plt.figure(figsize=(10, 6))
plt.plot(sample_sizes, batch_insert_times, marker="o", label="Insert")
plt.plot(sample_sizes, batch_update_times, marker="o", label="Update")
plt.plot(sample_sizes, batch_delete_times, marker="o", label="Delete")
plt.xticks(sample_sizes, [f"{s//1000}K" for s in sample_sizes], rotation=45)
plt.xlabel("Number of Documents")
plt.ylabel("Time (seconds)")
plt.title("Batch Operations: Time vs Number of Documents (MongoDB)")
plt.legend()
plt.grid(True, axis='y')
plt.tight_layout()
plt.savefig("MongoDB_Images/batch_operations.png", dpi=150)
plt.show()

# single operation plot
plt.figure(figsize=(10, 6))
plt.plot(sample_sizes, single_insert_times, marker="o", label="Insert")
plt.plot(sample_sizes, single_update_times, marker="o", label="Update")
plt.plot(sample_sizes, single_delete_times, marker="o", label="Delete")
plt.xticks(sample_sizes, [f"{s//1000}K" for s in sample_sizes], rotation=45)
plt.xlabel("Number of Documents")
plt.ylabel("Time (seconds)")
plt.title("Single Operations: Time vs Number of Documents (MongoDB)")
plt.legend()
plt.grid(True, axis='y')
plt.tight_layout()
plt.savefig("MongoDB_Images/single_operations.png", dpi=150)
plt.show()