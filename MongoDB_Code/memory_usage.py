import time
from pymongo import MongoClient
import matplotlib.pyplot as plt
import psutil
import os
from pathlib import Path

# configuration
MONGO_URI = "mongodb://127.0.0.1:27017"
DBNAME = "defaultdb"
COLLECTION = "user_review"       # Your collection name
SAMPLE_SIZES = list(range(10_000, 100_001, 10_000))  # 10k..100k

Path("Images").mkdir(exist_ok=True)

# connect
client = MongoClient(MONGO_URI)
db = client[DBNAME]
col = db[COLLECTION]

col.drop()

# sample doc
BASE_DOC = {
    "rating": 5,
    "title": "cute",
    "review_text": "very cute",
    "asin": "B09DQ5M2BB",
    "parent_asin": "B09DQ5M2BB",
    "user_id": "AFNT6ZJCYQN3WDIKUSWHJDXNND2Q",
    "timestamp": "12:33:48 AM",
    "helpful_vote": 3,
    "verified_purchase": True,
}

def generate_docs(n):
    return [BASE_DOC.copy() for _ in range(n)]

# process
process = psutil.Process(os.getpid())

mem_usages = []

for size in SAMPLE_SIZES:
    print(f"\n--- Measuring with {size} documents ---")

    # clear connection
    col.delete_many({})

    docs = generate_docs(size)

    start_time = time.time()

    # insert
    col.insert_many(docs, ordered=False)

    # update
    col.update_many({}, {"$set": {"helpful_vote": 10}})

    # delete
    col.delete_many({})

    elapsed = time.time() - start_time

    # Sample current process memory (RSS)
    mem_mb = process.memory_info().rss / (1024 * 1024)
    mem_usages.append(mem_mb)

    print(f"Memory: {mem_mb:.2f} MB, Elapsed: {elapsed:.4f} s")

# cleanup
col.drop()
client.close()

# plot
plt.figure(figsize=(8, 5))
plt.plot(SAMPLE_SIZES, mem_usages, marker="o", label="Memory Usage (MB)")
plt.xlabel("Number of Documents")
plt.ylabel("Memory Usage (MB)")
plt.title("Memory Usage vs Number of Documents (MongoDB)")
plt.grid(True)
plt.legend()
plt.tight_layout()
try:
    plt.savefig("MongoDB_Images/memory_usage.png", dpi=150)
except:
    pass
plt.show()
