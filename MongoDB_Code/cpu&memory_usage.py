import time
import pymongo
import matplotlib.pyplot as plt
import numpy as np
import psutil
import os

# ---------------- MongoDB setup ----------------
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["resource_benchmark_db"]
collection = db["benchmark_collection"]

# ---------------- Sample sizes ----------------
sample_sizes = list(range(10_000, 100_001, 10_000))

# ---------------- Results storage ----------------
cpu_usages = []
mem_usages = []

# ---------------- Sample schema ----------------
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

# ---------------- Process monitor ----------------
process = psutil.Process(os.getpid())

for size in sample_sizes:
    print(f"\n--- Measuring with {size} documents ---")
    collection.drop()

    # Simple batch insert for load
    docs = generate_docs(size)

    start_time = time.time()
    collection.insert_many(docs)
    collection.update_many({}, {"$set": {"helpful_vote": 10}})
    collection.delete_many({})
    elapsed = time.time() - start_time

    # Measure CPU and memory after operation
    # cpu_percent interval=0.1 will measure short usage spike
    cpu_percent = process.cpu_percent(interval=0.1)  
    mem_mb = process.memory_info().rss / (1024 * 1024)  # in MB

    cpu_usages.append(cpu_percent)
    mem_usages.append(mem_mb)

    print(f"CPU%: {cpu_percent:.2f}, Memory: {mem_mb:.2f} MB, Elapsed: {elapsed:.4f} s")

# ---------------- Cleanup ----------------
client.drop_database("resource_benchmark_db")

# ---------------- Plot CPU usage ----------------
plt.figure(figsize=(8, 5))
plt.plot(sample_sizes, cpu_usages, marker="o", label="CPU Usage (%)")
plt.xlabel("Number of Documents")
plt.ylabel("CPU Usage (%)")
plt.title("CPU Usage vs Number of Documents")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig("../Images/cpu_usage_vs_docs.png", dpi=150)
plt.show()

# ---------------- Plot Memory usage ----------------
plt.figure(figsize=(8, 5))
plt.plot(sample_sizes, mem_usages, marker="o", color="orange", label="Memory Usage (MB)")
plt.xlabel("Number of Documents")
plt.ylabel("Memory Usage (MB)")
plt.title("Memory Usage vs Number of Documents")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig("../MongoDB_Images/memory_usage_vs_docs.png", dpi=150)
plt.show()
