import time
import pymongo
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor

# --- MongoDB connection ---
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["first100k"]
collection = db["user_review"]

# --- Queries ---
def query_rating_5():
    list(collection.find({"rating": 5}))

def query_asin_equals_parent():
    list(collection.find({"$expr": {"$eq": ["$asin", "$parent_asin"]}}))

def query_verified_and_helpful():
    list(collection.find({"verified_purchase": True, "helpful_vote": {"$gt": 2}}))

def update_user_verified_false():
    collection.update_many(
        {"user_id": "AGBFYI2DDIKXC5Y4FARTYDTQBMFQ"},
        {"$set": {"verified_purchase": False}}
    )

def query_cute_word():
    list(collection.find({
        "$or": [
            {"title": {"$regex": "cute", "$options": "i"}},
            {"text": {"$regex": "cute", "$options": "i"}}
        ]
    }))

# All queries in a list
query_functions = [
    query_rating_5,
    query_asin_equals_parent,
    query_verified_and_helpful,
    update_user_verified_false,
    query_cute_word
]

# --- Benchmark ---
concurrent_counts = [2, 3, 4, 5]  # number of queries running in parallel
response_times = []

for count in concurrent_counts:
    print(f"\nRunning {count} concurrent queries...")
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=count) as executor:
        # Pick first 'count' queries from the list
        executor.map(lambda fn: fn(), query_functions[:count])
    duration = time.time() - start_time
    response_times.append(duration)
    print(f"Time taken: {duration:.4f} seconds")

# --- Plot ---
plt.figure(figsize=(8, 6))
plt.plot(concurrent_counts, response_times, marker="o", label="Response Time")
plt.xlabel("Number of Concurrent Queries")
plt.ylabel("Response Time (seconds)")
plt.title("Concurrent Queries vs Response Time")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig("Images/concurrent_queries_vs_time.png2", dpi=150)
plt.show()
