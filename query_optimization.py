import time
import pymongo
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# ---------- Config ----------
MONGO_URI = "mongodb://localhost:27017/"
SRC_DB, SRC_COL = "first100k", "user_review"              # source collection (your 100k dataset)
WORK_DB, WORK_COL = "first100k", "user_review_qopt"       # temp working collection
TARGET_USER = "AGBFYI2DDIKXC5Y4FARTYDTQBMFQ"           # the user_id to update
SAMPLE_SIZES = list(range(10_000, 100_001, 10_000))    # 10k..100k
MATCH_FRACTION = 0.01                                   # ~1% of docs will match the target user
Path("Images").mkdir(exist_ok=True)

# ---------- Connect ----------
client = pymongo.MongoClient(MONGO_URI)
src = client[SRC_DB][SRC_COL]
work = client[WORK_DB][WORK_COL]

def drop_user_id_index():
    try:
        # Drop all non-_id indexes; safe even if none exist
        work.drop_indexes()
    except pymongo.errors.OperationFailure:
        pass

def ensure_user_id_index():
    work.create_index("user_id")

def prepare_subset(n):
    """Clone first n docs from source into work (no _id collisions), and prep fields."""
    work.drop()
    # Pull first n from source, strip _id so new ones are generated
    cursor = src.find({}, {"_id": 0}).limit(n)
    batch = list(cursor)
    if not batch:
        raise RuntimeError(f"No data found in {SRC_DB}.{SRC_COL}.")
    work.insert_many(batch, ordered=False)

    # Make sure verified_purchase is True everywhere (so updates do real writes)
    work.update_many({}, {"$set": {"verified_purchase": True}})

    # Force ~1% of docs to match TARGET_USER so the update has something to touch
    k = max(1, int(n * MATCH_FRACTION))
    ids = [d["_id"] for d in work.find({}, {"_id": 1}).limit(k)]
    if ids:
        work.update_many({"_id": {"$in": ids}}, {"$set": {"user_id": TARGET_USER}})

def time_update(with_index: bool) -> float:
    """Time update_many on TARGET_USER with/without an index on user_id."""
    drop_user_id_index()
    if with_index:
        ensure_user_id_index()

    # Reset targeted docs to True so the update actually flips to False
    work.update_many({"user_id": TARGET_USER}, {"$set": {"verified_purchase": True}})
    t0 = time.perf_counter()
    work.update_many({"user_id": TARGET_USER}, {"$set": {"verified_purchase": False}})
    dt = time.perf_counter() - t0
    return dt

with_index_times = []
without_index_times = []

for n in SAMPLE_SIZES:
    print(f"\n--- Preparing subset: {n} docs ---")
    prepare_subset(n)

    print("Timing WITHOUT index on user_id ...")
    t_no = time.perf_counter()
    dt_no = time_update(with_index=False)
    without_index_times.append(dt_no)
    print(f"  Update time (no index): {dt_no:.6f}s")

    print("Timing WITH index on user_id ...")
    dt_yes = time_update(with_index=True)
    with_index_times.append(dt_yes)
    print(f"  Update time (with index): {dt_yes:.6f}s")

# ---------- Plot ----------
plt.figure(figsize=(10, 6))
plt.plot(SAMPLE_SIZES, without_index_times, marker="o", label="Update w/o index")
plt.plot(SAMPLE_SIZES, with_index_times, marker="o", label="Update with index")
plt.xticks(SAMPLE_SIZES, [f"{s//1000}K" for s in SAMPLE_SIZES], rotation=45)
plt.xlabel("Number of Documents")
plt.ylabel("Time (seconds)")
plt.title("Query Optimization: Update verified_purchase by user_id (10K–100K)")
plt.grid(True, axis="both")
plt.legend()
plt.tight_layout()
plt.savefig("Images/update_userid_index_vs_noindex.png", dpi=150)
plt.show()
