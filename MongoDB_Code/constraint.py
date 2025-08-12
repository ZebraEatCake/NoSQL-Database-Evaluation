import json
import time
from pathlib import Path

import pymongo
import matplotlib.pyplot as plt

# configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME   = "amazon"
COLL_NAME = "user_review_integrity_test"

SIZES = list(range(10_000, 100_001, 10_000))  # 10k..100k
IMAGES_DIR = Path("Images")
IMAGES_DIR.mkdir(exist_ok=True)

# connect
client = pymongo.MongoClient(MONGO_URI)
db  = client[DB_NAME]
col = db[COLL_NAME]

# helper
def reset_collection():
    """Drop collection (removes indexes & validators), recreate empty."""
    col.drop()

def set_validator(schema_dict):
    """Apply JSON Schema validator to the collection via collMod."""
    db.create_collection(COLL_NAME) if COLL_NAME not in db.list_collection_names() else None
    db.command({
        "collMod": COLL_NAME,
        "validator": {"$jsonSchema": schema_dict},
        "validationLevel": "strict",
        "validationAction": "error"
    })

def clear_validator():
    """Remove validator (no validation)."""
    db.create_collection(COLL_NAME) if COLL_NAME not in db.list_collection_names() else None
    db.command({"collMod": COLL_NAME, "validator": {}, "validationLevel": "off"})

def drop_non_id_indexes():
    try:
        col.drop_indexes()
    except pymongo.errors.OperationFailure:
        pass

def ensure_unique_user_id_index():
    col.create_index("user_id", unique=True)

def make_docs(n, unique_users=False):
    """Generate n sample docs. If unique_users=True, user_id=USER1..USERn; else fixed user_id."""
    docs = []
    for i in range(1, n + 1):
        user_id = f"USER{i}" if unique_users else "AFNT6ZJCYQN3WDIKUSWHJDXNND2Q"
        docs.append({
            "rating": 5,                          
            "title": "cute",
            "text": "very cute",
            "asin": "B09DQ5M2BB",
            "parent_asin": "B09DQ5M2BB",
            "user_id": user_id,
            "timestamp": "12:33:48 AM",           
            "helpful_vote": 3,
            "verified_purchase": True
        })
    return docs

def time_insert(n):
    """Insert N docs, return elapsed seconds."""
    docs = make_docs(n, unique_users=False)  
    t0 = time.perf_counter()
    col.insert_many(docs, ordered=False)
    return time.perf_counter() - t0

# json schema
check_rating_schema = {
    "bsonType": "object",
    "required": ["rating","title","text","asin","parent_asin","user_id","timestamp","helpful_vote","verified_purchase"],
    "properties": {
        "rating": {"bsonType": "int", "minimum": 1, "maximum": 5},
        "title": {"bsonType": "string"},
        "text": {"bsonType": "string"},
        "asin": {"bsonType": "string"},
        "parent_asin": {"bsonType": "string"},
        "user_id": {"bsonType": "string"},
        "timestamp": {"bsonType": "string"},
        "helpful_vote": {"bsonType": "int"},
        "verified_purchase": {"bsonType": "bool"}
    },
    "additionalProperties": True
}

notnull_schema = {
    "bsonType": "object",
    "required": ["rating","title","text","asin","parent_asin","user_id","timestamp","helpful_vote","verified_purchase"],
    "properties": {
        "rating": {"bsonType": "int"},
        "title": {"bsonType": "string"},
        "text": {"bsonType": "string"},
        "asin": {"bsonType": "string"},
        "parent_asin": {"bsonType": "string"},
        "user_id": {"bsonType": "string"},
        "timestamp": {"bsonType": "string"},
        "helpful_vote": {"bsonType": "int"},
        "verified_purchase": {"bsonType": "bool"}
    },
    "additionalProperties": True
}

Path("validator_check_rating.json").write_text(json.dumps(check_rating_schema, indent=2))
Path("validator_notnull.json").write_text(json.dumps(notnull_schema, indent=2))

# constraint
times_unique = []
times_check  = []
times_notnull = []

# 1) unique user id
for n in SIZES:
    print(f"[UNIQUE] n={n}")
    reset_collection()
    clear_validator()
    drop_non_id_indexes()
    docs = make_docs(n, unique_users=True)
    t0 = time.perf_counter()
    ensure_unique_user_id_index()
    col.insert_many(docs, ordered=False)
    elapsed = time.perf_counter() - t0
    times_unique.append(elapsed)

# 2) check rating
for n in SIZES:
    print(f"[CHECK rating 1..5] n={n}")
    reset_collection()
    drop_non_id_indexes()
    set_validator(check_rating_schema)
    docs = make_docs(n, unique_users=False)
    t0 = time.perf_counter()
    col.insert_many(docs, ordered=False)
    elapsed = time.perf_counter() - t0
    times_check.append(elapsed)

# 3) not null all fields
for n in SIZES:
    print(f"[NOT NULL all fields] n={n}")
    reset_collection()
    drop_non_id_indexes()
    set_validator(notnull_schema)
    docs = make_docs(n, unique_users=False)
    t0 = time.perf_counter()
    col.insert_many(docs, ordered=False)
    elapsed = time.perf_counter() - t0
    times_notnull.append(elapsed)

# plot
plt.figure(figsize=(10, 6))
plt.plot(SIZES, times_unique, marker="o", label="Unique(user_id)")
plt.plot(SIZES, times_check, marker="o", label="Check: rating 1–5")
plt.plot(SIZES, times_notnull, marker="o", label="Not Null (all fields)")
plt.xticks(SIZES, [f"{s//1000}K" for s in SIZES], rotation=45)
plt.xlabel("Number of Documents (inserted)")
plt.ylabel("Time (seconds)")
plt.title("Constraint: Time vs Data Size (Mongo DB)")
plt.grid(True, axis="both")
plt.legend()
plt.tight_layout()
plt.savefig("MongoDB_Images/constraint.png", dpi=150)
plt.show()
