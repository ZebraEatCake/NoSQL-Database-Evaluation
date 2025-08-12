import time
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
import psutil
import os
from pathlib import Path

# configuration
DBNAME = "defaultdb"
USER = "root"
HOST = "127.0.0.1"
PORT = 26257
TABLE = "user_review"
SAMPLE_SIZES = list(range(10_000, 100_001, 10_000))  # 10k..100k

# connect to db
conn = psycopg2.connect(
    dbname=DBNAME,
    user=USER,
    host=HOST,
    port=PORT,
    sslmode='disable'  # Change to 'require' if you have TLS enabled
)
conn.autocommit = True
cur = conn.cursor()

# create table
cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
cur.execute(f"""
CREATE TABLE {TABLE} (
    id SERIAL PRIMARY KEY,
    rating INT,
    title STRING,
    review_text STRING,
    asin STRING,
    parent_asin STRING,
    user_id STRING,
    timestamp STRING,
    helpful_vote INT,
    verified_purchase BOOL
)
""")

# sample data
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
    docs = []
    for _ in range(n):
        docs.append((
            BASE_DOC["rating"],
            BASE_DOC["title"],
            BASE_DOC["text"],
            BASE_DOC["asin"],
            BASE_DOC["parent_asin"],
            BASE_DOC["user_id"],
            BASE_DOC["timestamp"],
            BASE_DOC["helpful_vote"],
            BASE_DOC["verified_purchase"],
        ))
    return docs

# process
process = psutil.Process(os.getpid())

mem_usages = []

for size in SAMPLE_SIZES:
    print(f"\n--- Measuring with {size} documents ---")

    # clear table
    cur.execute(f"TRUNCATE TABLE {TABLE}")

    docs = generate_docs(size)

    start_time = time.time()

    insert_query = f"""
    INSERT INTO {TABLE} 
    (rating, title, review_text, asin, parent_asin, user_id, timestamp, helpful_vote, verified_purchase) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cur.executemany(insert_query, docs)

    # update
    cur.execute(f"UPDATE {TABLE} SET helpful_vote = 10")

    # delete
    cur.execute(f"DELETE FROM {TABLE}")

    elapsed = time.time() - start_time

    mem_mb = process.memory_info().rss / (1024 * 1024)
    mem_usages.append(mem_mb)

    print(f"Memory: {mem_mb:.2f} MB, Elapsed: {elapsed:.4f} s")

# cleanup
cur.execute(f"DROP TABLE {TABLE}")
cur.close()
conn.close()

# plot
plt.figure(figsize=(8, 5))
plt.plot(SAMPLE_SIZES, mem_usages, marker="o", color="orange", label="Memory Usage (MB)")
plt.xlabel("Number of Rows")
plt.ylabel("Memory Usage (MB)")
plt.title("Memory Usage vs Number of Rows (CockroachDB)")
plt.grid(True)
plt.legend()
plt.tight_layout()
try:
    plt.savefig("CockroachDB_Images/memory_usage.png", dpi=150)
except:
    pass
plt.show()
