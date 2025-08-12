import time
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
import os

# setup
conn = psycopg2.connect(
    dbname="defaultdb",
    user="root",
    host="127.0.0.1",
    port=26257,
    sslmode='disable'
)

if not os.path.exists("Images"):
    os.makedirs("Images")
conn.autocommit = True
cursor = conn.cursor()

# schema
cursor.execute("""
DROP TABLE IF EXISTS benchmark_table;
""")

cursor.execute("""
CREATE TABLE benchmark_table (
    id SERIAL PRIMARY KEY,
    rating INT,
    title TEXT,
    text TEXT,
    asin TEXT,
    parent_asin TEXT,
    user_id TEXT,
    timestamp TEXT,
    helpful_vote INT,
    verified_purchase BOOLEAN
);
""")

# sample size
sample_sizes = list(range(10_000, 100_001, 10_000))  # 10k to 100k

# result array
batch_insert_times = []
batch_update_times = []
batch_delete_times = []

single_insert_times = []
single_update_times = []
single_delete_times = []

# sample data
BASE_DATA = (
    5,
    "cute",
    "very cute",
    "B09DQ5M2BB",
    "B09DQ5M2BB",
    "AFNT6ZJCYQN3WDIKUSWHJDXNND2Q",
    "12:33:48 AM",
    3,
    True
)

def generate_data(n):
    return [BASE_DATA for _ in range(n)]

for size in sample_sizes:
    print(f"\n--- Testing with {size} rows ---")

    # batch operation
    cursor.execute("TRUNCATE TABLE benchmark_table;")

    # insert
    data = generate_data(size)
    insert_query = """
        INSERT INTO benchmark_table
        (rating, title, text, asin, parent_asin, user_id, timestamp, helpful_vote, verified_purchase)
        VALUES %s
    """
    from psycopg2.extras import execute_values
    start_time = time.time()
    execute_values(cursor, insert_query, data)
    insert_duration = time.time() - start_time
    batch_insert_times.append(insert_duration)
    print(f"[Batch] Insert time: {insert_duration:.4f} s")

    # update
    start_time = time.time()
    cursor.execute("UPDATE benchmark_table SET helpful_vote = 10;")
    update_duration = time.time() - start_time
    batch_update_times.append(update_duration)
    print(f"[Batch] Update time: {update_duration:.4f} s")

    # delete
    start_time = time.time()
    cursor.execute("DELETE FROM benchmark_table;")
    delete_duration = time.time() - start_time
    batch_delete_times.append(delete_duration)
    print(f"[Batch] Delete time: {delete_duration:.4f} s")

    # single operation
    cursor.execute("TRUNCATE TABLE benchmark_table;")

    # INSERT (single)
    start_time = time.time()
    insert_single_query = """
        INSERT INTO benchmark_table
        (rating, title, text, asin, parent_asin, user_id, timestamp, helpful_vote, verified_purchase)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for _ in range(size):
        cursor.execute(insert_single_query, BASE_DATA)
    insert_duration = time.time() - start_time
    single_insert_times.append(insert_duration)
    print(f"[Single] Insert time: {insert_duration:.4f} s")

    # update
    cursor.execute("SELECT id FROM benchmark_table;")
    ids = [row[0] for row in cursor.fetchall()]
    start_time = time.time()
    update_single_query = "UPDATE benchmark_table SET helpful_vote = 10 WHERE id = %s;"
    for id_ in ids:
        cursor.execute(update_single_query, (id_,))
    update_duration = time.time() - start_time
    single_update_times.append(update_duration)
    print(f"[Single] Update time: {update_duration:.4f} s")

    # delete
    cursor.execute("SELECT id FROM benchmark_table;")
    ids = [row[0] for row in cursor.fetchall()]
    start_time = time.time()
    delete_single_query = "DELETE FROM benchmark_table WHERE id = %s;"
    for id_ in ids:
        cursor.execute(delete_single_query, (id_,))
    delete_duration = time.time() - start_time
    single_delete_times.append(delete_duration)
    print(f"[Single] Delete time: {delete_duration:.4f} s")

# close conneciton
cursor.close()
conn.close()

# plot
labels = [f"{s//1000}K" for s in sample_sizes]
x = np.arange(len(sample_sizes))
width = 0.25

# batch 
plt.figure(figsize=(10, 6))
plt.plot(sample_sizes, batch_insert_times, marker="o", label="Insert")
plt.plot(sample_sizes, batch_update_times, marker="o", label="Update")
plt.plot(sample_sizes, batch_delete_times, marker="o", label="Delete")
plt.xticks(sample_sizes, [f"{s//1000}K" for s in sample_sizes], rotation=45)
plt.xlabel("Number of Rows")
plt.ylabel("Time (seconds)")
plt.title("Batch Operations: Time vs Number of Rows (CockroachDB)")
plt.legend()
plt.grid(True, axis='y')
plt.tight_layout()
plt.savefig("CockroachDB_Images/batch_operations_cockroach.png", dpi=150)
plt.show()

# single
plt.figure(figsize=(10, 6))
plt.plot(sample_sizes, single_insert_times, marker="o", label="Insert")
plt.plot(sample_sizes, single_update_times, marker="o", label="Update")
plt.plot(sample_sizes, single_delete_times, marker="o", label="Delete")
plt.xticks(sample_sizes, [f"{s//1000}K" for s in sample_sizes], rotation=45)
plt.xlabel("Number of Rows")
plt.ylabel("Time (seconds)")
plt.title("Single Operations: Time vs Number of Rows (CockroachDB)")
plt.legend()
plt.grid(True, axis='y')
plt.tight_layout()
try:
    plt.savefig("CockroachDB_Images/single_operations_cockroach.png", dpi=150)
except: 
    pass
plt.show()
