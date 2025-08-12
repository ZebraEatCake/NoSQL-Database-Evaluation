import time
import psycopg2
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
import os

# setup
conn = psycopg2.connect(
    dbname="defaultdb",
    user="root",
    host="127.0.0.1",
    port=26257,
    sslmode='disable'
)
conn.autocommit = True

def get_cursor():
    return conn.cursor()

# queries
def query_rating_5():
    with get_cursor() as cur:
        cur.execute("SELECT * FROM user_review WHERE rating = 5")
        return cur.fetchall()

def query_asin_equals_parent():
    with get_cursor() as cur:
        cur.execute("SELECT * FROM user_review WHERE asin = parent_asin")
        return cur.fetchall()

def query_verified_and_helpful():
    with get_cursor() as cur:
        cur.execute("SELECT * FROM user_review WHERE verified_purchase = TRUE AND helpful_vote > 2")
        return cur.fetchall()

def update_user_verified_false():
    with get_cursor() as cur:
        cur.execute("""
            UPDATE user_review
            SET verified_purchase = FALSE
            WHERE user_id = 'AGBFYI2DDIKXC5Y4FARTYDTQBMFQ'
        """)

def query_cute_word():
    with get_cursor() as cur:
        cur.execute("""
            SELECT * FROM user_review
            WHERE LOWER(title) LIKE '%cute%'
            OR LOWER(text) LIKE '%cute%'
        """)
        return cur.fetchall()

# all quries in list
query_functions = [
    query_rating_5,
    query_asin_equals_parent,
    query_verified_and_helpful,
    update_user_verified_false,
    query_cute_word
]

# benchmark
concurrent_counts = [2, 3, 4, 5]
response_times = []

for count in concurrent_counts:
    print(f"\nRunning {count} concurrent queries...")
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=count) as executor:
        list(executor.map(lambda fn: fn(), query_functions[:count]))
    duration = time.time() - start_time
    response_times.append(duration)
    print(f"Time taken: {duration:.4f} seconds")

# plot
plt.figure(figsize=(8, 6))
plt.plot(concurrent_counts, response_times, marker="o", label="Response Time")
plt.xlabel("Number of Concurrent Queries")
plt.ylabel("Response Time (seconds)")
plt.title("Concurrent Queries: Time vs Number of Concurrent Queries(CockroachDB)")
plt.grid(True)
plt.legend()
plt.tight_layout()
try:
    plt.savefig("CockroachDB_Images/concurrent_queries_vs_time_cockroachdb.png", dpi=150)
except:
    pass
plt.show()