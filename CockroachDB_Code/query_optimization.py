import time
import psycopg2
import matplotlib.pyplot as plt
from pathlib import Path

# configuration
DBNAME = "defaultdb"
USER = "root"
HOST = "127.0.0.1"
PORT = 26257
TABLE = "user_review"
WORK_TABLE = "user_review_qopt"
TARGET_USER = "AGBFYI2DDIKXC5Y4FARTYDTQBMFQ"
SAMPLE_SIZES = list(range(10_000, 100_001, 10_000))  # 10k..100k
MATCH_FRACTION = 0.01

# connect
conn = psycopg2.connect(
    dbname=DBNAME,
    user=USER,
    host=HOST,
    port=PORT,
    sslmode='disable'
)
conn.autocommit = True

def drop_user_id_index():
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT index_name
            FROM [SHOW INDEXES FROM {WORK_TABLE}]
            WHERE index_name = '{WORK_TABLE}_user_id_idx';
        """)
        result = cur.fetchone()
        if result:
            cur.execute(f"DROP INDEX {WORK_TABLE}_user_id_idx;")


def ensure_user_id_index():
    with conn.cursor() as cur:
        cur.execute(f"CREATE INDEX IF NOT EXISTS {WORK_TABLE}_user_id_idx ON {WORK_TABLE} (user_id);")

def prepare_subset(n):
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {WORK_TABLE};")
        
        cur.execute(f"""
            CREATE TABLE {WORK_TABLE} (
                id INT PRIMARY KEY DEFAULT unique_rowid(),
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

        cur.execute(f"""
            INSERT INTO {WORK_TABLE} (rating, title, text, asin, parent_asin, user_id, timestamp, helpful_vote, verified_purchase)
            SELECT rating, title, text, asin, parent_asin, user_id, timestamp, helpful_vote, verified_purchase
            FROM {TABLE}
            LIMIT {n};
        """)

        cur.execute(f"UPDATE {WORK_TABLE} SET verified_purchase = TRUE;")

        k = max(1, int(n * MATCH_FRACTION))
        cur.execute(f"""
            UPDATE {WORK_TABLE}
            SET user_id = %s
            WHERE id IN (
                SELECT id FROM {WORK_TABLE}
                ORDER BY id
                LIMIT %s
            );
        """, (TARGET_USER, k))

def time_update(with_index: bool) -> float:
    drop_user_id_index()
    if with_index:
        ensure_user_id_index()
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {WORK_TABLE}
            SET verified_purchase = TRUE
            WHERE user_id = %s;
        """, (TARGET_USER,))
        start = time.perf_counter()
        cur.execute(f"""
            UPDATE {WORK_TABLE}
            SET verified_purchase = FALSE
            WHERE user_id = %s;
        """, (TARGET_USER,))
        return time.perf_counter() - start

with_index_times = []
without_index_times = []

total_with_index = 0
total_without_index = 0

for n in SAMPLE_SIZES:
    print(f"\n--- Preparing subset: {n} docs ---")
    prepare_subset(n)

    print("Timing WITHOUT index on user_id ...")
    dt_no = time_update(with_index=False)
    total_without_index += dt_no
    without_index_times.append(total_without_index)
    print(f"  Update time (no index): {dt_no:.6f}s, Cumulative: {total_without_index:.6f}s")

    print("Timing WITH index on user_id ...")
    dt_yes = time_update(with_index=True)
    total_with_index += dt_yes
    with_index_times.append(total_with_index)
    print(f"  Update time (with index): {dt_yes:.6f}s, Cumulative: {total_with_index:.6f}s")


# plot
plt.figure(figsize=(10, 6))
plt.plot(SAMPLE_SIZES, without_index_times, marker="o", label="Update w/o index")
plt.plot(SAMPLE_SIZES, with_index_times, marker="o", label="Update with index")
plt.xticks(SAMPLE_SIZES, [f"{s//1000}K" for s in SAMPLE_SIZES], rotation=45)
plt.xlabel("Number of Documents")
plt.ylabel("Time (seconds)")
plt.title("Query Optimization: Time VS Number of Rows (CockroachDB)")
plt.grid(True, axis="both")
plt.legend()
plt.tight_layout()
plt.savefig("CockroachDB_Images/query_optimization.png", dpi=150)
plt.show()
