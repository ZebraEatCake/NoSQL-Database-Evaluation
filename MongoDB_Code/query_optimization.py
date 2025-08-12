import time
import psycopg2
import matplotlib.pyplot as plt
from pathlib import Path

# =========================
# Configuration
# =========================
DBNAME = "defaultdb"
USER = "root"
HOST = "127.0.0.1"
PORT = 26257
SSL_MODE = "disable"

SRC_TABLE = "user_review"
WORK_TABLE = "user_review_qopt"
TARGET_USER = "AGBFYI2DDIKXC5Y4FARTYDTQBMFQ"

SAMPLE_SIZES = list(range(10_000, 100_001, 10_000))  # 10k..100k
MATCH_FRACTION = 0.01                                 # ~1% of rows will match
OUT_DIR = Path("CockroachDB_Images")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# Connect
# =========================
conn = psycopg2.connect(
    dbname=DBNAME, user=USER, host=HOST, port=PORT, sslmode=SSL_MODE
)
conn.autocommit = True

# =========================
# Helpers
# =========================
def drop_user_id_index():
    # CockroachDB index name scoping uses table@index
    with conn.cursor() as cur:
        cur.execute(f"DROP INDEX IF EXISTS {WORK_TABLE}@user_id_idx;")

def ensure_user_id_index():
    with conn.cursor() as cur:
        cur.execute(f"CREATE INDEX IF NOT EXISTS user_id_idx ON {WORK_TABLE} (user_id);")
        # warm up the optimizer and caches so both paths are comparable
        cur.execute(f"SELECT COUNT(*) FROM {WORK_TABLE} WHERE user_id = %s;", (TARGET_USER,))

def prepare_subset(n: int):
    """Create a fresh working table with n rows and mark ~1% to match TARGET_USER."""
    with conn.cursor() as cur:
        # Recreate working table
        cur.execute(f"DROP TABLE IF EXISTS {WORK_TABLE};")
        cur.execute(f"""
            CREATE TABLE {WORK_TABLE} (
                id INT PRIMARY KEY DEFAULT unique_rowid(),
                rating INT,
                title STRING,
                text STRING,
                asin STRING,
                parent_asin STRING,
                user_id STRING,
                timestamp STRING,
                helpful_vote INT,
                verified_purchase BOOL
            );
        """)

        # Copy first n rows from source into work
        cur.execute(f"""
            INSERT INTO {WORK_TABLE} (rating, title, text, asin, parent_asin, user_id, timestamp, helpful_vote, verified_purchase)
            SELECT rating, title, text, asin, parent_asin, user_id, timestamp, helpful_vote, verified_purchase
            FROM {SRC_TABLE}
            LIMIT {n};
        """)

        # Ensure a known baseline value
        cur.execute(f"UPDATE {WORK_TABLE} SET verified_purchase = TRUE;")

        # Pick ~1% of rows and force them to match TARGET_USER
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
    """Time the update on TARGET_USER with/without an index on user_id."""
    drop_user_id_index()
    if with_index:
        ensure_user_id_index()
    else:
        # warm-up query for fairness
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {WORK_TABLE} WHERE user_id = %s;", (TARGET_USER,))

    # warm-up flip (not timed)
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {WORK_TABLE}
            SET verified_purchase = TRUE
            WHERE user_id = %s;
        """, (TARGET_USER,))

    # timed flip
    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {WORK_TABLE}
            SET verified_purchase = FALSE
            WHERE user_id = %s;
        """, (TARGET_USER,))
    return time.perf_counter() - start

# =========================
# Run the experiment
# =========================
with_index_times = []
without_index_times = []

for n in SAMPLE_SIZES:
    print(f"\n--- Preparing subset: {n} rows ---")
    prepare_subset(n)

    print("Timing WITHOUT index on user_id ...")
    dt_no = time_update(with_index=False)
    without_index_times.append(dt_no)
    print(f"  Update time (no index): {dt_no:.6f}s")

    print("Timing WITH index on user_id ...")
    dt_yes = time_update(with_index=True)
    with_index_times.append(dt_yes)
    print(f"  Update time (with index): {dt_yes:.6f}s")

# =========================
# Plot
# =========================
plt.figure(figsize=(10, 6))
plt.plot(SAMPLE_SIZES, without_index_times, marker="o", label="Update w/o index")
plt.plot(SAMPLE_SIZES, with_index_times, marker="o", label="Update with index")
plt.xticks(SAMPLE_SIZES, [f"{s//1000}K" for s in SAMPLE_SIZES], rotation=45)
plt.xlabel("Number of Rows")
plt.ylabel("Time (seconds)")
plt.title("Query Optimization: Time VS Number of Rows (CockroachDB)")
plt.grid(True, axis="both")
plt.legend()
plt.tight_layout()
plt.savefig(OUT_DIR / "query_optimization.png", dpi=150)
plt.show()
