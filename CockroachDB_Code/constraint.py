# cockroach_integrity_benchmark.py
import time
from pathlib import Path
import random

import psycopg2
from psycopg2.extras import execute_values
import matplotlib.pyplot as plt

# configuration
CRDB_DSN = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
SCHEMA   = "public"
TABLE    = "user_review_integrity_test"

SIZES = list(range(10_000, 100_001, 10_000))  # 10k..100k

# connect
conn = psycopg2.connect(CRDB_DSN)
conn.set_session(autocommit=True)

# helper
def drop_table():
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{TABLE}"')

def create_table_base(not_null=False):
    """Create table with optional NOT NULL on all columns (scenario 3)."""
    nn = " NOT NULL" if not_null else ""
    with conn.cursor() as cur:
        cur.execute(f'''
            CREATE TABLE "{SCHEMA}"."{TABLE}" (
                rating            INT{nn},
                title             STRING{nn},
                text              STRING{nn},
                asin              STRING{nn},
                parent_asin       STRING{nn},
                user_id           STRING{nn},
                "timestamp"       STRING{nn},
                helpful_vote      INT{nn},
                verified_purchase BOOL{nn}
            )
        ''')

def add_unique_user_id():
    with conn.cursor() as cur:
        cur.execute(f'''
            ALTER TABLE "{SCHEMA}"."{TABLE}"
            ADD CONSTRAINT user_review_user_id_key UNIQUE (user_id)
        ''')

def add_check_rating():
    with conn.cursor() as cur:
        cur.execute(f'''
            ALTER TABLE "{SCHEMA}"."{TABLE}"
            ADD CONSTRAINT rating_between_1_5 CHECK (rating BETWEEN 1 AND 5)
        ''')

def make_rows(n, unique_users=False):
    """Return list[tuple] matching table order."""
    rows = []
    for i in range(1, n + 1):
        user_id = f"USER{i}" if unique_users else "AFNT6ZJCYQN3WDIKUSWHJDXNND2Q"
        rows.append((
            5,                       # rating
            "cute",                  # title
            "very cute",             # text
            "B09DQ5M2BB",            # asin
            "B09DQ5M2BB",            # parent_asin
            user_id,                 # user_id
            "12:33:48 AM",           # timestamp (string for simplicity)
            3,                       # helpful_vote
            True                     # verified_purchase
        ))
    return rows

def bulk_insert(rows, page_size=1000):
    """Efficient multi-row insert using execute_values."""
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        execute_values(
            cur,
            f'INSERT INTO "{SCHEMA}"."{TABLE}" '
            '(rating, title, text, asin, parent_asin, user_id, "timestamp", helpful_vote, verified_purchase) '
            'VALUES %s',
            rows,
            page_size=page_size
        )
        return time.perf_counter() - t0

# constraints
times_unique = []
times_check  = []
times_notnull = []

# 1) unique user_id
for n in SIZES:
    print(f"[UNIQUE] n={n}")
    drop_table()
    create_table_base(not_null=False)
    add_unique_user_id()  # enforce uniqueness before insert
    rows = make_rows(n, unique_users=True)  # ensure no violations
    elapsed = bulk_insert(rows)
    times_unique.append(elapsed)

# 2) check rating 
for n in SIZES:
    print(f"[CHECK rating 1..5] n={n}")
    drop_table()
    create_table_base(not_null=False)
    add_check_rating()
    rows = make_rows(n, unique_users=False)
    elapsed = bulk_insert(rows)
    times_check.append(elapsed)

# 3) not null all fields
for n in SIZES:
    print(f"[NOT NULL all fields] n={n}")
    drop_table()
    create_table_base(not_null=True)  # columns defined with NOT NULL
    rows = make_rows(n, unique_users=False)
    elapsed = bulk_insert(rows)
    times_notnull.append(elapsed)

# plot
plt.figure(figsize=(10, 6))
plt.plot(SIZES, times_unique, marker="o", label="Unique(user_id)")
plt.plot(SIZES, times_check, marker="o", label="Check: rating 1–5")
plt.plot(SIZES, times_notnull, marker="o", label="Not Null (all fields)")
plt.xticks(SIZES, [f"{s//1000}K" for s in SIZES], rotation=45)
plt.xlabel("Number of Rows (inserted)")
plt.ylabel("Time (seconds)")
plt.title("Constraint: Time vs Data Size (CockroachDB)")
plt.grid(True, axis="both")
plt.legend()
plt.tight_layout()
plt.savefig("CockroachDB_Images/constraint.png", dpi=150)
plt.show()

conn.close()
