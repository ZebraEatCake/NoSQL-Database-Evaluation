import time
import statistics as stats
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pymongo

# ================== CONFIG ==================
MONGO_ROOT_URI = "mongodb://localhost:27017/admin"  # admin auth connection
ROOT_USER = None   # e.g. "admin"; set None if no auth required for localhost
ROOT_PWD  = None   # e.g. "secret"

DB_NAME = "first100k"
COLL_NAME = "user_review"
TARGET_USER_ID = "AGBFYI2DDIKXC5Y4FARTYDTQBMFQ"

VIEWER_COUNT_LEVELS = [10, 50, 100, 500, 1000]
BUYER_COUNT_LEVELS  = [10, 50, 100, 500, 1000]

OPS_PER_USER = 40          # how many ops each simulated user performs per run
REPEATS_PER_LEVEL = 5      # repeat the whole level and take p95 over all samples
VIEW_LIMIT = 200           # how many docs a viewer reads per op

USER_PASSWORD = "Pass123!"  # password for all created users
# ============================================

def admin_client():
    if ROOT_USER and ROOT_PWD:
        return pymongo.MongoClient(
            f"mongodb://{ROOT_USER}:{ROOT_PWD}@localhost:27017/admin",
            uuidRepresentation="standard"
        )
    return pymongo.MongoClient(MONGO_ROOT_URI, uuidRepresentation="standard")

def app_client(username, password):
    # Each user authenticates against admin
    return pymongo.MongoClient(
        f"mongodb://{username}:{password}@localhost:27017/admin",
        uuidRepresentation="standard"
    )

def ensure_roles_and_users():
    """Create roles and enough users for both roles; ignore if they already exist."""
    adm = admin_client()
    adb = adm["admin"]
    # ---- Roles ----
    def create_role(name, privileges, roles=None):
        roles = roles or []
        try:
            adb.command("createRole", name, privileges=privileges, roles=roles)
        except Exception:
            # role probably exists; try updating to be safe
            try:
                adb.command("updateRole", name, privileges=privileges, roles=roles)
            except Exception:
                pass

    create_role(
        "viewer",
        privileges=[{"resource": {"db": DB_NAME, "collection": COLL_NAME}, "actions": ["find"]}],
    )
    create_role(
        "buyer",
        privileges=[{"resource": {"db": DB_NAME, "collection": COLL_NAME}, "actions": ["find", "update", "remove"]}],
    )

    # ---- Users ----
    required_viewers = max(VIEWER_COUNT_LEVELS)
    required_buyers  = max(BUYER_COUNT_LEVELS)

    def create_user(username, role):
        try:
            adb.command("createUser", username, pwd=USER_PASSWORD, roles=[{"role": role, "db": "admin"}])
        except Exception:
            # user might exist; make sure role is present
            try:
                adb.command("updateUser", username, roles=[{"role": role, "db": "admin"}])
            except Exception:
                pass

    for i in range(required_viewers):
        create_user(f"viewer_{i}", "viewer")
    for i in range(required_buyers):
        create_user(f"buyer_{i}", "buyer")

def viewer_worker(credential_idx):
    client = app_client(f"viewer_{credential_idx}", USER_PASSWORD)
    col = client[DB_NAME][COLL_NAME]
    lat = []
    for _ in range(OPS_PER_USER):
        t0 = time.perf_counter()
        # read workload
        list(col.find({"rating": 5}).limit(VIEW_LIMIT))
        lat.append(time.perf_counter() - t0)
    client.close()
    return lat

def buyer_worker(credential_idx):
    client = app_client(f"buyer_{credential_idx}", USER_PASSWORD)
    col = client[DB_NAME][COLL_NAME]
    lat = []
    for _ in range(OPS_PER_USER):
        # Make sure there is a real write (toggle value each time)
        col.update_many({"user_id": TARGET_USER_ID}, {"$set": {"verified_purchase": True}})
        t0 = time.perf_counter()
        col.update_many({"user_id": TARGET_USER_ID}, {"$set": {"verified_purchase": False}})
        lat.append(time.perf_counter() - t0)
    client.close()
    return lat

def run_level(role: str, n_users: int):
    worker = viewer_worker if role == "viewer" else buyer_worker

    # warm-up (single user)
    worker(0)

    samples = []
    for _ in range(REPEATS_PER_LEVEL):
        with ThreadPoolExecutor(max_workers=n_users) as ex:
            results = list(ex.map(worker, range(n_users)))
        # flatten latencies
        for l in results:
            samples.extend(l)
    # p95
    samples_sorted = sorted(samples)
    idx = max(0, int(0.95 * len(samples_sorted)) - 1)
    p95 = samples_sorted[idx] if samples_sorted else 0.0
    return p95 * 1000.0  # ms

def main():
    ensure_roles_and_users()

    results = []  # (type, complexity, op, n_users, p95_ms)

    # VIEWER (Simple / Read)
    for n in VIEWER_COUNT_LEVELS:
        p95_ms = run_level("viewer", n)
        results.append(("Simple", "Low", "Read", n, p95_ms))
        print(f"[viewer] users={n}  p95={p95_ms:.2f} ms")

    # BUYER (Complex / Update)
    for n in BUYER_COUNT_LEVELS:
        p95_ms = run_level("buyer", n)
        results.append(("Complex", "High", "Update", n, p95_ms))
        print(f"[buyer ] users={n}  p95={p95_ms:.2f} ms")

    # Pretty table
    header = ("Access Control Type", "Complexity", "Operation", "Number of Users", "Enforcement Time (ms, p95)")
    colw = [22, 12, 10, 16, 28]
    print("\n" + " | ".join(h.ljust(w) for h, w in zip(header, colw)))
    print("-" * (sum(colw) + 3 * (len(colw) - 1)))
    for r in results:
        row = (r[0], r[1], r[2], str(r[3]), f"{r[4]:.2f}")
        print(" | ".join(v.ljust(w) for v, w in zip(row, colw)))

if __name__ == "__main__":
    main()
