import boto3
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── CONFIG ────────────────────────────────────────────────────────────────────
BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
REQUIRED_FILE    = "aws_credentials.json"
PARALLEL_THREADS = 20

# ── COUNTERS (shared across threads) ─────────────────────────────────────────
print_lock     = threading.Lock()
scan_done      = [0]
delete_done    = [0]
delete_total   = [0]


# ── PRINT HELPERS ─────────────────────────────────────────────────────────────
def box(title, lines):
    width = max(len(title), max(len(l) for l in lines)) + 4
    print("┌" + "─" * width + "┐")
    pad = (width - len(title)) // 2
    print("│" + " " * pad + title + " " * (width - pad - len(title)) + "│")
    print("├" + "─" * width + "┤")
    for line in lines:
        print("│  " + line + " " * (width - len(line) - 2) + "│")
    print("└" + "─" * width + "┘")

def divider(char="─", width=60):
    print(char * width)

def header(text):
    divider("═")
    print(f"  {text}")
    divider("═")

def safe_print(msg):
    with print_lock:
        print(msg)


# ── SCAN SUBFOLDERS ───────────────────────────────────────────────────────────
def scan_account_folders():
    print(f"\n  Scanning: {BASE_DIR}\n")
    ready = []
    for item in sorted(os.listdir(BASE_DIR)):
        item_path = os.path.join(BASE_DIR, item)
        if not os.path.isdir(item_path):
            continue
        cred_file = os.path.join(item_path, REQUIRED_FILE)
        if os.path.exists(cred_file):
            ready.append({"folder": item, "path": item_path})
            print(f"  [FOUND]   {item}")
        else:
            print(f"  [SKIP]    {item}  (no aws_credentials.json)")
    print()
    print(f"  Ready: {len(ready)} folder(s)")
    return ready


# ── LOAD CREDENTIALS ──────────────────────────────────────────────────────────
def load_credentials(folder_path):
    with open(os.path.join(folder_path, REQUIRED_FILE), "r") as f:
        creds = json.load(f)
    for key in ["aws_access_key_id", "aws_secret_access_key", "region"]:
        if key not in creds:
            raise KeyError(f"Missing '{key}' in aws_credentials.json")
    return creds


# ── FETCH ACCOUNT ID ──────────────────────────────────────────────────────────
def get_account_id(session):
    return session.client("sts").get_caller_identity()["Account"]


# ══════════════════════════════════════════════════════════════
# PHASE 1 — SCAN ONE ACCOUNT
# ══════════════════════════════════════════════════════════════
def scan_one_account(folder_info, total_folders):
    folder      = folder_info["folder"]
    folder_path = folder_info["path"]
    try:
        creds = load_credentials(folder_path)
        session = boto3.Session(
            aws_access_key_id     = creds["aws_access_key_id"],
            aws_secret_access_key = creds["aws_secret_access_key"],
            region_name           = creds["region"]
        )
        account_id   = get_account_id(session)
        client       = session.client("budgets", region_name="us-east-1")
        budget_names = []
        paginator    = client.get_paginator("describe_budgets")
        for page in paginator.paginate(AccountId=account_id):
            for budget in page.get("Budgets", []):
                budget_names.append(budget["BudgetName"])

        with print_lock:
            scan_done[0] += 1
            print(f"  [SCANNED {scan_done[0]}/{total_folders}]  {folder:10s}  ->  Account: {account_id}  |  Budgets: {len(budget_names)}")

        return {
            "folder"     : folder,
            "folder_path": folder_path,
            "account_id" : account_id,
            "creds"      : creds,
            "budgets"    : budget_names,
            "status"     : "ok"
        }
    except Exception as e:
        with print_lock:
            scan_done[0] += 1
            print(f"  [ERROR  {scan_done[0]}/{total_folders}]  {folder:10s}  ->  {e}")
        return {"folder": folder, "status": "error", "error": str(e)}


# ══════════════════════════════════════════════════════════════
# PHASE 2 — DELETE ALL BUDGETS FROM ONE ACCOUNT
# ══════════════════════════════════════════════════════════════
def delete_budgets_from_account(account_data):
    account_id   = account_data["account_id"]
    folder       = account_data["folder"]
    creds        = account_data["creds"]
    budget_names = account_data["budgets"]

    session = boto3.Session(
        aws_access_key_id     = creds["aws_access_key_id"],
        aws_secret_access_key = creds["aws_secret_access_key"],
        region_name           = creds["region"]
    )
    client  = session.client("budgets", region_name="us-east-1")
    deleted = [0]
    failed  = [0]

    def delete_one(budget_name):
        try:
            client.delete_budget(AccountId=account_id, BudgetName=budget_name)
            with print_lock:
                deleted[0]     += 1
                delete_done[0] += 1
                pct = int((delete_done[0] / delete_total[0]) * 100)
                filled  = int(pct / 2)
                bar     = "#" * filled + "-" * (50 - filled)
                print(f"  [{bar}] {pct}% ({delete_done[0]}/{delete_total[0]})  [{folder}] Deleted: {budget_name}")
        except Exception as e:
            with print_lock:
                failed[0]      += 1
                delete_done[0] += 1
                print(f"  [FAILED]  [{folder}] {budget_name} -> {e}")

    with ThreadPoolExecutor(max_workers=PARALLEL_THREADS) as executor:
        futures = [executor.submit(delete_one, name) for name in budget_names]
        for future in as_completed(futures):
            future.result()

    return deleted[0], failed[0]


# ── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    header("AWS BUDGET CANCELLER")

    # ── FIND FOLDERS ──────────────────────────────────────────────────────────
    ready_folders = scan_account_folders()
    if not ready_folders:
        print("\n  No valid folders found. Make sure each subfolder has aws_credentials.json")
        exit()

    # ── SCAN ALL ACCOUNTS IN PARALLEL ─────────────────────────────────────────
    print()
    header(f"PHASE 1 — SCANNING {len(ready_folders)} ACCOUNTS SIMULTANEOUSLY")
    print()

    total_folders = len(ready_folders)
    scan_results  = []

    with ThreadPoolExecutor(max_workers=total_folders) as executor:
        futures = {
            executor.submit(scan_one_account, fi, total_folders): fi
            for fi in ready_folders
        }
        for future in as_completed(futures):
            scan_results.append(future.result())

    scan_results.sort(key=lambda x: x["folder"])

    # ── TALLY RESULTS ─────────────────────────────────────────────────────────
    valid_accounts = [r for r in scan_results if r["status"] == "ok" and len(r["budgets"]) > 0]
    clean_accounts = [r for r in scan_results if r["status"] == "ok" and len(r["budgets"]) == 0]
    error_accounts = [r for r in scan_results if r["status"] == "error"]
    total_budgets  = sum(len(r["budgets"]) for r in valid_accounts)

    print()
    divider()
    print(f"  Accounts scanned    : {len(scan_results)}")
    print(f"  Total budgets found : {total_budgets}")
    print(f"  Already clean       : {len(clean_accounts)}")
    print(f"  Errors              : {len(error_accounts)}")
    divider()

    if total_budgets == 0:
        print("\n  All accounts are already clean. Nothing to delete!")
        exit()

    # ── CONFIRM ───────────────────────────────────────────────────────────────
    print()
    box("DELETION PLAN", [
        f"Accounts to clean   : {len(valid_accounts)}",
        f"Total budgets       : {total_budgets}",
        f"Method              : ALL accounts deleted SIMULTANEOUSLY",
        f"emails.csv          : NOT touched",
        f"WARNING             : This CANNOT be undone!",
    ])
    print()
    confirm = input("  Type YES to delete all budgets: ").strip().lower()
    if confirm != "yes":
        print("\n  Cancelled — nothing was deleted.")
        exit()

    # ── DELETE ALL SIMULTANEOUSLY ─────────────────────────────────────────────
    print()
    header(f"PHASE 2 — DELETING {total_budgets} BUDGETS SIMULTANEOUSLY")
    print()

    delete_total[0] = total_budgets
    all_del_results = []

    with ThreadPoolExecutor(max_workers=len(valid_accounts)) as executor:
        futures = {
            executor.submit(delete_budgets_from_account, acc): acc
            for acc in valid_accounts
        }
        for future in as_completed(futures):
            deleted, failed = future.result()
            all_del_results.append((deleted, failed))

    # ── FINAL SUMMARY ─────────────────────────────────────────────────────────
    total_deleted = sum(d for d, _ in all_del_results)
    total_failed  = sum(f for _, f in all_del_results)

    print()
    header("FINAL SUMMARY")
    print()
    for result in scan_results:
        folder = result["folder"].upper()
        if result["status"] == "ok" and len(result["budgets"]) > 0:
            print(f"  [DONE]   {folder:15s}  Account: {result['account_id']}  Deleted: {len(result['budgets'])}")
        elif result["status"] == "ok":
            print(f"  [CLEAN]  {folder:15s}  Account: {result['account_id']}  Already clean")
        else:
            print(f"  [ERROR]  {folder:15s}  {result.get('error', '')}")

    print()
    print(f"  Total deleted   : {total_deleted}")
    print(f"  Total failed    : {total_failed}")
    print(f"  Accounts cleaned: {len(valid_accounts)}")
    divider("═")
