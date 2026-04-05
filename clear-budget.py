import boto3
import json
import os
import time

# ── FILE PATHS ────────────────────────────────────────────────────────────────
BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(BASE_DIR, "aws_credentials.json")


# ── LOAD CREDENTIALS ──────────────────────────────────────────────────────────
def load_credentials():
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(f"aws_credentials.json not found at: {CREDENTIALS_FILE}")

    with open(CREDENTIALS_FILE, "r") as f:
        creds = json.load(f)

    required = ["aws_access_key_id", "aws_secret_access_key", "region"]
    for key in required:
        if key not in creds:
            raise KeyError(f"Missing '{key}' in aws_credentials.json")

    print(f"✅ Credentials loaded (region: {creds['region']})")
    return creds


# ── FETCH ACCOUNT ID ──────────────────────────────────────────────────────────
def get_account_id(session):
    sts = session.client("sts")
    identity = sts.get_caller_identity()
    account_id = identity["Account"]
    print(f"✅ Account ID fetched from AWS: {account_id}")
    return account_id


# ── FETCH ALL BUDGETS ─────────────────────────────────────────────────────────
def get_all_budgets(client, account_id):
    budgets = []
    paginator = client.get_paginator("describe_budgets")
    for page in paginator.paginate(AccountId=account_id):
        budgets.extend(page.get("Budgets", []))
    return budgets


# ── DELETE ALL BUDGETS ────────────────────────────────────────────────────────
def delete_all_budgets(client, account_id, budgets):
    print(f"\n🗑️  Deleting {len(budgets)} budgets...\n")

    deleted  = 0
    failed   = 0

    for i, budget in enumerate(budgets, start=1):
        name = budget["BudgetName"]
        print(f"🗑️  [{i}/{len(budgets)}] Deleting: '{name}'")
        try:
            client.delete_budget(
                AccountId=account_id,
                BudgetName=name
            )
            print(f"   ✅ Deleted")
            deleted += 1
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            failed += 1

        time.sleep(0.3)  # small delay to avoid throttling

    print(f"\n🎉 Done!")
    print(f"   Deleted : {deleted}")
    print(f"   Failed  : {failed}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("       AWS Budget Deleter")
    print("=" * 55)

    # 1. Load credentials
    creds = load_credentials()

    # 2. Create session
    session = boto3.Session(
        aws_access_key_id     = creds["aws_access_key_id"],
        aws_secret_access_key = creds["aws_secret_access_key"],
        region_name           = creds["region"]
    )

    # 3. Fetch account ID
    account_id = get_account_id(session)

    # 4. Fetch all budgets
    client  = session.client("budgets", region_name=creds["region"])
    budgets = get_all_budgets(client, account_id)

    if not budgets:
        print("\n⚠️  No budgets found in your account. Nothing to delete.")
    else:
        print(f"\n📊 Found {len(budgets)} budget(s) in your account:")
        for b in budgets:
            print(f"   - {b['BudgetName']}")

        print()
        confirm = input("▶  Delete ALL budgets? (yes/no): ").strip().lower()
        if confirm == "yes":
            delete_all_budgets(client, account_id, budgets)
        else:
            print("❌ Cancelled.")
