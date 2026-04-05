import os
import csv

# ── CONFIG ────────────────────────────────────────────────────────────────────
BASE_DIR          = os.path.dirname(os.path.abspath(__file__))

# Auto detect master file — works with or without .csv extension
def find_master_file():
    for name in ["master.csv", "master", "Master.csv", "Master"]:
        path = os.path.join(BASE_DIR, name)
        if os.path.exists(path):
            return path
    raise FileNotFoundError("master.csv not found in directory")

MASTER_CSV = find_master_file()
ALERTS_PER_BUDGET = 5
EMAILS_PER_ALERT  = 10


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

def divider():
    print("─" * 60)


# ── SMART CSV READER ──────────────────────────────────────────────────────────
def smart_open_csv(csv_file):
    encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252", "iso-8859-1"]
    for encoding in encodings:
        try:
            with open(csv_file, newline="", encoding=encoding) as f:
                content = f.read()
            rows = list(csv.DictReader(content.splitlines(), skipinitialspace=True))
            cleaned = []
            for row in rows:
                cleaned.append({k.strip().lstrip('\ufeff').lower(): v.strip() for k, v in row.items()})
            if cleaned:
                return cleaned, encoding
        except Exception:
            continue
    return [], None


# ── LOAD MASTER EMAILS ────────────────────────────────────────────────────────
def load_master_emails():
    if not os.path.exists(MASTER_CSV):
        raise FileNotFoundError(f"master file not found at: {MASTER_CSV}")

    print(f"  📂 Reading: {MASTER_CSV}")

    encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252", "iso-8859-1"]
    emails = []

    for encoding in encodings:
        try:
            emails = []
            with open(MASTER_CSV, newline="", encoding=encoding, errors="ignore") as f:
                # Try reading first line to detect delimiter
                first_line = f.readline().strip()
                f.seek(0)

                # Detect delimiter
                delimiter = ","
                if ";" in first_line:
                    delimiter = ";"
                elif "	" in first_line:
                    delimiter = "	"

                reader = csv.reader(f, delimiter=delimiter)
                headers = None
                email_col = None

                for row_idx, row in enumerate(reader):
                    if row_idx == 0:
                        # Clean headers
                        headers = [h.strip().lstrip('﻿').lower() for h in row]
                        # Find email column
                        for col_idx, h in enumerate(headers):
                            if "email" in h:
                                email_col = col_idx
                                break
                        if email_col is None:
                            email_col = 0  # default to first column
                        continue

                    if email_col < len(row):
                        email = row[email_col].strip()
                        if email and "@" in email:
                            emails.append(email)

            if emails:
                print(f"  ✅ Loaded {len(emails):,} emails (encoding: {encoding}, delimiter: '{delimiter}')")
                return emails

        except Exception as e:
            continue

    print(f"  ❌ Could not read emails from master file")
    return []


# ── DETECT ACCOUNT FOLDERS ────────────────────────────────────────────────────
def detect_account_folders():
    print(f"\n  🔍 Scanning for account folders...")
    folders = []
    for item in sorted(os.listdir(BASE_DIR)):
        full_path = os.path.join(BASE_DIR, item)
        if not os.path.isdir(full_path):
            continue
        has_credentials = os.path.exists(os.path.join(full_path, "aws_credentials.json"))
        has_budgets     = os.path.exists(os.path.join(full_path, "budgets.csv"))
        if has_credentials and has_budgets:
            folders.append(full_path)
            print(f"    ✅ {item}")
        else:
            missing = []
            if not has_credentials: missing.append("aws_credentials.json")
            if not has_budgets:     missing.append("budgets.csv")
            print(f"    ⚠️  Skipping '{item}' — missing: {', '.join(missing)}")
    print(f"\n  ✅ Found {len(folders)} valid folders")
    return folders


# ── SCAN budgets.csv AND CALCULATE EMAILS NEEDED ─────────────────────────────
def calculate_emails_needed(folder_path):
    """Scan budgets.csv and calculate exactly how many emails needed"""
    rows, _ = smart_open_csv(os.path.join(folder_path, "budgets.csv"))
    budget_count  = sum(1 for row in rows if row.get("name", "").strip())
    emails_needed = budget_count * ALERTS_PER_BUDGET * EMAILS_PER_ALERT
    return budget_count, emails_needed


# ── WRITE FRESH emails.csv ────────────────────────────────────────────────────
def write_emails_csv(folder_path, emails):
    """Wipe old emails.csv and write fresh emails with empty report"""
    emails_csv = os.path.join(folder_path, "emails.csv")
    with open(emails_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["emails", "report"])
        writer.writeheader()
        for email in emails:
            writer.writerow({"emails": email, "report": ""})


# ── MARK EMAILS IN MASTER.CSV ────────────────────────────────────────────────
def mark_master_csv(distributed_emails):
    """Mark distributed emails as 'distributed' in master.csv report column"""
    print(f"  📝 Marking {len(distributed_emails):,} emails in master file...")

    distributed_set = set(e.lower() for e in distributed_emails)
    updated         = 0

    encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252", "iso-8859-1"]

    for encoding in encodings:
        try:
            # Read all lines
            with open(MASTER_CSV, newline="", encoding=encoding, errors="ignore") as f:
                first_line = f.readline().strip()
                f.seek(0)

                delimiter = ","
                if ";" in first_line:
                    delimiter = ";"
                elif "	" in first_line:
                    delimiter = "	"

                reader  = csv.reader(f, delimiter=delimiter)
                rows    = list(reader)

            if not rows:
                continue

            # Find email and report column indices
            headers    = [h.strip().lstrip('﻿').lower() for h in rows[0]]
            email_col  = next((i for i, h in enumerate(headers) if "email" in h), 0)
            report_col = next((i for i, h in enumerate(headers) if "report" in h), None)

            # Add report column if missing
            if report_col is None:
                headers.append("report")
                report_col = len(headers) - 1
                rows[0].append("report")
                for row in rows[1:]:
                    row.append("")

            # Mark distributed emails
            for row in rows[1:]:
                if email_col < len(row):
                    email = row[email_col].strip().lower()
                    if email in distributed_set:
                        while len(row) <= report_col:
                            row.append("")
                        row[report_col] = "distributed"
                        updated += 1

            # Write back
            with open(MASTER_CSV, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=delimiter)
                writer.writerows(rows)

            print(f"  ✅ master file updated — {updated:,} emails marked as 'distributed'")
            return

        except Exception as e:
            continue

    print(f"  ❌ Could not update master file: check file permissions")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    divider()
    print("        Email Distributor")
    print("   Scans budgets.csv → calculates → distributes from master.csv")
    divider()

    # 1. Load master emails
    print()
    all_emails   = load_master_emails()
    total_emails = len(all_emails)

    # 2. Detect folders
    folders = detect_account_folders()
    if not folders:
        print("\n  ❌ No valid folders found!")
        exit()

    # 3. Scan each budgets.csv and calculate emails needed
    print(f"\n  📊 Scanning budgets.csv in each folder...")
    divider()
    folder_info  = []
    total_needed = 0

    for folder in folders:
        name                     = os.path.basename(folder)
        budget_count, emails_needed = calculate_emails_needed(folder)
        total_needed            += emails_needed
        folder_info.append({
            "path"         : folder,
            "name"         : name,
            "budgets"      : budget_count,
            "emails_needed": emails_needed,
        })
        print(f"  📁 {name}")
        print(f"       Budgets found    : {budget_count}")
        print(f"       Calculation      : {budget_count} × {ALERTS_PER_BUDGET} alerts × {EMAILS_PER_ALERT} emails")
        print(f"       Emails needed    : {emails_needed:,}")
        print()

    # 4. Show full plan
    divider()
    box("DISTRIBUTION PLAN", [
        f"Total emails in master.csv : {total_emails:,}",
        f"Total emails needed        : {total_needed:,}",
        f"Total account folders      : {len(folders)}",
        f"Emails per budget          : {ALERTS_PER_BUDGET} alerts × {EMAILS_PER_ALERT} = {ALERTS_PER_BUDGET * EMAILS_PER_ALERT}",
        f"Status                     : {'✅ Enough' if total_emails >= total_needed else f'⚠️  Short by {total_needed - total_emails:,}'}",
    ])

    # 5. Check if enough emails — STOP if not enough
    if total_emails < total_needed:
        print()
        box("❌ NOT ENOUGH EMAILS — STOPPED", [
            f"Emails available in master.csv : {total_emails:,}",
            f"Emails needed for all folders  : {total_needed:,}",
            f"Shortfall                      : {total_needed - total_emails:,} emails missing",
            f"Action                         : Script stopped — nothing distributed",
            f"Fix                            : Add {total_needed - total_emails:,} more emails to master.csv",
        ])
        exit()

    # 6. Confirm
    print()
    print("  ⚠️  This will WIPE and replace emails.csv in all folders!")
    confirm = input("  ▶  Proceed? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("  ❌ Cancelled.")
        exit()

    # 7. Distribute emails
    divider()
    print(f"\n  🚀 Distributing emails...\n")

    email_pointer  = 0
    skipped        = 0

    for i, f in enumerate(folder_info, start=1):
        emails_needed = f["emails_needed"]

        # Check if any emails left
        if email_pointer >= total_emails:
            print(f"  ⚠️  [{i}/{len(folder_info)}] '{f['name']}' — Emails ran out! Stopping.")
            skipped += len(folder_info) - i + 1
            break

        folder_emails = all_emails[email_pointer:email_pointer + emails_needed]
        email_pointer += len(folder_emails)

        if not folder_emails:
            print(f"  ⚠️  [{i}/{len(folder_info)}] '{f['name']}' — No emails left! Stopping.")
            skipped += len(folder_info) - i + 1
            break

        write_emails_csv(f["path"], folder_emails)

        # Show if folder got full or partial emails
        if len(folder_emails) < emails_needed:
            print(f"  ⚠️  [{i}/{len(folder_info)}] '{f['name']}' — Partial ({len(folder_emails):,} of {emails_needed:,} needed)")
        else:
            print(f"  ✅ [{i}/{len(folder_info)}] '{f['name']}'")

        print(f"       Emails written : {len(folder_emails):,} / {emails_needed:,}")
        print(f"       emails.csv     : fresh ✅")
        print()

    # 8. Mark distributed emails in master.csv
    print(f"  📝 Updating master.csv...")
    all_distributed = all_emails[:email_pointer]
    mark_master_csv(all_distributed)

    # 9. Final summary
    box("FINAL SUMMARY", [
        f"Folders processed        : {len(folder_info) - skipped} of {len(folder_info)}",
        f"Folders skipped          : {skipped} (emails ran out)",
        f"Total emails distributed : {min(email_pointer, total_emails):,}",
        f"Emails remaining unused  : {max(0, total_emails - email_pointer):,}",
        f"master.csv updated       : {min(email_pointer, total_emails):,} marked as distributed ✅",
        f"budgets.csv              : not touched ✅",
        f"Next step                : Run create_budgets.py in each folder",
    ])
