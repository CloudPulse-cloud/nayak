import boto3
import json
import os
import time

# ── FILE PATHS ────────────────────────────────────────────────────────────────
BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE  = os.path.join(BASE_DIR, "aws_credentials.json")
EC2_CREDS_FILE    = os.path.join(BASE_DIR, "ec2_credentials.json")
KEY_PAIR_FILE     = os.path.join(BASE_DIR, "ec2_keypair.pem")


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
    print(f"  ✅ Credentials loaded (region: {creds['region']})")
    return creds


# ── LOAD EC2 CREDENTIALS ──────────────────────────────────────────────────────
def load_ec2_credentials():
    if not os.path.exists(EC2_CREDS_FILE):
        return None
    with open(EC2_CREDS_FILE, "r") as f:
        return json.load(f)


# ── FETCH ALL RUNNING INSTANCES ───────────────────────────────────────────────
def get_all_instances(ec2):
    response = ec2.describe_instances(
        Filters=[{
            "Name"  : "instance-state-name",
            "Values": ["pending", "running", "stopping", "stopped"]
        }]
    )
    instances = []
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            name = ""
            for tag in instance.get("Tags", []):
                if tag["Key"] == "Name":
                    name = tag["Value"]
                    break
            instances.append({
                "id"    : instance["InstanceId"],
                "type"  : instance["InstanceType"],
                "state" : instance["State"]["Name"],
                "ip"    : instance.get("PublicIpAddress", "N/A"),
                "name"  : name if name else "No Name"
            })
    return instances


# ── TERMINATE INSTANCE ────────────────────────────────────────────────────────
def terminate_instance(ec2, instance_id):
    try:
        ec2.terminate_instances(InstanceIds=[instance_id])
        print(f"  ✅ Instance {instance_id} terminated successfully")
        return True
    except Exception as e:
        print(f"  ❌ Failed to terminate {instance_id}: {e}")
        return False


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    divider()
    print("        AWS EC2 Terminator")
    divider()

    # 1. Load credentials
    creds = load_credentials()

    session = boto3.Session(
        aws_access_key_id     = creds["aws_access_key_id"],
        aws_secret_access_key = creds["aws_secret_access_key"],
        region_name           = creds["region"]
    )

    ec2 = session.client("ec2", region_name=creds["region"])

    # 2. Check ec2_credentials.json for saved instance
    ec2_creds = load_ec2_credentials()
    if ec2_creds:
        print(f"\n  📄 Found saved EC2 credentials (ec2_credentials.json):")
        print()
        box("SAVED INSTANCE", [
            f"Instance ID  : {ec2_creds.get('instance_id', 'N/A')}",
            f"Public IP    : {ec2_creds.get('public_ip', 'N/A')}",
            f"OS           : {ec2_creds.get('os', 'N/A')}",
        ])
        print()
        use_saved = input("  ▶  Terminate this saved instance? (yes/no): ").strip().lower()
        if use_saved == "yes":
            terminate_instance(ec2, ec2_creds["instance_id"])
            # Clear saved credentials
            os.remove(EC2_CREDS_FILE)
            print(f"  🗑️  ec2_credentials.json removed")
            exit()

    # 3. Fetch all running instances
    print(f"\n  🔍 Fetching all EC2 instances...\n")
    instances = get_all_instances(ec2)

    if not instances:
        print("  ⚠️  No instances found in your account.")
        exit()

    # 4. Show all instances
    print(f"  {'#':<4} {'Instance ID':<22} {'Type':<14} {'State':<12} {'IP':<18} {'Name'}")
    print(f"  {'─'*4} {'─'*22} {'─'*14} {'─'*12} {'─'*18} {'─'*20}")
    for i, inst in enumerate(instances, start=1):
        print(f"  {i:<4} {inst['id']:<22} {inst['type']:<14} {inst['state']:<12} {inst['ip']:<18} {inst['name']}")

    print()
    print("  Options:")
    print("  - Enter a number to terminate that instance")
    print("  - Enter 'all' to terminate ALL instances")
    print("  - Enter 'no' to cancel")
    print()

    choice = input("  ▶  Your choice: ").strip().lower()

    if choice == "no":
        print("  ❌ Cancelled.")

    elif choice == "all":
        print()
        confirm = input(f"  ⚠️  Terminate ALL {len(instances)} instances? (yes/no): ").strip().lower()
        if confirm == "yes":
            for inst in instances:
                print(f"  🗑️  Terminating {inst['id']} ({inst['name']})...")
                terminate_instance(ec2, inst["id"])
                time.sleep(0.5)
            # Clear saved credentials
            if os.path.exists(EC2_CREDS_FILE):
                os.remove(EC2_CREDS_FILE)
                print(f"  🗑️  ec2_credentials.json removed")
                if os.path.exists(KEY_PAIR_FILE):
                    os.remove(KEY_PAIR_FILE)
                    print(f"  🗑️  ec2_keypair.pem removed")
            print()
            box("DONE", [
                f"All {len(instances)} instances terminated ✅",
            ])
        else:
            print("  ❌ Cancelled.")

    else:
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(instances):
                inst = instances[idx]
                print()
                box("TERMINATING", [
                    f"Instance ID : {inst['id']}",
                    f"Type        : {inst['type']}",
                    f"State       : {inst['state']}",
                    f"IP          : {inst['ip']}",
                    f"Name        : {inst['name']}",
                ])
                print()
                confirm = input("  ▶  Confirm termination? (yes/no): ").strip().lower()
                if confirm == "yes":
                    terminate_instance(ec2, inst["id"])
                    if os.path.exists(EC2_CREDS_FILE):
                        os.remove(EC2_CREDS_FILE)
                        print(f"  🗑️  ec2_credentials.json removed")
                    if os.path.exists(KEY_PAIR_FILE):
                        os.remove(KEY_PAIR_FILE)
                        print(f"  🗑️  ec2_keypair.pem removed")
                else:
                    print("  ❌ Cancelled.")
            else:
                print("  ⚠️  Invalid number.")
        except ValueError:
            print("  ⚠️  Invalid input.")
