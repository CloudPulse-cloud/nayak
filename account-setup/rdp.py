import boto3
import json
import os
import time
import uuid
import base64
import subprocess
import sys
from datetime import datetime, timedelta, timezone

# ── AUTO INSTALL MISSING LIBRARIES ───────────────────────────────────────────
def install_if_missing(package):
    try:
        __import__(package)
    except ImportError:
        print(f"  📦 Installing missing library: {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package, "-q"])
        print(f"  ✅ {package} installed successfully")

install_if_missing("cryptography")

# ── FILE PATHS ────────────────────────────────────────────────────────────────
BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(BASE_DIR, "aws_credentials.json")
EC2_CREDS_FILE   = os.path.join(BASE_DIR, "ec2_credentials.json")
KEY_PAIR_FILE    = os.path.join(BASE_DIR, "ec2_keypair.pem")

AUTO_TERMINATE_AT = 0.02

# ── REDSHIFT OPTIONS ──────────────────────────────────────────────────────────
REDSHIFT_OPTIONS = [
    {"label": "2 CPU  | 15 GB RAM  | 160 GB SSD  | Cheapest",     "type": "dc2.large",    "cost": 0.25},
    {"label": "2 CPU  | 16 GB RAM  | 32 GB SSD   | RA3 entry",    "type": "ra3.xlplus",   "cost": 1.086},
    {"label": "4 CPU  | 32 GB RAM  | 32 GB SSD   | RA3 standard", "type": "ra3.4xlarge",  "cost": 3.26},
    {"label": "16 CPU | 96 GB RAM  | 64 GB SSD   | RA3 large",    "type": "ra3.16xlarge", "cost": 13.04},
    {"label": "16 CPU | 244 GB RAM | 2.56 TB SSD | DC2 fastest",  "type": "dc2.8xlarge",  "cost": 4.80},
]

# ── EC2 OPTIONS (ALL) ─────────────────────────────────────────────────────────
EC2_OPTIONS_ALL = [
    {"label": "1 GB RAM  | 1 CPU  | No HDD    | Linux",   "type": "t2.micro",  "cost": 0.0116, "os": "linux"},
    {"label": "2 GB RAM  | 1 CPU  | No HDD    | Linux",   "type": "t2.small",  "cost": 0.0230, "os": "linux"},
    {"label": "4 GB RAM  | 2 CPU  | No HDD    | Linux",   "type": "t2.medium", "cost": 0.0464, "os": "linux"},
    {"label": "8 GB RAM  | 2 CPU  | No HDD    | Linux",   "type": "t2.large",  "cost": 0.0928, "os": "linux"},
    {"label": "4 GB RAM  | 2 CPU  | 32 GB SSD | Linux",   "type": "m5.large",  "cost": 0.096,  "os": "linux"},
    {"label": "1 GB RAM  | 1 CPU  | 30 GB SSD | Windows", "type": "t2.micro",  "cost": 0.0162, "os": "windows"},
    {"label": "2 GB RAM  | 1 CPU  | 30 GB SSD | Windows", "type": "t2.small",  "cost": 0.0324, "os": "windows"},
    {"label": "4 GB RAM  | 2 CPU  | 30 GB SSD | Windows", "type": "t2.medium", "cost": 0.0584, "os": "windows"},
    {"label": "8 GB RAM  | 2 CPU  | 30 GB SSD | Windows", "type": "t2.large",  "cost": 0.1168, "os": "windows"},
]


# ── DETECT AVAILABLE EC2 INSTANCE TYPES ──────────────────────────────────────
def get_available_ec2_options(ec2):
    """Use dry-run to test which instance types can actually be launched"""
    print(f"  🔍 Detecting launchable EC2 instance types on this account...")

    # Get latest Amazon Linux 2 AMI for dry run test
    try:
        response = ec2.describe_images(
            Owners=["amazon"],
            Filters=[
                {"Name": "name",         "Values": ["amzn2-ami-hvm-*-x86_64-gp2"]},
                {"Name": "state",        "Values": ["available"]},
                {"Name": "architecture", "Values": ["x86_64"]},
            ]
        )
        images = sorted(response["Images"], key=lambda x: x["CreationDate"], reverse=True)
        test_ami = images[0]["ImageId"] if images else None
    except Exception:
        test_ami = None

    available    = []
    seen_types   = set()

    for opt in EC2_OPTIONS_ALL:
        if opt["type"] in seen_types:
            # Same type already tested — reuse result
            if opt["type"] in [o["type"] for o in available]:
                available.append(opt)
            continue

        seen_types.add(opt["type"])

        if not test_ami:
            available.append(opt)
            continue

        try:
            ec2.run_instances(
                ImageId      = test_ami,
                InstanceType = opt["type"],
                MinCount     = 1,
                MaxCount     = 1,
                DryRun       = True
            )
        except Exception as e:
            err = str(e)
            if "DryRunOperation" in err:
                # DryRun succeeded — instance type is launchable
                available.append(opt)
            elif "NotUsableWith" in err or "not eligible" in err or "Unsupported" in err or "InsufficientInstanceCapacity" in err:
                print(f"    ⚠️  {opt['type']} not available on this account — skipping")
            else:
                # Unknown error — include anyway
                available.append(opt)

    # Fallback to t2.micro if nothing available
    if not available:
        print(f"  ⚠️  No instances detected — defaulting to t2.micro")
        available = [o for o in EC2_OPTIONS_ALL if o["type"] == "t2.micro"]

    print(f"  ✅ Found {len(available)} launchable instance types")
    return available


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
        raise FileNotFoundError(f"aws_credentials.json not found: {CREDENTIALS_FILE}")
    with open(CREDENTIALS_FILE, "r") as f:
        creds = json.load(f)
    required = ["aws_access_key_id", "aws_secret_access_key", "region"]
    for key in required:
        if key not in creds:
            raise KeyError(f"Missing '{key}' in aws_credentials.json")
    print(f"  ✅ Credentials loaded (region: {creds['region']})")
    return creds


# ── FETCH ACCOUNT ID ──────────────────────────────────────────────────────────
def get_account_id(session):
    sts        = session.client("sts")
    identity   = sts.get_caller_identity()
    account_id = identity["Account"]
    print(f"  ✅ Account ID fetched: {account_id}")
    return account_id


# ── GET CURRENT SPENDING ──────────────────────────────────────────────────────
def get_current_spending(session):
    try:
        ce         = session.client("ce", region_name="us-east-1")
        today      = datetime.now(timezone.utc)
        start_date = today.replace(day=1).strftime("%Y-%m-%d")
        end_date   = today.strftime("%Y-%m-%d")
        if start_date == end_date:
            start_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        response = ce.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="MONTHLY",
            Metrics=["UnblendedCost"]
        )
        amount = float(response["ResultsByTime"][0]["Total"]["UnblendedCost"]["Amount"])
        return round(amount, 6)
    except Exception:
        return None


# ── CHECK REDSHIFT ACTIVATED ──────────────────────────────────────────────────
def is_redshift_activated(session, region):
    """Check if Redshift is activated on this account"""
    print(f"\n  🔍 Checking if Redshift is activated on this account...")
    try:
        redshift = session.client("redshift", region_name=region)
        redshift.describe_clusters()
        print(f"  ✅ Redshift is activated on this account!")
        return True
    except Exception as e:
        if "OptInRequired" in str(e):
            print(f"  ⚠️  Redshift is NOT activated on this account")
            return False
        # Any other error means Redshift is accessible
        print(f"  ✅ Redshift is accessible on this account")
        return True


# ── SPENDING MONITOR COUNTDOWN ────────────────────────────────────────────────
def countdown(hours, resource_id, cost_per_hour, session, terminate_func, resource_label):
    total_seconds  = int(hours * 3600)
    elapsed        = 0
    check_interval = 120
    last_check     = 0
    current_spend  = 0.0
    warning_shown  = False
    spend_before   = get_current_spending(session) or 0.0
    time_to_hit    = round(AUTO_TERMINATE_AT / cost_per_hour * 60, 1)

    print(f"\n  ⏱️  {resource_label} running for max {hours} hour(s)")
    print(f"  ℹ️  Resource ID      : {resource_id}")
    print(f"  ℹ️  Cost per hour    : ${cost_per_hour}")
    print(f"  ℹ️  Est. time to hit : ~{time_to_hit} mins")
    print(f"  ℹ️  Auto-terminate   : At ${AUTO_TERMINATE_AT} spend")
    print(f"  ℹ️  Press CTRL+C     : To terminate early\n")

    try:
        while elapsed < total_seconds:
            remaining   = total_seconds - elapsed
            hrs         = remaining // 3600
            mins        = (remaining % 3600) // 60
            secs        = remaining % 60
            cost_so_far = round((elapsed / 3600) * cost_per_hour, 6)

            print(f"  ⏳ Remaining: {hrs:02d}:{mins:02d}:{secs:02d}  |  Est. cost: ${cost_so_far}  |  AWS spend: ${current_spend}  |  Limit: ${AUTO_TERMINATE_AT}", end="\r")

            if elapsed - last_check >= check_interval:
                last_check   = elapsed
                latest_spend = get_current_spending(session)
                if latest_spend is not None:
                    current_spend = latest_spend

            if current_spend >= (AUTO_TERMINATE_AT - 0.005) and not warning_shown:
                warning_shown = True
                print()
                print()
                divider()
                print()
                box("BUDGET GENERATED", [
                    f"Spend Before           : ${spend_before}",
                    f"Current AWS Spend      : ${current_spend}",
                    f"Spend Generated        : ${round(current_spend - spend_before, 6)}",
                    f"Auto-terminate Limit   : ${AUTO_TERMINATE_AT}",
                    f"Status                 : ⚠️  Limit approaching!",
                ])
                print()
                print(f"  🚨 WARNING: AWS spend reached ${current_spend}!")
                print(f"  🚨 {resource_label} will be TERMINATED in 20 seconds!")
                print()
                for i in range(20, 0, -1):
                    print(f"  💥 Terminating in: {i:02d} seconds...", end="\r")
                    time.sleep(1)
                print()
                print()
                terminate_func()
                final_spend = get_current_spending(session)
                print()
                box("AUTO-TERMINATE SUMMARY", [
                    f"Reason                 : Spend reached ${current_spend}",
                    f"Limit                  : ${AUTO_TERMINATE_AT}",
                    f"Resource               : {resource_id}",
                    f"Status                 : Terminated ✅",
                    f"Final AWS Spend        : ${final_spend if final_spend else 'Check Console'}",
                    f"Budget Alert Emails    : Will fire on next billing update",
                ])
                return True

            time.sleep(1)
            elapsed += 1
        print()
    except KeyboardInterrupt:
        print(f"\n\n  ⚠️  CTRL+C — terminating {resource_label}...")

    return False


# ══════════════════════════════════════════════════════════════
#  REDSHIFT FUNCTIONS
# ══════════════════════════════════════════════════════════════

def choose_redshift_node():
    print()
    divider()
    print("  Choose Redshift node type:\n")
    print(f"  {'#':<4} {'CPU / RAM / Storage':<48} {'$/hr':<10} {'Type'}")
    print(f"  {'─'*4} {'─'*48} {'─'*10} {'─'*15}")
    for i, opt in enumerate(REDSHIFT_OPTIONS, start=1):
        print(f"  {i:<4} {opt['label']:<48} ${opt['cost']:<9} {opt['type']}")
    print()
    while True:
        try:
            choice = int(input(f"  ▶  Enter number (1-{len(REDSHIFT_OPTIONS)}): ").strip())
            if 1 <= choice <= len(REDSHIFT_OPTIONS):
                selected = REDSHIFT_OPTIONS[choice - 1]
                print(f"\n  ✅ Selected: {selected['label']} (${selected['cost']}/hr)")
                return selected
            print(f"  ⚠️  Enter between 1 and {len(REDSHIFT_OPTIONS)}")
        except ValueError:
            print("  ⚠️  Invalid — enter a number")


def create_redshift_cluster(redshift, node_type):
    cluster_id = "trigger-spend-rs"
    try:
        redshift.create_cluster(
            ClusterIdentifier  = cluster_id,
            NodeType           = node_type,
            MasterUsername     = "admin",
            MasterUserPassword = "Trigger12345",
            DBName             = "triggerdb",
            ClusterType        = "single-node",
            PubliclyAccessible = False,
            Tags=[{"Key": "Name", "Value": "trigger-spend-redshift"}]
        )
        print(f"  ✅ Redshift cluster created: {cluster_id}")
        return cluster_id
    except redshift.exceptions.ClusterAlreadyExistsFault:
        print(f"  ⚠️  Cluster already exists — using: {cluster_id}")
        return cluster_id
    except Exception as e:
        print(f"  ❌ Failed to create Redshift cluster: {e}")
        return None


def wait_for_redshift(redshift, cluster_id):
    print(f"  ⏳ Waiting for Redshift cluster (~5-10 mins)...")
    for _ in range(60):
        try:
            response = redshift.describe_clusters(ClusterIdentifier=cluster_id)
            status   = response["Clusters"][0].get("ClusterStatus", "").lower()
            print(f"    Status: {status}...", end="\r")
            if status == "available":
                print()
                print(f"  ✅ Redshift cluster available!")
                return True
        except Exception:
            pass
        time.sleep(15)
    return False


def delete_redshift_cluster(redshift, cluster_id):
    print(f"\n  🗑️  Deleting Redshift cluster: {cluster_id}")
    try:
        for _ in range(40):
            try:
                response = redshift.describe_clusters(ClusterIdentifier=cluster_id)
                status   = response["Clusters"][0].get("ClusterStatus", "").lower()
                print(f"    Waiting for deletable state: {status}...", end="\r")
                if status in ["available", "incompatible-network", "incompatible-restore"]:
                    break
            except Exception:
                break
            time.sleep(15)
        print()
        redshift.delete_cluster(
            ClusterIdentifier        = cluster_id,
            SkipFinalClusterSnapshot = True
        )
        print(f"  ✅ Redshift cluster deletion initiated")
        print(f"  ⏳ Waiting for full deletion...")
        while True:
            try:
                redshift.describe_clusters(ClusterIdentifier=cluster_id)
                time.sleep(10)
            except redshift.exceptions.ClusterNotFoundFault:
                print(f"  ✅ Redshift cluster fully deleted!")
                break
            except Exception:
                break
    except Exception as e:
        print(f"  ❌ Delete failed: {e}")
        print(f"  ⚠️  Manually delete cluster '{cluster_id}' in AWS Console!")


def run_redshift(session, region, spend_before):
    redshift = session.client("redshift", region_name=region)
    selected = choose_redshift_node()

    print()
    while True:
        try:
            hours = float(input("  ▶  Max hours to run Redshift? (e.g. 0.1, 1): ").strip())
            if hours > 0:
                break
            print("  ⚠️  Enter > 0")
        except ValueError:
            print("  ⚠️  Invalid")

    estimated_cost = round(hours * selected["cost"], 4)
    time_to_hit    = round(AUTO_TERMINATE_AT / selected["cost"] * 60, 1)

    print()
    box("REDSHIFT LAUNCH PLAN", [
        f"Node Type              : {selected['label']}",
        f"AWS Type               : {selected['type']}",
        f"Region                 : {region}",
        f"Max Duration           : {hours} hour(s)",
        f"Cost per hour          : ${selected['cost']}",
        f"Max Estimated Cost     : ${estimated_cost}",
        f"Est. time to hit $0.02 : ~{time_to_hit} minutes",
        f"Auto-terminate at      : ${AUTO_TERMINATE_AT} spend",
    ])

    print()
    if input("  ▶  Proceed? (yes/no): ").strip().lower() != "yes":
        print("  ❌ Cancelled.")
        return

    cluster_id = create_redshift_cluster(redshift, selected["type"])
    if not cluster_id:
        return

    if not wait_for_redshift(redshift, cluster_id):
        delete_redshift_cluster(redshift, cluster_id)
        return

    divider()
    terminate_func = lambda: delete_redshift_cluster(redshift, cluster_id)
    already_done   = countdown(hours, cluster_id, selected["cost"], session, terminate_func, "Redshift")

    if not already_done:
        delete_redshift_cluster(redshift, cluster_id)
        spend_after = get_current_spending(session)
        print()
        box("FINAL SUMMARY", [
            f"Node Type              : {selected['label']}",
            f"Cluster ID             : {cluster_id}",
            f"Run Duration           : {hours} hour(s)",
            f"Max Estimated Cost     : ${estimated_cost}",
            f"Spend Before           : ${spend_before}",
            f"Spend After            : ${spend_after if spend_after is not None else 'Check Console'}",
            f"Redshift Status        : Deleted ✅",
            f"Budget Alert Emails    : Will fire on next billing update",
        ])


# ══════════════════════════════════════════════════════════════
#  EC2 FUNCTIONS
# ══════════════════════════════════════════════════════════════

def choose_ec2_instance(ec2_options):
    print()
    divider()
    print("  Choose EC2 instance type:\n")
    print(f"  {'#':<4} {'RAM / CPU / Storage / OS':<48} {'$/hr':<10} {'AWS Type'}")
    print(f"  {'─'*4} {'─'*48} {'─'*10} {'─'*12}")
    for i, opt in enumerate(ec2_options, start=1):
        print(f"  {i:<4} {opt['label']:<48} ${opt['cost']:<9} {opt['type']}")
    print()
    while True:
        try:
            choice = int(input(f"  ▶  Enter number (1-{len(ec2_options)}): ").strip())
            if 1 <= choice <= len(ec2_options):
                selected = ec2_options[choice - 1]
                print(f"\n  ✅ Selected: {selected['label']} (${selected['cost']}/hr)")
                return selected
            print(f"  ⚠️  Enter between 1 and {len(ec2_options)}")
        except ValueError:
            print("  ⚠️  Invalid — enter a number")


def get_latest_ami(ec2, os_type):
    try:
        if os_type == "windows":
            filters = [
                {"Name": "name",         "Values": ["Windows_Server-2022-English-Full-Base-*"]},
                {"Name": "state",        "Values": ["available"]},
                {"Name": "architecture", "Values": ["x86_64"]},
            ]
        else:
            filters = [
                {"Name": "name",         "Values": ["amzn2-ami-hvm-*-x86_64-gp2"]},
                {"Name": "state",        "Values": ["available"]},
                {"Name": "architecture", "Values": ["x86_64"]},
            ]
        response = ec2.describe_images(Owners=["amazon"], Filters=filters)
        images   = sorted(response["Images"], key=lambda x: x["CreationDate"], reverse=True)
        if images:
            print(f"  ✅ Latest {os_type} AMI: {images[0]['ImageId']}")
            return images[0]["ImageId"]
    except Exception as e:
        print(f"  ❌ AMI fetch failed: {e}")
    return None


def create_security_group(ec2, os_type):
    sg_name = "trigger-spend-sg"
    try:
        try:
            existing = ec2.describe_security_groups(GroupNames=[sg_name])
            sg_id    = existing["SecurityGroups"][0]["GroupId"]
            ec2.delete_security_group(GroupId=sg_id)
            time.sleep(2)
        except Exception:
            pass
        response = ec2.create_security_group(
            GroupName   = sg_name,
            Description = "Trigger spend security group"
        )
        sg_id = response["GroupId"]
        port  = 3389 if os_type == "windows" else 22
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{
                "IpProtocol": "tcp",
                "FromPort"  : port,
                "ToPort"    : port,
                "IpRanges"  : [{"CidrIp": "0.0.0.0/0"}]
            }]
        )
        print(f"  ✅ Security group created (port {port} open)")
        return sg_id
    except Exception as e:
        print(f"  ⚠️  Security group failed: {e}")
        return None


def create_key_pair(ec2):
    key_name = "ec2-trigger-keypair"
    try:
        try:
            ec2.delete_key_pair(KeyName=key_name)
        except Exception:
            pass
        response    = ec2.create_key_pair(KeyName=key_name)
        private_key = response["KeyMaterial"]
        with open(KEY_PAIR_FILE, "w") as f:
            f.write(private_key)
        os.chmod(KEY_PAIR_FILE, 0o400)
        print(f"  ✅ Key pair created: ec2_keypair.pem")
        return key_name, private_key
    except Exception as e:
        print(f"  ❌ Key pair failed: {e}")
        return None, None


def launch_ec2_instance(ec2, ami_id, instance_type, key_name, sg_id, os_type, password=None):
    try:
        kwargs = dict(
            ImageId          = ami_id,
            InstanceType     = instance_type,
            MinCount         = 1,
            MaxCount         = 1,
            KeyName          = key_name,
            SecurityGroupIds = [sg_id] if sg_id else [],
            TagSpecifications=[{
                "ResourceType": "instance",
                "Tags": [{"Key": "Name", "Value": "trigger-spend-ec2"}]
            }]
        )
        if os_type == "linux" and password:
            kwargs["UserData"] = f"""#!/bin/bash
echo "ec2-user:{password}" | chpasswd
sed -i 's/^PasswordAuthentication no/PasswordAuthentication yes/' /etc/ssh/sshd_config
systemctl restart sshd
"""
        response    = ec2.run_instances(**kwargs)
        instance_id = response["Instances"][0]["InstanceId"]
        print(f"  ✅ EC2 launched: {instance_id}")
        return instance_id
    except Exception as e:
        print(f"  ❌ EC2 launch failed: {e}")
        return None


def get_public_ip(ec2, instance_id):
    try:
        response = ec2.describe_instances(InstanceIds=[instance_id])
        return response["Reservations"][0]["Instances"][0].get("PublicIpAddress", "N/A")
    except Exception:
        return "N/A"


def save_ec2_credentials(instance_id, public_ip, os_type, username, password):
    creds = {
        "instance_id" : instance_id,
        "public_ip"   : public_ip,
        "os"          : os_type,
        "username"    : username,
        "password"    : password if password else "Use ec2_keypair.pem",
        "rdp_port"    : 3389 if os_type == "windows" else "N/A",
        "rdp_command" : f"mstsc /v:{public_ip}" if os_type == "windows" else f"ssh -i ec2_keypair.pem {username}@{public_ip}",
    }
    with open(EC2_CREDS_FILE, "w") as f:
        json.dump(creds, f, indent=4)
    print(f"  ✅ EC2 credentials saved: ec2_credentials.json")


def terminate_ec2_instance(ec2, instance_id):
    try:
        ec2.terminate_instances(InstanceIds=[instance_id])
        print(f"  ✅ EC2 {instance_id} terminated")
    except Exception as e:
        print(f"  ❌ Terminate failed: {e}")


def run_ec2(session, region, spend_before):
    ec2           = session.client("ec2", region_name=region)
    ec2_options   = get_available_ec2_options(ec2)
    selected      = choose_ec2_instance(ec2_options)
    os_type  = selected["os"]
    username = "Administrator" if os_type == "windows" else "ec2-user"

    # Password setup
    print()
    divider()
    print(f"\n  🔐 Password Setup\n")
    if os_type == "windows":
        password = None
        print(f"  ℹ️  Windows password auto-generated by AWS")
    else:
        if input("  ▶  Set custom password? (yes/no): ").strip().lower() == "yes":
            while True:
                password = input("  ▶  Enter password (min 8 chars): ").strip()
                if len(password) >= 8:
                    break
                print("  ⚠️  Too short")
        else:
            password = None

    # Run mode selection
    print()
    divider()
    print("  Choose how long to run EC2:\n")
    print("  1  ->  Few hours     (auto-terminate after selected hours)")
    print("  2  ->  Long time     (runs until YOU manually terminate)")
    print()

    while True:
        run_mode = input("  ▶  Enter 1 for few hours or 2 for long time: ").strip()
        if run_mode in ["1", "2"]:
            break
        print("  ⚠️  Please enter 1 or 2")

    if run_mode == "1":
        print()
        while True:
            try:
                hours = float(input("  ▶  How many hours to run EC2? (e.g. 1, 2, 0.5): ").strip())
                if hours > 0:
                    break
                print("  ⚠️  Enter > 0")
            except ValueError:
                print("  ⚠️  Invalid")
    else:
        hours = None

    estimated_cost = round((hours or 24) * selected["cost"], 6)

    print()
    box("EC2 LAUNCH PLAN", [
        f"Instance               : {selected['label']}",
        f"AWS Type               : {selected['type']}",
        f"OS                     : {os_type.capitalize()}",
        f"Username               : {username}",
        f"Region                 : {region}",
        f"Run Mode               : {'Few hours — ' + str(hours) + ' hr(s)' if run_mode == '1' else 'Long time — manual termination'}",
        f"Cost per hour          : ${selected['cost']}",
        f"Estimated Cost         : {'$' + str(estimated_cost) if run_mode == '1' else 'Depends on usage'}",
        f"Auto-terminate         : {'Yes — after ' + str(hours) + ' hr(s)' if run_mode == '1' else 'No — manual only'}",
        f"Credentials saved to   : ec2_credentials.json",
    ])

    print()
    if input("  ▶  Proceed? (yes/no): ").strip().lower() != "yes":
        print("  ❌ Cancelled.")
        return

    divider()
    print("  🚀 Launching EC2...\n")

    key_name, private_key = create_key_pair(ec2)
    ami_id                = get_latest_ami(ec2, os_type)
    sg_id                 = create_security_group(ec2, os_type)

    if not ami_id or not key_name:
        print("  ❌ Setup failed. Exiting.")
        return

    instance_id = launch_ec2_instance(ec2, ami_id, selected["type"], key_name, sg_id, os_type, password)
    if not instance_id:
        return

    # Wait for running
    print(f"  ⏳ Waiting for EC2 to start...")
    ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])
    print(f"  ✅ EC2 is running!")

    public_ip = get_public_ip(ec2, instance_id)
    print(f"  ✅ Public IP: {public_ip}")

    save_ec2_credentials(instance_id, public_ip, os_type, username, password)

    box("EC2 CREDENTIALS", [
        f"Instance ID   : {instance_id}",
        f"Public IP     : {public_ip}",
        f"Username      : {username}",
        f"Password      : {password if password else 'Use ec2_keypair.pem'}",
        f"Connect       : {'mstsc /v:' + public_ip if os_type == 'windows' else 'ssh -i ec2_keypair.pem ' + username + '@' + public_ip}",
    ])

    divider()

    if run_mode == "1":
        # Few hours mode — auto terminate after timer
        terminate_func = lambda: terminate_ec2_instance(ec2, instance_id)
        already_done   = countdown(hours, instance_id, selected["cost"], session, terminate_func, "EC2")

        if not already_done:
            divider()
            terminate_ec2_instance(ec2, instance_id)
            if os.path.exists(EC2_CREDS_FILE):
                os.remove(EC2_CREDS_FILE)
            if os.path.exists(KEY_PAIR_FILE):
                os.remove(KEY_PAIR_FILE)

        spend_after = get_current_spending(session)
        print()
        box("FINAL SUMMARY", [
            f"Instance               : {selected['label']}",
            f"Instance ID            : {instance_id}",
            f"Run Mode               : Few hours ({hours} hr(s))",
            f"Estimated Cost         : ${estimated_cost}",
            f"Spend Before           : ${spend_before}",
            f"Spend After            : ${spend_after if spend_after is not None else 'Check Console'}",
            f"EC2 Status             : Terminated ✅",
            f"Budget Alert Emails    : Will fire on next billing update",
        ])

    else:
        # Long time mode — no timer, no auto-terminate
        print()
        box("INSTANCE RUNNING — LONG TIME MODE", [
            f"Instance ID            : {instance_id}",
            f"Public IP              : {public_ip}",
            f"OS                     : {os_type.capitalize()}",
            f"Username               : {username}",
            f"Cost per hour          : ${selected['cost']}",
            f"Auto-terminate         : ❌ Disabled",
            f"To terminate           : Run terminate_ec2.py",
            f"Credentials saved to   : ec2_credentials.json",
        ])
        print()
        print("  ℹ️  Instance is running indefinitely")
        print("  ℹ️  Run terminate_ec2.py when you want to stop it")
        print("  ℹ️  Monitor costs at: console.aws.amazon.com/billing")


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    divider()
    print("        AWS Spending Trigger")
    print("   (Auto-detects Redshift → Falls back to EC2)")
    divider()

    # 1. Load credentials
    creds = load_credentials()

    session = boto3.Session(
        aws_access_key_id     = creds["aws_access_key_id"],
        aws_secret_access_key = creds["aws_secret_access_key"],
        region_name           = creds["region"]
    )

    account_id = get_account_id(session)
    region     = creds["region"]

    # 2. Check spending
    print(f"\n  🔍 Checking current spending...")
    spend_before = get_current_spending(session)
    if spend_before is not None:
        print(f"  💰 Current spend: ${spend_before}")
    else:
        print(f"  ⚠️  Could not fetch spend (Cost Explorer may not be enabled)")
        spend_before = 0.0

    # 3. Detect Redshift
    redshift_active = is_redshift_activated(session, region)

    print()
    if redshift_active:
        box("SERVICE STATUS", [
            f"Redshift               : ✅ Activated",
            f"EC2                    : ✅ Always available",
            f"Auto-terminate at      : ${AUTO_TERMINATE_AT}",
        ])
    else:
        box("SERVICE STATUS", [
            f"Redshift               : ❌ Not activated (manual Console activation needed)",
            f"EC2                    : ✅ Always available",
            f"Auto-terminate at      : ${AUTO_TERMINATE_AT}",
        ])

    # 4. Let user choose
    print()
    print("  Choose spending method:\n")
    print(f"  1  ->  Redshift  {'✅ Activated' if redshift_active else '⚠️  May not work (not activated)'}  |  Faster spend  |  Hits $0.02 in ~1-5 mins")
    print(f"  2  ->  EC2       ✅ Always works              |  Slower spend  |  Hits $0.02 in ~1-2 hrs")
    print()

    while True:
        choice = input("  ▶  Enter 1 or 2: ").strip()
        if choice == "1":
            print()
            run_redshift(session, region, spend_before)
            break
        elif choice == "2":
            print()
            run_ec2(session, region, spend_before)
            break
        else:
            print("  ⚠️  Please enter 1 or 2")
