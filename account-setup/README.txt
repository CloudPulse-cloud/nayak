═══════════════════════════════════════════════════════════════
         AWS BUDGET CREATOR — TEAM SETUP GUIDE
         CloudPulse | Project: Nayak
═══════════════════════════════════════════════════════════════

WHAT IS THIS FOLDER?
─────────────────────
This folder contains template files for setting up AWS accounts.
Copy and rename this folder for EACH AWS account you want to run.


HOW TO SET UP — STEP BY STEP
──────────────────────────────

STEP 1 — Install Python
   Download from: https://www.python.org/downloads/
   Make sure to check "Add Python to PATH" during install.

STEP 2 — Install required library
   Open CMD and run:
      pip install boto3

STEP 3 — Get the script
   Download main.py from this GitHub repo.
   Place it in your working folder.

STEP 4 — Create account folders
   For EACH AWS account, create a separate folder like this:

      📁 Your Working Folder/
         ├── main.py
         │
         ├── 📁 Account_Darrel/
         │    ├── aws_credentials.json   ← AWS keys for this account
         │    ├── budgets.csv            ← budget names and amounts
         │    └── emails.csv            ← email list for alerts
         │
         ├── 📁 Account_John/
         │    ├── aws_credentials.json
         │    ├── budgets.csv
         │    └── emails.csv
         │
         └── 📁 Account_Sarah/
              ├── aws_credentials.json
              ├── budgets.csv
              └── emails.csv

   NOTE: Folder name can be anything. Script auto-detects all
         folders that contain aws_credentials.json inside.


STEP 5 — Set up aws_credentials.json
   Create this file inside EACH account folder.
   Use this exact format:

      {
          "aws_access_key_id"     : "AKIAXXXXXXXXXXXXXXXX",
          "aws_secret_access_key" : "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
          "region"                : "us-east-1"
      }

   Where to get these keys:
      AWS Console → IAM → Users → Your User → Security credentials
      → Create access key → Copy both keys


STEP 6 — Set up budgets.csv
   This file contains the list of budgets to create.
   Format (must have these exact column names):

      name,amount
      YOUR-HULU-PLAN,0.01
      NETFLIX-NOTICE,0.01
      YOUR-AMAZON-PRIME,0.01

   Rules:
   - First row must be:  name,amount
   - amount is in USD — use 0.01 for alert-only budgets
   - One budget per row
   - No spaces in budget names (use hyphens instead)


STEP 7 — Set up emails.csv
   This file contains email addresses to receive budget alerts.
   Format (must have these exact column names):

      emails,report
      john@example.com,
      sarah@example.com,
      team@company.com,

   Rules:
   - First row must be:  emails,report
   - Leave the report column empty — script fills it automatically
   - One email per row
   - Already-sent emails (report=sent) are skipped automatically


STEP 8 — Run the script
   Open CMD in your working folder and run:

      python main.py

   The script will:
   1. Check for updates automatically
   2. Detect all account folders
   3. Ask how many accounts to run at once
   4. Check each account for errors
   5. Show which accounts are ready
   6. Ask for final confirmation
   7. Create budgets and assign emails


═══════════════════════════════════════════════════════════════
FILE REQUIREMENTS SUMMARY
═══════════════════════════════════════════════════════════════

   File                    Required?   Location
   ──────────────────────────────────────────────────────────
   main.py                 YES         Root working folder
   aws_credentials.json    YES         Inside each account folder
   budgets.csv             YES         Inside each account folder
   emails.csv              YES         Inside each account folder

   DO NOT put aws_credentials.json in the root folder —
   it must be inside each account's own folder.


═══════════════════════════════════════════════════════════════
COMMON ERRORS AND FIXES
═══════════════════════════════════════════════════════════════

   ERROR: boto3 not installed
   FIX:   Run:  pip install boto3

   ERROR: aws_credentials.json not found
   FIX:   Make sure the file is inside the account folder,
          not in the root working folder.

   ERROR: AWS credentials are invalid
   FIX:   Check your access key and secret key for typos.
          Generate new keys from AWS Console if needed.

   ERROR: Not enough emails in emails.csv
   FIX:   Add more email addresses to emails.csv.
          Formula: budgets × 5 × 10 = emails needed
          Example: 1000 budgets need 50,000 emails

   ERROR: budgets.csv is empty
   FIX:   Make sure budgets.csv has at least one row
          with a name and amount.

   ERROR: Script says UPDATE REQUIRED
   FIX:   Press ENTER — script updates itself automatically.
          Re-run after update completes.


═══════════════════════════════════════════════════════════════
NEED HELP?
═══════════════════════════════════════════════════════════════

   Contact your team lead before making any changes
   to aws_credentials.json or any script files.

   DO NOT share your aws_credentials.json with anyone.
   DO NOT upload aws_credentials.json to GitHub.

═══════════════════════════════════════════════════════════════
