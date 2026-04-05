# create_budgets.py
# Single phase creation (budget + notifications in one API call)
# + Nuclear-grade CSV reader (handles ANY encoding, ANY delimiter, regex fallback)
# + Pre-flight checks with animated warnings
# + SAB THIK HAI + Press ENTER to send confirmation
# + Auto-update system

import csv
import json
import os
import re
import time
import threading
import sys
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

# ── CTRL+C HANDLER — stops all threads immediately ────────────────────────────
import signal as _signal

_SHUTDOWN = False   # global flag — threads check this and stop

def _handle_ctrl_c(sig, frame):
    global _SHUTDOWN
    _SHUTDOWN = True
    print("\n\n  Ctrl+C detected — stopping after current batch...")
    print("  Please wait a moment for threads to finish cleanly.")

# Register handler immediately
_signal.signal(_signal.SIGINT, _handle_ctrl_c)

# boto3 checked at runtime to give friendly error
try:
    import boto3
except ImportError:
    boto3 = None

# ══════════════════════════════════════════════════════════════
# AUTO-UPDATE CONFIG
# ══════════════════════════════════════════════════════════════
VERSION     = "1.0.3"   # bump this number when you push an update

# ── Set these to your GitHub raw file URLs ─────────────────────
# Step 1: Create a GitHub repo (github.com → New repository)
# Step 2: Upload create_budgets.py and version.txt there
# Step 3: Replace YOUR_USERNAME and YOUR_REPO below
VERSION_URL = "https://raw.githubusercontent.com/CloudPulse-cloud/nayak/refs/heads/main/version.txt"
SCRIPT_URL  = "https://raw.githubusercontent.com/CloudPulse-cloud/nayak/refs/heads/main/main.py"
# ──────────────────────────────────────────────────────────────

# ── FIXED SETTINGS ────────────────────────────────────────────────────────────
ALERTS_PER_BUDGET = 5
EMAILS_PER_ALERT  = 10
THRESHOLDS        = [1, 2, 3, 50, 100]
MAX_RETRY_ROUNDS  = 5

# ── SPEED PROFILES ────────────────────────────────────────────────────────────
PROFILES = {
    "safe": {
        "PARALLEL_THREADS":     10,     # reduced from 15
        "ACCOUNTS_IN_PARALLEL": 1,
        "RATE_LIMIT_PER_SEC":   10,     # reduced from 15
        "RETRY_DELAY":          10,
        "BATCH_SIZE":           500,
        "BATCH_PAUSE":          2,
    },
    "fast": {
        "PARALLEL_THREADS":     20,
        "ACCOUNTS_IN_PARALLEL": 3,
        "RATE_LIMIT_PER_SEC":   18,
        "RETRY_DELAY":          3,
        "BATCH_SIZE":           999999,
        "BATCH_PAUSE":          0,
    }
}

# ── ANSI COLORS ───────────────────────────────────────────────────────────────
os.system("")
RED    = "\033[91m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
CYAN   = "\033[96m"
WHITE  = "\033[97m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

W = 66

# ── EMBEDDED TONES (base64) ───────────────────────────────────────────────────
# Run embed_tones.py once to inject your audio files here.
# After embedding, this script is self-contained — works on any machine.
SUCCESS_TONE_B64 = ""   # loaded from tones.json automatically
WARNING_TONE_B64 = ""   # loaded from tones.json automatically

# ── AUTO-LOAD TONES FROM tones.json ──────────────────────────────────────────
# Run tone.py ONCE to create tones.json — never needed again after that.
# tones.json sits next to main.py permanently and survives all script updates.
def _load_tones():
    global SUCCESS_TONE_B64, WARNING_TONE_B64
    try:
        _path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tones.json")
        if os.path.exists(_path):
            with open(_path, "r") as _f:
                _d = json.load(_f)
            SUCCESS_TONE_B64 = _d.get("success", "")
            WARNING_TONE_B64 = _d.get("warning", "")
    except Exception:
        pass

_load_tones()


def check_for_updates():
    """
    Checks GitHub for a newer version on every run.
    If update found → shows big warning → user presses Enter → auto-downloads.
    Tones survive update because tones.json is preserved separately.
    Silently skips if no internet or GitHub not configured yet.
    """
    if "YOUR_USERNAME" in VERSION_URL:
        return   # GitHub not configured yet — skip silently

    try:
        import urllib.request
        import shutil

        # Fetch latest version number from GitHub (5 sec timeout)
        with urllib.request.urlopen(VERSION_URL, timeout=5) as r:
            latest = r.read().decode().strip()

    except Exception:
        return   # No internet or wrong URL — skip silently, never block script

    if latest == VERSION:
        return   # Already up to date — continue normally

    # ── UPDATE AVAILABLE — force update ────────────────────────
    TW = shutil.get_terminal_size(fallback=(100, 40)).columns - 2
    TW = max(TW, 60)

    def top(c):  print(f"{c}╔{'═'*(TW-2)}╗{RESET}")
    def mid(c):  print(f"{c}╠{'═'*(TW-2)}╣{RESET}")
    def bot(c):  print(f"{c}╚{'═'*(TW-2)}╝{RESET}")
    def emp(c):  print(f"{c}║{' '*(TW-2)}║{RESET}")
    def row(t, c, bold=False):
        inner  = TW - 4
        padded = t.center(inner)
        b      = BOLD if bold else ""
        print(f"{c}║  {b}{padded}{RESET}{c}  ║{RESET}")

    play_tone(WARNING_TONE_B64, wait_for_exit=False)

    print()
    for i in range(8):
        c = YELLOW if i % 2 == 0 else RED
        print(f"\r{c}{'█' * TW}{RESET}", end="", flush=True)
        time.sleep(0.1)
    print(); print()

    top(YELLOW); emp(YELLOW)
    row("U P D A T E   R E Q U I R E D  !  !  !", YELLOW, bold=True)
    emp(YELLOW)
    row(f"Your version    :  {VERSION}", YELLOW)
    row(f"Latest version  :  {latest}", YELLOW)
    emp(YELLOW)
    row("Script cannot run until updated.", YELLOW, bold=True)
    emp(YELLOW); bot(YELLOW)

    print()
    top(CYAN); emp(CYAN)
    row("Your team lead pushed a new update.", CYAN)
    row("Press ENTER to download and install automatically.", CYAN)
    row("Takes about 5 seconds. Tones are preserved.", CYAN)
    emp(CYAN); bot(CYAN)

    print()
    try:
        input(f"  {BOLD}{CYAN}>>> Press ENTER to update now : {RESET}")
    except KeyboardInterrupt:
        print(f"\n  {RED}Update cancelled. Script cannot run without updating.{RESET}\n")
        sys.exit(1)

    # ── Download new script ────────────────────────────────────
    print(f"\n  {CYAN}Downloading version {latest}...{RESET}", end="", flush=True)
    try:
        import urllib.request
        with urllib.request.urlopen(SCRIPT_URL, timeout=30) as r:
            new_content = r.read().decode("utf-8")
        print(f"  {GREEN}Done{RESET}")
    except Exception as e:
        print(f"\n  {RED}Download failed: {e}{RESET}")
        print(f"  {YELLOW}Check your internet connection and try again.{RESET}\n")
        sys.exit(1)

    # ── Save new script over current file ─────────────────────
    print(f"  {CYAN}Installing update...{RESET}", end="", flush=True)
    try:
        script_path = os.path.abspath(__file__)
        with open(script_path, "w", encoding="utf-8") as f:
            f.write(new_content)
        print(f"  {GREEN}Done{RESET}")
    except Exception as e:
        print(f"\n  {RED}Could not save update: {e}{RESET}")
        print(f"  {YELLOW}Try running as administrator.{RESET}\n")
        sys.exit(1)

    # ── Success ────────────────────────────────────────────────
    play_tone(SUCCESS_TONE_B64)
    print()
    top(GREEN); emp(GREEN)
    row(f"UPDATED SUCCESSFULLY  —  Version {latest}", GREEN, bold=True)
    emp(GREEN)
    row("Please re-run the script to use the new version.", GREEN)
    emp(GREEN); bot(GREEN)
    print()
    sys.exit(0)


def play_tone(b64_data, wait_for_exit=False):
    """
    CONFIRMED WORKING on this machine: winmm.dll MCI
    Fallback: PowerShell WMP COM with CREATE_NO_WINDOW
    Plays tone in background thread simultaneously with animation.
    """
    if not b64_data:
        return
    import threading, os

    def _play():
        tmp = None
        try:
            import base64, ctypes, tempfile, subprocess

            # Decode base64 audio
            audio_bytes = base64.b64decode(b64_data)

            # Detect extension from file header
            if audio_bytes[:4] == b'RIFF':
                ext = '.wav'
            elif audio_bytes[:3] == b'ID3' or audio_bytes[:2] in (b'\xff\xfb', b'\xff\xf3', b'\xff\xf2'):
                ext = '.mp3'
            else:
                ext = '.mp3'   # default to mp3

            # Write to temp file using mkstemp (most reliable on Windows)
            fd, tmp = tempfile.mkstemp(suffix=ext)
            os.close(fd)
            with open(tmp, 'wb') as f:
                f.write(audio_bytes)

            played = False

            # ══════════════════════════════════════════════════
            # METHOD 1: winmm.dll MCI — CONFIRMED WORKING
            # No new process, no window, plays MP3/WAV natively
            # ══════════════════════════════════════════════════
            if os.name == 'nt':
                try:
                    winmm = ctypes.WinDLL('winmm')
                    buf   = ctypes.create_unicode_buffer(512)
                    alias = 'budgetsound'
                    path  = tmp   # mkstemp gives Windows-style path already

                    r = winmm.mciSendStringW(
                        f'open "{path}" alias {alias}',
                        buf, 512, 0
                    )
                    if r == 0:
                        winmm.mciSendStringW(f'play {alias} wait', buf, 512, 0)
                        winmm.mciSendStringW(f'close {alias}', buf, 512, 0)
                        played = True
                    else:
                        # Try explicit mpegvideo type for MP3
                        alias2 = 'budgetsound2'
                        r2 = winmm.mciSendStringW(
                            f'open "{path}" type mpegvideo alias {alias2}',
                            buf, 512, 0
                        )
                        if r2 == 0:
                            winmm.mciSendStringW(f'play {alias2} wait', buf, 512, 0)
                            winmm.mciSendStringW(f'close {alias2}', buf, 512, 0)
                            played = True
                except Exception:
                    pass

            # ══════════════════════════════════════════════════
            # METHOD 2: PowerShell WMP COM — CONFIRMED WORKING
            # CREATE_NO_WINDOW = no terminal minimize
            # ══════════════════════════════════════════════════
            if not played and os.name == 'nt':
                try:
                    CREATE_NO_WINDOW = 0x08000000
                    path_ps = tmp.replace('\\', '/')
                    ps = (
                        f"$w=New-Object -ComObject WMPlayer.OCX.7;"
                        f"$w.URL='{path_ps}';"
                        f"$w.settings.autoStart=$true;"
                        f"$w.controls.play();"
                        f"Start-Sleep -Seconds 10;"
                        f"$w.controls.stop();"
                    )
                    subprocess.run(
                        ['powershell', '-WindowStyle', 'Hidden',
                         '-NonInteractive', '-Command', ps],
                        timeout=15,
                        creationflags=CREATE_NO_WINDOW,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        stdin=subprocess.DEVNULL
                    )
                    played = True
                except Exception:
                    pass

            # ══════════════════════════════════════════════════
            # macOS / Linux fallbacks
            # ══════════════════════════════════════════════════
            if not played:
                for cmd in [
                    ['afplay', tmp],
                    ['aplay', '-q', tmp],
                    ['ffplay', '-nodisp', '-autoexit', '-loglevel', 'quiet', tmp],
                    ['mpg123', '-q', tmp],
                ]:
                    try:
                        subprocess.run(cmd, timeout=15,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        played = True
                        break
                    except Exception:
                        continue

        except Exception:
            pass
        finally:
            try:
                import time as _t
                _t.sleep(0.5)
                if tmp and os.path.exists(tmp):
                    os.unlink(tmp)
            except Exception:
                pass

    # Non-daemon: Python waits for audio before process exits (for warnings)
    # Daemon:     plays in background, okay to cut off if script ends
    t = threading.Thread(target=_play, daemon=not wait_for_exit)
    t.start()


# ── EMAIL REGEX ───────────────────────────────────────────────────────────────
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


# ══════════════════════════════════════════════════════════════
# NUCLEAR-GRADE CSV READER
# Never fails — uses regex email extraction as last resort
# ══════════════════════════════════════════════════════════════
def clean_key(k):
    return re.sub(r'[\x00\r\n\ufeff"\'`\t]', '', k).strip().lower()

def clean_val(v):
    return re.sub(r'[\x00\r\n"\'`]', '', v).strip()

def try_decode(raw_bytes):
    """
    Tries every known encoding including UTF-16 variants.
    Returns (text, encoding_name) or (None, None).
    """
    encodings = [
        ("utf-8-sig",   {}),
        ("utf-8",       {"errors": "replace"}),
        ("utf-16",      {}),        # auto-detects LE/BE from BOM
        ("utf-16-le",   {}),        # UTF-16 Little Endian no BOM
        ("utf-16-be",   {}),        # UTF-16 Big Endian no BOM
        ("latin-1",     {}),
        ("cp1252",      {}),
        ("iso-8859-1",  {}),
        ("cp1250",      {}),        # Eastern European
        ("cp1251",      {}),        # Cyrillic
        ("ascii",       {"errors": "replace"}),
    ]
    for enc, kwargs in encodings:
        try:
            text = raw_bytes.decode(enc, **kwargs)
            if text and len(text.strip()) > 0:
                return text, enc
        except Exception:
            continue
    return None, None


def smart_open_csv(csv_file):
    """
    Attempts to read CSV using:
      Pass 1 — Multiple encodings × multiple delimiters
      Pass 2 — Regex email extraction (nuclear fallback)

    Returns (list_of_dicts, encoding_used)
    where each dict has at least {'emails': '...' , 'report': '...'}
    """
    with open(csv_file, "rb") as f:
        raw = f.read()

    if not raw.strip():
        return [], None

    text, encoding = try_decode(raw)
    if text is None:
        # Last resort: decode ignoring errors
        text     = raw.decode("utf-8", errors="ignore")
        encoding = "utf-8-ignore"

    # Normalize line endings
    text = text.replace('\r\n', '\n').replace('\r', '\n').lstrip('\ufeff')

    # ── Pass 1: Try CSV parsing with different delimiters ─────
    delimiters = [',', ';', '\t', '|', ':']

    for delim in delimiters:
        try:
            reader = csv.DictReader(io.StringIO(text), delimiter=delim)
            rows   = list(reader)
            if not rows:
                continue

            cleaned = []
            for row in rows:
                cr = {}
                for k, v in row.items():
                    if k is None:
                        continue
                    ck = clean_key(k)
                    cv = clean_val(v) if v else ""
                    if ck:
                        cr[ck] = cv
                if cr:
                    cleaned.append(cr)

            # Accept if we found at least one valid row with data
            if cleaned:
                return cleaned, f"{encoding} / delim='{delim}'"

        except Exception:
            continue

    # ── Pass 2: Nuclear fallback — extract all emails via regex ──
    print(f"  {YELLOW}CSV parsing failed — using regex email extraction fallback{RESET}")
    found_emails = EMAIL_REGEX.findall(text)
    unique_emails = list(dict.fromkeys(found_emails))   # deduplicate, preserve order

    if unique_emails:
        rows = [{"emails": e, "report": ""} for e in unique_emails]
        print(f"  {YELLOW}Regex fallback extracted {len(rows)} emails{RESET}")
        return rows, f"{encoding} / regex-fallback"

    return [], None


def find_email_column(row):
    """Fuzzy match — accepts any key containing 'mail' or common variants."""
    candidates = ["emails", "email", "e-mail", "e_mail",
                  "mail", "emailaddress", "email address",
                  "email_address", "address"]
    for key in row.keys():
        k = key.lower().strip()
        if k in candidates or "mail" in k:
            return key
    # Last resort: find any column whose value looks like an email
    for key, val in row.items():
        if val and EMAIL_REGEX.match(val.strip()):
            return key
    return None


def find_report_column(row):
    for key in row.keys():
        k = key.lower().strip()
        if "report" in k or "status" in k or "sent" in k:
            return key
    return None


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


# ══════════════════════════════════════════════════════════════
# ANIMATED WARNING
# ══════════════════════════════════════════════════════════════
def show_warning(title_lines, detail_lines, fix_line):
    def solid(c): print(f"{c}{'█' * W}{RESET}")
    def blank(c): print(f"{c}██{' ' * (W - 4)}██{RESET}")
    def line(t, c):
        padded = t.center(W - 4)
        print(f"{c}██  {padded}  ██{RESET}")

    print()
    play_tone(WARNING_TONE_B64, wait_for_exit=True)   # non-daemon: plays WITH animation, Python waits before exit
    for i in range(10):
        c = RED if i % 2 == 0 else YELLOW
        print(f"\r{c}{'█' * W}{RESET}", end="", flush=True)
        time.sleep(0.15)
    print()

    solid(RED); solid(RED); blank(RED)
    for t in title_lines:
        line(t, RED)
    blank(RED); solid(RED); solid(RED)

    print()
    for i in range(6):
        c = YELLOW if i % 2 == 0 else RED
        print(f"\r{c}{'▓' * W}{RESET}", end="", flush=True)
        time.sleep(0.18)
    print(); print()

    solid(YELLOW); blank(YELLOW)
    line("SCRIPT STOPPED — DETAILS BELOW", YELLOW)
    blank(YELLOW); solid(YELLOW)

    print()
    print(f"{RED}{'─' * W}{RESET}")
    for dl in detail_lines:
        print(f"  {WHITE}{dl}{RESET}")
    print(f"{RED}{'─' * W}{RESET}")
    print()

    print(f"{YELLOW}  Script will exit in...{RESET}")
    print()
    for i in range(5, 0, -1):
        c = RED if i <= 2 else YELLOW
        print(f"\r  {c}{BOLD}  {i}...{RESET}   ", end="", flush=True)
        time.sleep(1)
    print(); print()

    solid(RED); blank(RED)
    line(fix_line, RED)
    blank(RED); solid(RED)
    print()
    sys.exit(1)


def show_no_budgets_warning():
    show_warning(
        title_lines=[
            "██████╗ ██╗   ██╗██████╗  ██████╗ ███████╗████████╗███████╗",
            "██╔══██╗██║   ██║██╔══██╗██╔════╝ ██╔════╝╚══██╔══╝██╔════╝",
            "██████╔╝██║   ██║██║  ██║██║  ███╗█████╗     ██║   ███████╗ ",
            "██╔══██╗██║   ██║██║  ██║██║   ██║██╔══╝     ██║   ╚════██║ ",
            "██████╔╝╚██████╔╝██████╔╝╚██████╔╝███████╗   ██║   ███████║ ",
            "╚═════╝  ╚═════╝ ╚═════╝  ╚═════╝ ╚══════╝   ╚═╝   ╚══════╝ ",
            "",
            "NO BUDGETS FOUND IN budgets.csv !",
        ],
        detail_lines=[
            "budgets.csv is empty or has no valid rows.",
            "",
            "Make sure budgets.csv has columns:  name, amount",
            "and contains at least one valid budget entry.",
        ],
        fix_line="  ADD BUDGETS TO budgets.csv AND RE-RUN  "
    )


def show_no_emails_warning(available, needed, budgets_count):
    show_warning(
        title_lines=[
            "███╗   ██╗ ██████╗     ███████╗███╗   ███╗ █████╗ ██╗██╗     ",
            "████╗  ██║██╔═══██╗    ██╔════╝████╗ ████║██╔══██╗██║██║     ",
            "██╔██╗ ██║██║   ██║    █████╗  ██╔████╔██║███████║██║██║     ",
            "██║╚██╗██║██║   ██║    ██╔══╝  ██║╚██╔╝██║██╔══██║██║██║     ",
            "██║ ╚████║╚██████╔╝    ███████╗██║ ╚═╝ ██║██║  ██║██║███████╗",
            "╚═╝  ╚═══╝ ╚═════╝     ╚══════╝╚═╝     ╚═╝╚═╝  ╚═╝╚═╝╚══════╝",
            "",
            "NOT ENOUGH EMAILS IN emails.csv !",
        ],
        detail_lines=[
            f"Budgets to create        : {budgets_count:,}",
            f"Emails needed            : {needed:,}",
            f"Unsent emails available  : {available:,}",
            f"Emails short by          : {needed - available:,}",
            "",
            f"Formula: {budgets_count:,} budgets x {ALERTS_PER_BUDGET} thresholds"
            f" x {EMAILS_PER_ALERT} emails = {needed:,}",
        ],
        fix_line="  ADD MORE EMAILS TO emails.csv AND RE-RUN  "
    )

def show_error(title, details, fix=None):
    """
    Universal error handler — shows big warning, plays tone, exits.
    Call this anywhere an unrecoverable error occurs.
    """
    import shutil
    TW = shutil.get_terminal_size(fallback=(100, 40)).columns - 2
    TW = max(TW, 60)

    def solid(c): print(f"{c}{'█' * TW}{RESET}")
    def blank(c): print(f"{c}║{' ' * (TW-2)}║{RESET}")
    def top(c):   print(f"{c}╔{'═' * (TW-2)}╗{RESET}")
    def bot(c):   print(f"{c}╚{'═' * (TW-2)}╝{RESET}")
    def row(t, c, bold=False):
        inner  = TW - 4
        padded = t.center(inner)
        b      = BOLD if bold else ""
        print(f"{c}║  {b}{padded}{RESET}{c}  ║{RESET}")
    def row_left(t, c):
        inner  = TW - 4
        padded = t.ljust(inner)
        print(f"{c}║  {padded}  ║{RESET}")

    play_tone(WARNING_TONE_B64, wait_for_exit=True)

    print()
    for i in range(8):
        c = RED if i % 2 == 0 else YELLOW
        print(f"\r{c}{'█' * TW}{RESET}", end="", flush=True)
        time.sleep(0.12)
    print(); print()

    # Big ERROR box
    top(RED); blank(RED)
    row("E  R  R  O  R   !  !  !", RED, bold=True)
    blank(RED)
    row(title, RED, bold=True)
    blank(RED); bot(RED)

    print()
    for i in range(4):
        c = YELLOW if i % 2 == 0 else RED
        print(f"\r{c}{'▓' * TW}{RESET}", end="", flush=True)
        time.sleep(0.15)
    print(); print()

    # Details box
    top(YELLOW); blank(YELLOW)
    row("DETAILS", YELLOW, bold=True)
    blank(YELLOW)
    for line in details:
        row_left(f"  {line}", YELLOW)
    blank(YELLOW)
    if fix:
        row("HOW TO FIX", YELLOW, bold=True)
        blank(YELLOW)
        if isinstance(fix, str):
            fix = [fix]
        for line in fix:
            row_left(f"  ➤  {line}", YELLOW)
        blank(YELLOW)
    bot(YELLOW)

    # Countdown
    print()
    print(f"  {YELLOW}Script will exit in...{RESET}")
    for i in range(5, 0, -1):
        c = RED if i <= 2 else YELLOW
        print(f"\r  {c}{BOLD}  {i}...{RESET}   ", end="", flush=True)
        time.sleep(1)
    print(); print()
    sys.exit(1)




# ══════════════════════════════════════════════════════════════
# SAB THIK HAI — MULTICOLOR + RESPONSIVE + PRESS ENTER TO SEND
# ══════════════════════════════════════════════════════════════
def show_sab_thik_hai(budgets_count, emails_count, needed):
    import random, shutil

    MAGENTA = "\033[95m"
    BLUE    = "\033[94m"
    LETTER_COLORS = [RED, YELLOW, GREEN, CYAN, MAGENTA, WHITE, BLUE]

    TW = shutil.get_terminal_size(fallback=(120, 40)).columns - 2
    TW = max(TW, 60)

    BLOCK = {
        'A': [" ### ","#   #","#   #","#####","#   #","#   #","#   #"],
        'B': ["#### ","#   #","#   #","#### ","#   #","#   #","#### "],
        'C': [" ####","#    ","#    ","#    ","#    ","#    "," ####"],
        'D': ["#### ","#   #","#   #","#   #","#   #","#   #","#### "],
        'E': ["#####","#    ","#    ","#### ","#    ","#    ","#####"],
        'F': ["#####","#    ","#    ","#### ","#    ","#    ","#    "],
        'G': [" ####","#    ","#    ","# ###","#   #","#   #"," ####"],
        'H': ["#   #","#   #","#   #","#####","#   #","#   #","#   #"],
        'I': ["#####","  #  ","  #  ","  #  ","  #  ","  #  ","#####"],
        'J': [" ####","   # ","   # ","   # ","#  # ","#  # "," ### "],
        'K': ["#   #","#  # ","# #  ","##   ","# #  ","#  # ","#   #"],
        'L': ["#    ","#    ","#    ","#    ","#    ","#    ","#####"],
        'M': ["#   #","## ##","# # #","#   #","#   #","#   #","#   #"],
        'N': ["#   #","##  #","# # #","#  ##","#   #","#   #","#   #"],
        'O': [" ### ","#   #","#   #","#   #","#   #","#   #"," ### "],
        'P': ["#### ","#   #","#   #","#### ","#    ","#    ","#    "],
        'R': ["#### ","#   #","#   #","#### ","# #  ","#  # ","#   #"],
        'S': [" ####","#    ","#    "," ### ","    #","    #","#### "],
        'T': ["#####","  #  ","  #  ","  #  ","  #  ","  #  ","  #  "],
        'U': ["#   #","#   #","#   #","#   #","#   #","#   #"," ### "],
        'V': ["#   #","#   #","#   #","#   #"," # # "," # # ","  #  "],
        'W': ["#   #","#   #","#   #","# # #","## ##","#   #","#   #"],
        'X': ["#   #"," # # ","  #  ","  #  ","  #  "," # # ","#   #"],
        'Y': ["#   #"," # # ","  #  ","  #  ","  #  ","  #  ","  #  "],
        'Z': ["#####","   # ","  #  "," #   ","#    ","#    ","#####"],
        '!': ["  #  ","  #  ","  #  ","  #  ","  #  ","     ","  #  "],
        ' ': ["     ","     ","     ","     ","     ","     ","     "],
        '-': ["     ","     ","     ","#####","     ","     ","     "],
    }

    def vis_width(text):
        # Each letter: 5 cols + 2 gap = 7 visible chars
        return sum(5 + 2 for c in text)

    def render_multicolor(text):
        """Both lines — each letter a different color."""
        rows  = [""] * 7
        vis_w = vis_width(text)
        ci    = 0
        for ch in text.upper():
            pat = BLOCK.get(ch, BLOCK[' '])
            col = LETTER_COLORS[ci % len(LETTER_COLORS)] if ch != ' ' else ""
            if ch != ' ': ci += 1
            for i in range(7):
                seg = ""
                for px in pat[i]:
                    seg += (col + BOLD + "█" + RESET) if px == "#" else " "
                rows[i] += seg + "  "
        return rows, vis_w

    MESSAGES = [
        {"l1": "SAB THIK",  "l2": "HAI !",    "sub": "All checks passed — Ready to roll!"},
        {"l1": "AAJ MAZA",  "l2": "AAYEGA !", "sub": "Today is going to be awesome!"},
        {"l1": "CHALIYE",   "l2": "SHURU !",  "sub": "Let's get this started right now!"},
        {"l1": "FULL SEND", "l2": "LETS GO !", "sub": "Budgets and emails going out NOW!"},
        {"l1": "BUDGET",    "l2": "BANEGA !",  "sub": "Budgets will be made, emails will fly!"},
    ]

    msg     = random.choice(MESSAGES)
    r1, vw1 = render_multicolor(msg["l1"])
    r2, vw2 = render_multicolor(msg["l2"])   # both lines multicolor
    BW      = TW                              # never exceed terminal width

    def top(c):  print(f"{c}╔{'═' * (BW-2)}╗{RESET}")
    def mid(c):  print(f"{c}╠{'═' * (BW-2)}╣{RESET}")
    def bot(c):  print(f"{c}╚{'═' * (BW-2)}╝{RESET}")
    def emp(c):  print(f"{c}║{' ' * (BW-2)}║{RESET}")
    def txt(t, c, bold=False):
        inner  = BW - 4
        padded = t.center(inner)
        b      = BOLD if bold else ""
        print(f"{c}║  {b}{padded}{RESET}{c}  ║{RESET}")
    def blk(colored_row, vis_w, c):
        inner     = BW - 2
        left_pad  = max(0, (inner - vis_w) // 2)
        right_pad = max(0, inner - vis_w - left_pad)
        print(f"{c}║{' ' * left_pad}{colored_row}{c}{' ' * right_pad}║{RESET}")

    print()
    play_tone(SUCCESS_TONE_B64)   # daemon: plays WITH animation simultaneously
    for i in range(10):
        c = GREEN if i % 2 == 0 else CYAN
        print(f"\r{c}{'█' * BW}{RESET}", end="", flush=True)
        time.sleep(0.07)
    print(); print()

    top(GREEN); emp(GREEN); emp(GREEN)
    for line in r1: blk(line, vw1, GREEN)
    emp(GREEN)
    for line in r2: blk(line, vw2, GREEN)
    emp(GREEN); emp(GREEN)
    txt(f"—   {msg['sub']}   —", GREEN)
    emp(GREEN); bot(GREEN)

    print()
    for i in range(6):
        c = CYAN if i % 2 == 0 else GREEN
        print(f"\r{c}{'▓' * BW}{RESET}", end="", flush=True)
        time.sleep(0.08)
    print(); print()

    top(GREEN)
    txt("P R E - F L I G H T   C H E C K   S U M M A R Y", GREEN, bold=True)
    mid(GREEN); emp(GREEN)
    txt(f"  ✔   Budgets ready to create   :   {budgets_count:,}", GREEN)
    txt(f"  ✔   Unsent emails available   :   {emails_count:,}", GREEN)
    txt(f"  ✔   Emails needed             :   {needed:,}", GREEN)
    txt(f"  ✔   Emails surplus            :   {emails_count - needed:,}", GREEN)
    emp(GREEN); bot(GREEN)

    print()
    top(CYAN); emp(CYAN)
    txt("F I N A L   C O N F I R M A T I O N", CYAN, bold=True)
    emp(CYAN)
    txt(f"Create  {budgets_count:,}  budgets   +   assign  {needed:,}  emails", CYAN)
    emp(CYAN)
    txt("Press  ENTER  to start                         Ctrl+C  to cancel", CYAN)
    emp(CYAN); bot(CYAN)

    print()
    try:
        input(f"  {BOLD}{GREEN}>>> Press ENTER to send : {RESET}")
    except KeyboardInterrupt:
        print(f"\n\n  {RED}Cancelled by user.{RESET}\n")
        sys.exit(0)

    print()
    print(f"  {BOLD}{GREEN}Starting now...{RESET}")
    print()


# ── GLOBAL RATE LIMITER ───────────────────────────────────────────────────────
class RateLimiter:
    def __init__(self, calls_per_sec):
        self.calls_per_sec = calls_per_sec
        self.lock          = threading.Lock()
        self.tokens        = calls_per_sec
        self.last_refill   = time.time()

    def acquire(self):
        while True:
            with self.lock:
                now          = time.time()
                elapsed      = now - self.last_refill
                self.tokens  = min(self.calls_per_sec, self.tokens + elapsed * self.calls_per_sec)
                self.last_refill = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
            time.sleep(0.05)


# ── ASK MODE ─────────────────────────────────────────────────────────────────
def ask_mode():
    print()
    divider()
    print("  Select Speed Mode:\n")
    print("  1  ->  SAFE MODE  (~45 min for 20k budgets, account-safe)")
    print("  2  ->  FAST MODE  (~20 min for 20k budgets, higher risk)")
    print()
    while True:
        choice = input("  Enter 1 or 2: ").strip()
        if choice == "1":
            print("  Selected: SAFE MODE")
            return "safe"
        elif choice == "2":
            print("  Selected: FAST MODE")
            return "fast"
        else:
            print("  Please enter 1 or 2")


def ask_parallel_mode(account_folders):
    """
    Shows all available accounts and asks how many to run in parallel.
    Returns the number of accounts to run simultaneously.
    """
    total = len(account_folders)

    # Single account — no question needed
    if total == 1:
        return 1

    print()
    divider()
    print(f"  Accounts found : {GREEN}{total}{RESET}\n")

    for i, folder in enumerate(account_folders, 1):
        name = os.path.basename(folder)
        print(f"    {i:>2}.  {name}")

    print()
    print(f"  How many accounts to run at the same time?\n")
    print(f"  1  ->  ONE AT A TIME      (safe, ~{total * 42} min total)")
    print(f"  2  ->  ALL {total} AT ONCE      "
          f"(fastest, ~42 min total — each account has own AWS limit)")
    print(f"  3  ->  CUSTOM number")
    print()

    while True:
        choice = input("  Enter 1, 2 or 3: ").strip()

        if choice == "1":
            print(f"  Selected: ONE AT A TIME — will run {total} accounts sequentially")
            return 1

        elif choice == "2":
            print(f"  Selected: ALL {total} AT ONCE — running all accounts in parallel")
            return total

        elif choice == "3":
            while True:
                try:
                    n = int(input(f"  Enter number (1 to {total}): ").strip())
                    if 1 <= n <= total:
                        est = max(1, (total // n)) * 42
                        print(f"  Selected: {n} accounts at a time — estimated ~{est} min total")
                        return n
                    else:
                        print(f"  Please enter a number between 1 and {total}")
                except ValueError:
                    print(f"  Please enter a valid number")
        else:
            print("  Please enter 1, 2 or 3")


# ── DETECT ACCOUNT MODE ───────────────────────────────────────────────────────
def detect_account_mode(base_dir):
    local_creds = os.path.join(base_dir, "aws_credentials.json")
    if os.path.exists(local_creds):
        print("  Single account mode detected")
        return "single"
    subfolders = [
        os.path.join(base_dir, d)
        for d in sorted(os.listdir(base_dir))
        if os.path.isdir(os.path.join(base_dir, d))
        and os.path.exists(os.path.join(base_dir, d, "aws_credentials.json"))
    ]
    if subfolders:
        print(f"  Multi account mode detected — {len(subfolders)} account(s) found")
        return "multi"
    raise FileNotFoundError("No aws_credentials.json found.")


def get_account_folders(base_dir, account_mode):
    if account_mode == "single":
        return [base_dir]
    return [
        os.path.join(base_dir, d)
        for d in sorted(os.listdir(base_dir))
        if os.path.isdir(os.path.join(base_dir, d))
        and os.path.exists(os.path.join(base_dir, d, "aws_credentials.json"))
    ]




# ── LOAD CREDENTIALS ──────────────────────────────────────────────────────────
def load_credentials(folder):
    creds_path = os.path.join(folder, "aws_credentials.json")
    if not os.path.exists(creds_path):
        show_error(
            "aws_credentials.json NOT FOUND",
            [f"Expected location: {creds_path}"],
            ["Create aws_credentials.json in your account folder",
             "It must contain: aws_access_key_id, aws_secret_access_key, region"]
        )
    try:
        with open(creds_path, "r") as f:
            creds = json.load(f)
    except json.JSONDecodeError as e:
        show_error(
            "aws_credentials.json IS INVALID JSON",
            [f"File: {creds_path}", f"JSON Error: {e}"],
            ["Fix the JSON syntax in aws_credentials.json",
             "Use a JSON validator at jsonlint.com"]
        )
    missing = [k for k in ["aws_access_key_id","aws_secret_access_key","region"] if k not in creds]
    if missing:
        show_error(
            "aws_credentials.json IS MISSING KEYS",
            [f"File: {creds_path}",
             f"Missing keys: {missing}"],
            ['Add the missing keys to aws_credentials.json',
             'Required: aws_access_key_id, aws_secret_access_key, region']
        )
    return creds


def get_account_id(session):
    try:
        return session.client("sts").get_caller_identity()["Account"]
    except Exception as e:
        err = str(e)
        if "InvalidClientTokenId" in err or "AuthFailure" in err:
            show_error(
                "AWS CREDENTIALS ARE INVALID",
                ["The Access Key ID or Secret Key is wrong or expired",
                 f"Error: {err[:120]}"],
                ["Check your aws_credentials.json for typos",
                 "Generate new credentials from AWS Console → IAM → Users → Security credentials"]
            )
        elif "ExpiredToken" in err:
            show_error(
                "AWS CREDENTIALS HAVE EXPIRED",
                [f"Error: {err[:120]}"],
                ["Generate new access keys from AWS Console → IAM"]
            )
        else:
            show_error(
                "AWS CONNECTION FAILED",
                [f"Error: {err[:120]}"],
                ["Check your internet connection",
                 "Verify the region in aws_credentials.json is correct"]
            )


# ── DETECT SPENDING ───────────────────────────────────────────────────────────
def detect_spending(session):
    print("  Detecting current AWS spending...")
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
        return round(amount, 4)
    except Exception as e:
        print(f"  Could not fetch spending: {e}")
        return None


# ── LOAD BUDGETS ──────────────────────────────────────────────────────────────
def load_budgets(folder):
    csv_file = os.path.join(folder, "budgets.csv")
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"budgets.csv not found: {csv_file}")

    rows, encoding = smart_open_csv(csv_file)
    print(f"  budgets.csv encoding      : {encoding}")

    if rows:
        print(f"  budgets.csv columns found : {list(rows[0].keys())}")

    budgets = []
    for r in rows:
        name   = r.get("name", "").strip()
        amount = r.get("amount", "").strip()
        if name and amount:
            budgets.append({"name": name, "amount": amount})

    print(f"  Valid budgets loaded       : {len(budgets)}")
    return budgets


# ── LOAD EMAILS (nuclear, skips sent) ─────────────────────────────────────────
def load_all_emails(folder):
    csv_file = os.path.join(folder, "emails.csv")
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"emails.csv not found: {csv_file}")

    rows, encoding = smart_open_csv(csv_file)
    print(f"  emails.csv encoding       : {encoding}")

    if not rows:
        print(f"  {RED}CRITICAL: Could not read emails.csv with any method{RESET}")
        return []

    if rows:
        print(f"  emails.csv columns found  : {list(rows[0].keys())}")
        # Show first row as sample
        print(f"  emails.csv sample row     : {dict(list(rows[0].items())[:3])}")

    all_emails  = []
    sent_count  = 0
    empty_count = 0

    for row in rows:
        email_col  = find_email_column(row)
        report_col = find_report_column(row)

        if email_col is None:
            # Try to find any email-like value in this row
            for v in row.values():
                if v and EMAIL_REGEX.match(v.strip()):
                    email_col_val = v.strip()
                    report = ""
                    if not report or report != "sent":
                        all_emails.append(email_col_val)
                    break
            continue

        email  = row.get(email_col, "").strip()
        report = row.get(report_col, "").strip().lower() if report_col else ""

        if not email:
            empty_count += 1
            continue

        if report == "sent":
            sent_count += 1
            continue

        all_emails.append(email)

    print(f"  Total rows in file        : {len(rows)}")
    print(f"  Already sent   (skipped)  : {sent_count}")
    print(f"  Empty rows     (skipped)  : {empty_count}")
    print(f"  Available unsent emails   : {GREEN}{len(all_emails):,}{RESET}")

    return all_emails


# ── MARK EMAILS SENT ──────────────────────────────────────────────────────────
csv_lock = threading.Lock()

# ── IN-MEMORY SENT TRACKER ───────────────────────────────────────────────────
# Tracks sent emails in memory during the run.
# Writes to CSV only ONCE at the very end — not per budget.
# This is the key fix for performance with large email files.
_sent_emails_set = set()

def mark_emails_sent(csv_file, email_list):
    """Add emails to in-memory set. CSV is written once at end via flush_sent_emails."""
    with csv_lock:
        for e in email_list:
            _sent_emails_set.add(e.lower().strip())

def flush_sent_emails(csv_file):
    """Write all sent emails to CSV at once — call this once after all budgets created."""
    if not _sent_emails_set:
        return
    with csv_lock:
        rows, _ = smart_open_csv(csv_file)
        if not rows:
            return
        fieldnames = list(rows[0].keys())
        if "report" not in fieldnames:
            fieldnames.append("report")
        for row in rows:
            email_col = find_email_column(row)
            if not email_col:
                continue
            e = row.get(email_col, "").strip().lower()
            if e in _sent_emails_set:
                row["report"] = "sent"
            if "report" not in row:
                row["report"] = ""
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        print(f"  emails.csv updated — {len(_sent_emails_set):,} emails marked as sent")


# ── GET EXISTING BUDGETS ──────────────────────────────────────────────────────
def get_existing_budgets(client, account_id):
    print("  Fetching existing budgets from AWS...")
    existing = set()
    try:
        paginator = client.get_paginator("describe_budgets")
        for page in paginator.paginate(AccountId=account_id):
            for b in page.get("Budgets", []):
                existing.add(b["BudgetName"])
        print(f"  Found {len(existing)} existing budgets in AWS")
    except Exception as e:
        print(f"  Could not fetch existing budgets: {e}")
    return existing


# ══════════════════════════════════════════════════════════════
# PRE-FLIGHT CHECKS
# ══════════════════════════════════════════════════════════════
def preflight_checks(budgets, all_emails, existing_budgets):
    print()
    print(f"  {'─'*50}")
    print(f"  PRE-FLIGHT CHECKS")
    print(f"  {'─'*50}")

    new_budgets = [b for b in budgets if b["name"] not in existing_budgets]

    # Check 1 — budgets.csv not empty
    if len(budgets) == 0:
        show_no_budgets_warning()
    print(f"  [1] budgets.csv           : {GREEN}OK{RESET} — {len(budgets):,} total | {len(new_budgets):,} new")

    # Check 2 — calculate emails needed
    emails_needed = len(new_budgets) * ALERTS_PER_BUDGET * EMAILS_PER_ALERT
    available     = len(all_emails)
    print(f"  [2] Emails needed         : {emails_needed:,}")
    print(f"      Formula               : {len(new_budgets):,} x {ALERTS_PER_BUDGET} x {EMAILS_PER_ALERT}")
    print(f"  [3] Emails available      : {GREEN}{available:,}{RESET}")

    # Check 3 — enough emails?
    if available == 0 or available < emails_needed:
        show_no_emails_warning(available, emails_needed, len(new_budgets))
    print(f"  [4] Email check           : {GREEN}OK{RESET} — surplus of {available - emails_needed:,} emails")
    print(f"  {'─'*50}")

    # All passed — SAB THIK HAI + Press ENTER
    show_sab_thik_hai(len(new_budgets), available, emails_needed)

    return new_budgets


# ── BUILD BUDGET TASKS ────────────────────────────────────────────────────────
def build_budget_tasks(new_budgets, all_emails, notification_type):
    total_alert_slots = len(new_budgets) * len(THRESHOLDS)
    emails_per_alert  = max(1, len(all_emails) // total_alert_slots) if total_alert_slots else 1
    remainder         = len(all_emails) % total_alert_slots if total_alert_slots else 0

    slices, start = [], 0
    for idx in range(total_alert_slots):
        end    = start + emails_per_alert + (1 if idx < remainder else 0)
        slices.append(all_emails[start:end][:EMAILS_PER_ALERT])
        start  = end

    tasks, alert_idx = [], 0
    for budget in new_budgets:
        notifications = []
        sent_emails   = []
        for threshold in THRESHOLDS:
            alert_emails = slices[alert_idx] if alert_idx < len(slices) else []
            alert_idx   += 1
            if not alert_emails:
                continue
            for e in alert_emails:
                if e not in sent_emails:
                    sent_emails.append(e)
            notifications.append({
                "Notification": {
                    "NotificationType"  : notification_type,
                    "ComparisonOperator": "GREATER_THAN",
                    "Threshold"         : float(threshold),
                    "ThresholdType"     : "PERCENTAGE",
                    "NotificationState" : "ALARM"
                },
                "Subscribers": [
                    {"SubscriptionType": "EMAIL", "Address": e}
                    for e in alert_emails
                ]
            })
        tasks.append({
            "name"         : budget["name"],
            "amount"       : budget["amount"],
            "notifications": notifications,
            "sent_emails"  : sent_emails
        })
    return tasks


# ══════════════════════════════════════════════════════════════
# SINGLE PHASE — CREATE BUDGET + NOTIFICATIONS IN ONE API CALL
# ══════════════════════════════════════════════════════════════
def run_creation(client, account_id, tasks, profile, emails_csv_path, rate_limiter):
    lock         = threading.Lock()
    created      = []
    failed       = []
    exists       = []
    count        = [0]
    total_emails = [0]
    total_tasks  = len(tasks)
    start_time   = [time.time()]

    def create_one(task):
        if _SHUTDOWN:
            return {"status": "cancelled", "task": task}
        rate_limiter.acquire()
        if _SHUTDOWN:
            return {"status": "cancelled", "task": task}
        for attempt in range(6):
            try:
                client.create_budget(
                    AccountId = account_id,
                    Budget    = {
                        "BudgetName" : task["name"],
                        "BudgetLimit": {"Amount": str(task["amount"]), "Unit": "USD"},
                        "TimeUnit"   : "MONTHLY",
                        "BudgetType" : "COST",
                        "CostTypes"  : {
                            "IncludeTax"              : True,
                            "IncludeSubscription"     : True,
                            "UseBlended"              : False,
                            "IncludeRefund"           : False,
                            "IncludeCredit"           : False,
                            "IncludeUpfront"          : True,
                            "IncludeRecurring"        : True,
                            "IncludeOtherSubscription": True,
                            "IncludeSupport"          : True,
                            "IncludeDiscount"         : True,
                            "UseAmortized"            : False
                        }
                    },
                    NotificationsWithSubscribers=task["notifications"]
                )
                with lock:
                    count[0]        += 1
                    total_emails[0] += len(task["sent_emails"])
                    elapsed          = time.time() - start_time[0]
                    mins, secs       = divmod(int(elapsed), 60)
                    speed            = round(count[0] / elapsed, 1) if elapsed > 0 else 0
                    remaining        = int((total_tasks - count[0]) / speed) if speed > 0 else 0
                    eta_m, eta_s     = divmod(remaining, 60)
                    print(
                        f"  [OK]  {count[0]}/{total_tasks}"
                        f"  | Emails: {total_emails[0]:,}"
                        f"  | Elapsed: {mins}m {secs:02d}s"
                        f"  | Speed: {speed}/sec"
                        f"  | ETA: {eta_m}m {eta_s:02d}s"
                    )
                return {"status": "created", "task": task}

            except client.exceptions.DuplicateRecordException:
                with lock:
                    print(f"  [EXISTS] '{task['name']}'")
                return {"status": "exists", "task": task}

            except Exception as e:
                err = str(e)
                if "Throttling" in err or "Rate exceeded" in err or "TooManyRequests" in err:
                    # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                    wait = 2 ** attempt
                    with lock:
                        print(f"  [THROTTLE] attempt {attempt+1}/6 — waiting {wait}s...")
                    time.sleep(wait)
                    continue
                with lock:
                    print(f"  [FAIL]  '{task['name']}' — {err[:80]}")
                return {"status": "failed", "task": task, "error": err}

        with lock:
            print(f"  [FAIL]  '{task['name']}' — throttled after 6 retries")
        return {"status": "failed", "task": task, "error": "ThrottlingException"}

    batch_sz = profile["BATCH_SIZE"]
    batches  = [tasks[i:i+batch_sz] for i in range(0, len(tasks), batch_sz)]

    for b_idx, batch in enumerate(batches):
        if _SHUTDOWN:
            print(f"\n  {YELLOW}Stopped by user — saving progress...{RESET}")
            break

        if len(batches) > 1:
            print(f"\n  Batch {b_idx+1}/{len(batches)} ({len(batch)} budgets)...")

        with ThreadPoolExecutor(max_workers=profile["PARALLEL_THREADS"]) as executor:
            futures = {executor.submit(create_one, t): t for t in batch}
            for future in as_completed(futures):
                result = future.result()
                if result["status"] == "created":
                    created.append(result["task"])
                elif result["status"] == "exists":
                    exists.append(result["task"])
                elif result["status"] == "cancelled":
                    failed.append(result["task"])  # treat cancelled as failed for retry
                else:
                    failed.append(result["task"])

        if _SHUTDOWN:
            break

        if b_idx < len(batches) - 1 and profile["BATCH_PAUSE"] > 0:
            print(f"  Batch pause {profile['BATCH_PAUSE']}s — letting API breathe...")
            time.sleep(profile["BATCH_PAUSE"])

    return created, failed, exists


# ── PROCESS ONE ACCOUNT ───────────────────────────────────────────────────────
def check_account_preflight(folder):
    """
    Checks one account folder silently — no verbose output.
    Returns dict with status='ok' or status='error' and details.
    """
    import io as _io
    name   = os.path.basename(folder)
    issues = []

    # Suppress prints during silent check
    _old_stdout = sys.stdout
    sys.stdout  = _io.StringIO()

    try:
        creds = load_credentials(folder)
        session = boto3.Session(
            aws_access_key_id     = creds["aws_access_key_id"],
            aws_secret_access_key = creds["aws_secret_access_key"],
            region_name           = creds["region"]
        )
        account_id = get_account_id(session)
        budgets    = load_budgets(folder)
        all_emails = load_all_emails(folder)

        if len(budgets) == 0:
            issues.append("budgets.csv is empty")

        emails_needed = len(budgets) * ALERTS_PER_BUDGET * EMAILS_PER_ALERT
        if len(all_emails) == 0:
            issues.append("No unsent emails in emails.csv")
        elif len(all_emails) < emails_needed:
            issues.append(f"Not enough emails: need {emails_needed:,}, have {len(all_emails):,}")

        sys.stdout = _old_stdout

        if issues:
            return {"status":"error","name":name,"folder":folder,
                    "account_id":account_id,"issues":issues,
                    "budgets":len(budgets),"emails":len(all_emails)}

        return {"status":"ok","name":name,"folder":folder,
                "account_id":account_id,"budgets":len(budgets),
                "emails":len(all_emails),"issues":[]}

    except SystemExit:
        # show_error() called sys.exit — restore stdout and capture message
        _captured = sys.stdout.getvalue() if hasattr(sys.stdout, 'getvalue') else ""
        sys.stdout = _old_stdout
        # Extract meaningful error from captured output if any
        err_msg = "Credentials or file error — check aws_credentials.json"
        return {"status":"error","name":name,"folder":folder,
                "account_id":"unknown","issues":[err_msg],
                "budgets":0,"emails":0}

    except Exception as e:
        sys.stdout = _old_stdout
        return {"status":"error","name":name,"folder":folder,
                "account_id":"unknown","issues":[str(e)[:120]],
                "budgets":0,"emails":0}


def show_account_mini_status(check_result, index, total):
    """
    Shows a small success or warning box per account after checking.
    Plays the appropriate tone. Waits so user can see + hear each one.
    """
    import shutil
    TW   = shutil.get_terminal_size(fallback=(100, 40)).columns - 2
    TW   = max(TW, 60)
    name = check_result["name"]
    aid  = check_result["account_id"]

    def top(c):  print(f"{c}╔{'═'*(TW-2)}╗{RESET}")
    def bot(c):  print(f"{c}╚{'═'*(TW-2)}╝{RESET}")
    def emp(c):  print(f"{c}║{' '*(TW-2)}║{RESET}")
    def row(t, c, bold=False):
        inner  = TW - 4
        padded = t.center(inner)
        b      = BOLD if bold else ""
        print(f"{c}║  {b}{padded}{RESET}{c}  ║{RESET}")

    print(f"\n  [{index}/{total}] Checking: {BOLD}{name}{RESET}")

    if check_result["status"] == "ok":
        top(GREEN); emp(GREEN)
        row(f"ACCOUNT {index}/{total}  —  {name}", GREEN, bold=True)
        row(f"ID: {aid}", GREEN)
        emp(GREEN)
        row(f"Budgets : {check_result['budgets']:,}    Unsent emails : {check_result['emails']:,}", GREEN)
        emp(GREEN)
        row("READY TO GO", GREEN, bold=True)
        emp(GREEN); bot(GREEN)
    else:
        play_tone(WARNING_TONE_B64, wait_for_exit=False)
        top(RED); emp(RED)
        row(f"ACCOUNT {index}/{total}  —  {name}", RED, bold=True)
        row(f"ID: {aid}", RED)
        emp(RED)
        for issue in check_result["issues"]:
            row(f"ISSUE: {issue}", RED)
        emp(RED)
        row("CANNOT RUN — NEEDS FIXING", RED, bold=True)
        emp(RED); bot(RED)

    time.sleep(1.5)   # pause so user can see + hear each result


def show_preflight_summary_and_ask(results):
    """
    After checking all accounts, shows full summary and asks:
    1 = Run only good accounts
    2 = Stop completely
    """
    import shutil
    TW  = shutil.get_terminal_size(fallback=(100, 40)).columns - 2
    TW  = max(TW, 60)
    ok  = [r for r in results if r["status"] == "ok"]
    bad = [r for r in results if r["status"] == "error"]

    def top(c):  print(f"{c}╔{'═'*(TW-2)}╗{RESET}")
    def mid(c):  print(f"{c}╠{'═'*(TW-2)}╣{RESET}")
    def bot(c):  print(f"{c}╚{'═'*(TW-2)}╝{RESET}")
    def emp(c):  print(f"{c}║{' '*(TW-2)}║{RESET}")
    def row(t, c, bold=False):
        inner  = TW - 4
        padded = t.center(inner)
        b      = BOLD if bold else ""
        print(f"{c}║  {b}{padded}{RESET}{c}  ║{RESET}")
    def row_l(t, c):
        inner  = TW - 4
        padded = t.ljust(inner)
        print(f"{c}║  {padded}  ║{RESET}")

    print()
    top(WHITE)
    row("PREFLIGHT CHECK COMPLETE — ALL ACCOUNTS", WHITE, bold=True)
    mid(WHITE)
    emp(WHITE)
    row(f"Total accounts checked : {len(results)}", WHITE)
    row(f"READY to run           : {GREEN}{len(ok)}{RESET}{WHITE}", WHITE)
    row(f"Have ERRORS            : {RED}{len(bad)}{RESET}{WHITE}", WHITE)
    emp(WHITE)

    if ok:
        mid(WHITE)
        row(f"GOOD ACCOUNTS ({len(ok)})", GREEN, bold=True)
        emp(WHITE)
        for r in ok:
            row_l(f"  {GREEN}OK{RESET}   {r['name']:<25} | Budgets: {r['budgets']:>6,} | Emails: {r['emails']:>10,}", WHITE)
        emp(WHITE)

    if bad:
        mid(WHITE)
        row(f"ERROR ACCOUNTS ({len(bad)})", RED, bold=True)
        emp(WHITE)
        for r in bad:
            row_l(f"  {RED}ERR{RESET}  {r['name']:<25} | {r['issues'][0][:50]}", WHITE)
        emp(WHITE)

    bot(WHITE)

    # All accounts have errors — must stop
    if not ok:
        print(f"\n  {RED}{BOLD}No accounts are ready to run. Fix errors above and re-run.{RESET}\n")
        play_tone(WARNING_TONE_B64, wait_for_exit=True)
        sys.exit(1)

    # All accounts are good — no need to ask
    if not bad:
        print(f"\n  {GREEN}All {len(ok)} accounts are ready!{RESET}")
        return [r["folder"] for r in ok]

    # Mix — ask user what to do
    print()
    print(f"  {YELLOW}Some accounts have errors. What do you want to do?{RESET}\n")
    print(f"  1  ->  RUN only the {len(ok)} GOOD account(s) — skip the {len(bad)} with errors")
    print(f"  2  ->  STOP completely — fix errors first then re-run")
    print()

    while True:
        choice = input("  Enter 1 or 2: ").strip()
        if choice == "1":
            print(f"\n  {GREEN}Continuing with {len(ok)} good account(s)...{RESET}")
            return [r["folder"] for r in ok]
        elif choice == "2":
            print(f"\n  {RED}Stopped. Fix the errors above and re-run.{RESET}\n")
            sys.exit(0)
        else:
            print("  Please enter 1 or 2")


def process_account(folder, notification_type, profile, mode):
    import io as _io

    # Load everything silently
    _old = sys.stdout
    sys.stdout = _io.StringIO()
    creds      = load_credentials(folder)
    session    = boto3.Session(
        aws_access_key_id     = creds["aws_access_key_id"],
        aws_secret_access_key = creds["aws_secret_access_key"],
        region_name           = creds["region"]
    )
    account_id = get_account_id(session)
    budgets    = load_budgets(folder)
    all_emails = load_all_emails(folder)
    client     = session.client("budgets", region_name=creds["region"])
    existing   = get_existing_budgets(client, account_id)
    sys.stdout = _old

    emails_csv_path = os.path.join(folder, "emails.csv")
    new_budgets     = [b for b in budgets if b["name"] not in existing]
    emails_needed   = len(new_budgets) * ALERTS_PER_BUDGET * EMAILS_PER_ALERT
    unsent_emails   = all_emails[:emails_needed] if len(all_emails) >= emails_needed else all_emails
    tasks           = build_budget_tasks(new_budgets, unsent_emails, notification_type)
    rate_limiter    = RateLimiter(profile["RATE_LIMIT_PER_SEC"])

    print(f"\n{'─'*60}")
    print(f"  {GREEN}{os.path.basename(folder)}{RESET}  |  {account_id}  |  {len(new_budgets):,} budgets")
    print(f"{'─'*60}")

    grand_created = 0
    grand_exists  = 0
    pending       = tasks
    start_time    = time.time()

    for round_num in range(MAX_RETRY_ROUNDS + 1):
        if not pending:
            break

        label = "INITIAL RUN" if round_num == 0 else f"RETRY ROUND {round_num}"
        print(f"\n  {label} — {len(pending)} budget(s) | Mode: {mode.upper()}")

        created, failed, exists = run_creation(
            client, account_id, pending,
            profile, emails_csv_path, rate_limiter
        )

        grand_created += len(created)
        grand_exists  += len(exists)
        pending        = failed

        elapsed    = time.time() - start_time
        mins, secs = divmod(int(elapsed), 60)

        print(f"\n  {'─'*40}")
        print(f"  Created : {GREEN}{len(created)}{RESET}  |  Failed : {RED}{len(failed)}{RESET}  |  Time : {mins}m {secs}s")
        print(f"  {'─'*40}")

        if failed:
            for t in failed:
                print(f"    - {t['name']}")

        if pending and round_num < MAX_RETRY_ROUNDS:
            print(f"\n  Waiting {profile['RETRY_DELAY']}s before retry round {round_num+1}...")
            time.sleep(profile["RETRY_DELAY"])

    total_elapsed = time.time() - start_time
    mins, secs    = divmod(int(total_elapsed), 60)

    # Write emails.csv ONCE at end — massive performance improvement
    print(f"\n  Saving emails.csv...")
    flush_sent_emails(emails_csv_path)

    print(f"\n  {GREEN}Account complete!{RESET}")
    print(f"  Total Created  : {grand_created:,}")
    print(f"  Total Exists   : {grand_exists}")
    print(f"  Still Failed   : {len(pending)}")
    print(f"  Total Time     : {mins}m {secs}s")
    divider()

    return {
        "account_id"   : account_id,
        "budgets"      : len(budgets),
        "created"      : grand_created,
        "emails_sent"  : grand_created * ALERTS_PER_BUDGET * EMAILS_PER_ALERT,
        "failed"       : len(pending),
        "failed_tasks" : pending,        # keep failed tasks for retry
        "elapsed_sec"  : int(total_elapsed),
        "client"       : client,
        "account_id_v" : account_id,
        "profile"      : profile,
        "emails_csv"   : emails_csv_path,
        "rate_limiter" : rate_limiter,
    }


# ── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # Check for updates FIRST — before anything else
    check_for_updates()

    # Check boto3
    if boto3 is None:
        os.system("")
        print(f"\n{RED}{'=' * 60}{RESET}")
        print(f"{RED}  MISSING: boto3 is not installed!{RESET}")
        print(f"{RED}  Fix:     pip install boto3{RESET}")
        print(f"{RED}{'=' * 60}{RESET}\n")
        play_tone(WARNING_TONE_B64, wait_for_exit=True)
        sys.exit(1)

    try:
        divider()
        print("        AWS Budget Creator  (Single Phase — Fast & Safe)")
        divider()

        account_mode    = detect_account_mode(BASE_DIR)
        account_folders = get_account_folders(BASE_DIR, account_mode)
        mode            = ask_mode()
        profile         = PROFILES[mode]

        # Ask how many accounts to run in parallel
        accounts_in_parallel = ask_parallel_mode(account_folders)

        print()
        divider()
        print("  Choose alert method:\n")
        print("  1  ->  ACTUAL     — fires when real spend exceeds threshold")
        print("  2  ->  FORECASTED — fires when AWS predicts spend will exceed threshold")
        print()
        while True:
            method = input("  Enter 1 or 2: ").strip()
            if method == "1":
                notification_type = "ACTUAL"
                print("  Selected: ACTUAL alerts")
                break
            elif method == "2":
                notification_type = "FORECASTED"
                print("  Selected: FORECASTED alerts")
                break
            else:
                print("  Please enter 1 or 2")

        est_mins = round((20000 / profile["RATE_LIMIT_PER_SEC"]) / 60)

        # ── Run Summary first ──────────────────────────────────
        print()
        box("RUN SUMMARY", [
            f"Account Mode           : {account_mode.upper()}",
            f"Total Accounts Found   : {len(account_folders)}",
            f"Accounts in Parallel   : {accounts_in_parallel}",
            f"Speed Mode             : {mode.upper()}",
            f"Approach               : Single phase (budget + emails in 1 API call)",
            f"Parallel Threads       : {profile['PARALLEL_THREADS']}",
            f"Rate Cap               : {profile['RATE_LIMIT_PER_SEC']} calls/sec",
            f"Alert Mode             : {notification_type}",
            f"Alerts per Budget      : {ALERTS_PER_BUDGET} (at {THRESHOLDS}%)",
            f"Emails per Alert       : {EMAILS_PER_ALERT}",
            f"Max Retry Rounds       : {MAX_RETRY_ROUNDS}",
            f"Estimated Time (20k)   : ~{est_mins * max(1, len(account_folders) // accounts_in_parallel)} min total",
            f"Sent emails            : SKIPPED automatically",
        ])

        print()
        confirm = input("  Proceed? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("  Cancelled.")
            sys.exit(0)

        # ── Check ALL accounts with tone per account ───────────
        print()
        divider()
        print(f"  Checking all {len(account_folders)} account(s)...")
        divider()

        check_results = []
        for i, folder in enumerate(account_folders, 1):
            result = check_account_preflight(folder)
            check_results.append(result)
            show_account_mini_status(result, i, len(account_folders))

        # ── Account summary + ask run good only or stop ────────
        good_folders = show_preflight_summary_and_ask(check_results)

        # ── SAB THIK HAI + PRESS ENTER to send ────────────────
        ok_results    = [r for r in check_results if r["status"] == "ok"
                         and r["folder"] in good_folders]
        total_budgets = sum(r["budgets"] for r in ok_results)
        total_emails  = sum(r["emails"]  for r in ok_results)
        emails_needed = total_budgets * ALERTS_PER_BUDGET * EMAILS_PER_ALERT
        show_sab_thik_hai(total_budgets, total_emails, emails_needed)

        acc_batch_size  = accounts_in_parallel
        account_batches = [
            good_folders[i:i+acc_batch_size]
            for i in range(0, len(good_folders), acc_batch_size)
        ]

        all_results  = []
        script_start = time.time()

        for acc_batch in account_batches:
            if acc_batch_size == 1 or len(acc_batch) == 1:
                for folder in acc_batch:
                    result = process_account(folder, notification_type, profile, mode)
                    all_results.append(result)
            else:
                with ThreadPoolExecutor(max_workers=acc_batch_size) as executor:
                    futures = {
                        executor.submit(process_account, folder, notification_type, profile, mode): folder
                        for folder in acc_batch
                    }
                    for future in as_completed(futures):
                        try:
                            all_results.append(future.result())
                        except Exception as e:
                            print(f"  Account error: {e}")

        total_elapsed          = time.time() - script_start
        total_mins, total_secs = divmod(int(total_elapsed), 60)
        total_created          = sum(r["created"]     for r in all_results)
        total_failed           = sum(r["failed"]      for r in all_results)
        total_emails           = sum(r["emails_sent"] for r in all_results)

        # ── FINAL GRAND SUMMARY ────────────────────────────────
        print()
        summary_lines = [
            f"Speed Mode             : {mode.upper()}",
            f"Account Mode           : {account_mode.upper()}",
            f"Total Accounts         : {len(all_results)}",
            f"Total Budgets Created  : {total_created:,}",
            f"Total Failed           : {RED}{total_failed}{RESET}" if total_failed else f"Total Failed           : {GREEN}0{RESET}",
            f"Total Emails Assigned  : {total_emails:,}",
            f"emails.csv Updated     : yes (report = sent)",
            f"Total Time             : {total_mins}m {total_secs}s",
        ]
        for r in all_results:
            m, s = divmod(r["elapsed_sec"], 60)
            summary_lines.append(
                f"  [{r['account_id']}] Created: {r['created']:,} | Failed: {r['failed']} | Time: {m}m {s}s"
            )
        box("FINAL GRAND SUMMARY", summary_lines)

        # ── ASK TO RETRY FAILED BUDGETS ────────────────────────
        if total_failed > 0:
            print()
            print(f"  {RED}{'─'*50}{RESET}")
            print(f"  {RED}{BOLD}{total_failed} budget(s) failed to create.{RESET}")
            print(f"  {RED}{'─'*50}{RESET}")
            print()
            for r in all_results:
                if r["failed"] > 0:
                    print(f"  Account: {r['account_id']} — {r['failed']} failed")
                    for t in r["failed_tasks"][:5]:
                        print(f"    - {t['name']}")
                    if r["failed"] > 5:
                        print(f"    ... and {r['failed']-5} more")
            print()
            print(f"  Do you want to retry the {total_failed} failed budget(s)?")
            print(f"  1  ->  YES — retry now")
            print(f"  2  ->  NO  — skip and exit")
            print()

            while True:
                retry_choice = input("  Enter 1 or 2: ").strip()
                if retry_choice == "1":
                    print(f"\n  {CYAN}Retrying {total_failed} failed budget(s)...{RESET}\n")
                    retry_created = 0
                    retry_failed  = 0
                    for r in all_results:
                        if not r["failed_tasks"]:
                            continue
                        print(f"\n  {'─'*50}")
                        print(f"  Retrying account: {r['account_id']}")
                        print(f"  {'─'*50}")
                        created, failed, exists = run_creation(
                            r["client"], r["account_id_v"],
                            r["failed_tasks"], r["profile"],
                            r["emails_csv"], r["rate_limiter"]
                        )
                        retry_created += len(created)
                        retry_failed  += len(failed)
                        # Update totals
                        r["created"]      += len(created)
                        r["failed"]        = len(failed)
                        r["failed_tasks"]  = failed

                    print()
                    print(f"  {GREEN}{'─'*50}{RESET}")
                    print(f"  Retry complete!")
                    print(f"  {GREEN}Recovered : {retry_created}{RESET}")
                    print(f"  {RED}Still failed : {retry_failed}{RESET}")
                    print(f"  {GREEN}{'─'*50}{RESET}")

                    if retry_failed > 0:
                        print(f"\n  {YELLOW}{retry_failed} budget(s) still could not be created.{RESET}")
                        print(f"  Check your AWS account limits and credentials.")
                    else:
                        print(f"\n  {GREEN}All budgets created successfully!{RESET}")
                    break

                elif retry_choice == "2":
                    print(f"\n  {YELLOW}Skipped retry. {total_failed} budget(s) were not created.{RESET}\n")
                    break
                else:
                    print("  Please enter 1 or 2")
        else:
            print(f"\n  {GREEN}All budgets created successfully — zero failures!{RESET}\n")

    except KeyboardInterrupt:
        print(f"\n\n  {YELLOW}{'─'*50}{RESET}")
        print(f"  {YELLOW}Script stopped by user (Ctrl+C).{RESET}")
        print(f"  {YELLOW}{'─'*50}{RESET}")
        print(f"  emails.csv has been updated with emails sent so far.")
        print(f"  Re-run the script to continue — already-sent emails are skipped.")
        print()
        sys.exit(0)

    except SystemExit:
        raise

    except FileNotFoundError as e:
        show_error(
            "FILE NOT FOUND",
            [str(e)],
            ["Make sure all required files are in the correct folders",
             "Required per account: aws_credentials.json, budgets.csv, emails.csv"]
        )

    except PermissionError as e:
        show_error(
            "PERMISSION DENIED — Cannot access file",
            [str(e)],
            ["Run the script as administrator",
             "Make sure no other program has the file open (close Excel etc.)"]
        )

    except Exception as e:
        import traceback
        tb_lines = traceback.format_exc().strip().split("\n")
        detail_lines = [str(e)[:120]] + [l for l in tb_lines if l.strip()][-5:]
        show_error(
            "UNEXPECTED ERROR OCCURRED",
            detail_lines,
            ["Check the error details above",
             "Make sure all CSV files are valid",
             "Make sure AWS credentials are correct and not expired"]
        )
