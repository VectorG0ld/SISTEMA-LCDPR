# daily_backup.py
import os, json, zipfile
from datetime import datetime

def _state_path(project_dir):
    p = os.path.join(project_dir, "banco_de_dados", "backup_state.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    return p

def _db_path(project_dir, profile):
    return os.path.join(project_dir, "banco_de_dados", profile, "data", "lcdpr.db")

def run_daily_backup(project_dir: str, profile: str):
    today = datetime.now().strftime("%Y-%m-%d")
    state_file = _state_path(project_dir)
    try:
        state = json.load(open(state_file, "r", encoding="utf-8"))
    except Exception:
        state = {}

    last = state.get("last_backup_date")
    if last == today:
        return  # já fez hoje

    src = _db_path(project_dir, profile)
    if not os.path.exists(src):
        return

    out_dir = os.path.join(project_dir, "banco_de_dados", profile, "backups")
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    out_zip = os.path.join(out_dir, f"backup_{ts}.zip")

    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(src, arcname="lcdpr.db")

    state["last_backup_date"] = today
    json.dump(state, open(state_file, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
