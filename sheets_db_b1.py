"""Breach 1 (FP1 — Disintermediation) data layer backed by Google Sheets.

Replaces SQLite db.py for B1 cases. Same function signatures so app.py can swap imports.
"""

import json
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build

SA_FILE = Path(__file__).parent / "service_account.json"
B1_SHEET_ID = os.environ.get(
    "B1_SHEET_ID", "1BzV24db7cuetXNqMch10knC9aygr0-jHgZLaeSprWPc"
)

HEADERS = [
    "row_id", "source", "lng_nas_id", "customer_mobile", "partner_name", "partner_id",
    "partner_email", "city", "mis_city", "zone", "expiry_dt", "tenure",
    "r_oct", "r_nov", "r_dec", "r_jan", "risk_score", "partner_status",
    "connected", "calling_remarks", "disintermediation", "call_recording",
    "called_by", "call_timestamp", "calling_status", "report_text", "reported_by",
    "cancelled_time", "action_type", "email_state", "email_sent_at", "email_case_type",
    "penalty_state", "penalty_amount", "penalty_email_state", "penalty_email_sent_at",
    "created_at", "updated_at",
]
COL = {h: i for i, h in enumerate(HEADERS)}

ELOG_HEADERS = [
    "log_id", "case_row_id", "partner_name", "partner_email", "recipient_email",
    "case_type", "language", "variables_json", "subject", "body_preview",
    "status", "is_test", "sent_at", "error",
]
ELOG_COL = {h: i for i, h in enumerate(ELOG_HEADERS)}

# Safe columns that upsert may overwrite (never overwrite action/email/penalty state)
SAFE_UPDATE_KEYS = {
    "expiry_dt", "city", "mis_city", "zone", "partner_name", "tenure",
    "r_oct", "r_nov", "r_dec", "r_jan", "risk_score", "partner_status",
    "connected", "calling_remarks", "disintermediation", "call_recording",
    "called_by", "call_timestamp", "calling_status", "partner_email",
    "partner_id", "report_text", "reported_by", "cancelled_time",
}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _col_letter(idx):
    """Convert 0-based column index to sheet letter(s). 0=A, 25=Z, 26=AA, etc."""
    if idx < 26:
        return chr(ord("A") + idx)
    return chr(ord("A") + idx // 26 - 1) + chr(ord("A") + idx % 26)


def _get_service():
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = service_account.Credentials.from_service_account_file(str(SA_FILE), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def _now_ist() -> str:
    d = datetime.utcnow() + timedelta(hours=5, minutes=30)
    return d.strftime("%Y-%m-%d %H:%M:%S")


# ── Internal helpers ────────────────────────────────────────────────────────

def _read_all_cases() -> list[list]:
    svc = _get_service()
    last_col = _col_letter(len(HEADERS) - 1)
    result = svc.spreadsheets().values().get(
        spreadsheetId=B1_SHEET_ID,
        range=f"B1Cases!A2:{last_col}10000",
    ).execute()
    return result.get("values", [])


def _read_all_email_logs() -> list[list]:
    svc = _get_service()
    last_col = _col_letter(len(ELOG_HEADERS) - 1)
    result = svc.spreadsheets().values().get(
        spreadsheetId=B1_SHEET_ID,
        range=f"B1EmailLog!A2:{last_col}10000",
    ).execute()
    return result.get("values", [])


def _row_to_dict(row: list) -> dict:
    padded = row + [""] * (len(HEADERS) - len(row))
    obj = {}
    for i, h in enumerate(HEADERS):
        val = padded[i]
        obj[h] = None if val == "" else val
    # Numeric conversions
    if obj.get("penalty_amount") is not None:
        try:
            obj["penalty_amount"] = float(obj["penalty_amount"])
        except (ValueError, TypeError):
            obj["penalty_amount"] = -2000
    # Alias: expose row_id as "id" for frontend compatibility
    obj["id"] = obj["row_id"]
    return obj


def _elog_to_dict(row: list) -> dict:
    padded = row + [""] * (len(ELOG_HEADERS) - len(row))
    obj = {}
    for i, h in enumerate(ELOG_HEADERS):
        val = padded[i]
        obj[h] = None if val == "" else val
    if obj.get("is_test") is not None:
        obj["is_test"] = str(obj["is_test"]) == "1" or str(obj["is_test"]).lower() == "true"
    obj["id"] = obj["log_id"]
    obj["case_id"] = obj["case_row_id"]
    return obj


def _find_row(source: str, lng_nas_id: str, customer_mobile: str, data: list = None) -> int:
    """Return 0-based index in data list matching dedup key, or -1."""
    if data is None:
        data = _read_all_cases()
    src_col = COL["source"]
    lng_col = COL["lng_nas_id"]
    mob_col = COL["customer_mobile"]
    for i, row in enumerate(data):
        padded = row + [""] * (len(HEADERS) - len(row))
        if (str(padded[src_col]) == str(source) and
            str(padded[lng_col]) == str(lng_nas_id) and
            str(padded[mob_col]) == str(customer_mobile)):
            return i
    return -1


def _find_row_by_id(row_id: str, data: list = None) -> int:
    """Return 0-based index by row_id, or -1."""
    if data is None:
        data = _read_all_cases()
    for i, row in enumerate(data):
        if row and str(row[0]) == str(row_id):
            return i
    return -1


def _batch_update_row(tab: str, row_num: int, updates: dict, col_map: dict):
    """Update multiple columns in one API call. row_num is 1-based sheet row."""
    svc = _get_service()
    requests = []
    for col_name, value in updates.items():
        if col_name not in col_map:
            continue
        col_idx = col_map[col_name]
        letter = _col_letter(col_idx)
        cell = f"{tab}!{letter}{row_num}"
        requests.append({
            "range": cell,
            "values": [[value if value is not None else ""]],
        })
    if requests:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=B1_SHEET_ID,
            body={"valueInputOption": "RAW", "data": requests},
        ).execute()


def _append_row(tab: str, values: list):
    """Append a single row to a tab."""
    svc = _get_service()
    svc.spreadsheets().values().append(
        spreadsheetId=B1_SHEET_ID,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [values]},
    ).execute()


# ── Public API ──────────────────────────────────────────────────────────────

def init_db():
    """No-op. Google Sheets schema is managed via headers in row 1."""
    pass


def upsert_breach1_case(data: dict):
    source = data.get("source", "churn_logic")
    lng = data.get("lng_nas_id", "")
    mob = data.get("customer_mobile", "")

    all_data = _read_all_cases()
    idx = _find_row(source, lng, mob, all_data)

    if idx == -1:
        # Insert new row
        row_id = str(uuid.uuid4())[:8]
        now = _now_ist()
        row = [""] * len(HEADERS)
        row[COL["row_id"]] = row_id
        row[COL["source"]] = source
        row[COL["email_state"]] = "pending"
        row[COL["penalty_state"]] = "none"
        row[COL["penalty_amount"]] = "-2000"
        row[COL["penalty_email_state"]] = "pending"
        row[COL["created_at"]] = now
        row[COL["updated_at"]] = now
        for k, v in data.items():
            if k in COL:
                row[COL[k]] = str(v) if v is not None else ""
        _append_row("B1Cases", row)
    else:
        # Update safe columns only
        updates = {}
        existing_row = all_data[idx] + [""] * (len(HEADERS) - len(all_data[idx]))
        for k, v in data.items():
            if k not in SAFE_UPDATE_KEYS:
                continue
            # Don't blank existing partner_email or partner_id
            if k in ("partner_email", "partner_id"):
                existing_val = existing_row[COL[k]]
                if not v and existing_val:
                    continue
            updates[k] = str(v) if v is not None else ""
        if updates:
            updates["updated_at"] = _now_ist()
            row_num = idx + 2  # 0-based data index → 1-based sheet row (header is row 1)
            _batch_update_row("B1Cases", row_num, updates, COL)


def get_breach1_cases(partner=None, zone=None, status=None, email_state=None,
                      search=None, source=None, action_type=None) -> list:
    rows = _read_all_cases()
    cases = [_row_to_dict(r) for r in rows]

    if partner and partner != "all":
        cases = [c for c in cases if c.get("partner_name") == partner]
    if zone and zone != "all":
        cases = [c for c in cases if zone.lower() in (c.get("zone") or "").lower()]
    if status and status != "all":
        cases = [c for c in cases if c.get("partner_status") == status]
    if email_state and email_state != "all":
        cases = [c for c in cases if c.get("email_state") == email_state]
    if source and source != "all":
        cases = [c for c in cases if c.get("source") == source]
    if action_type and action_type != "all":
        cases = [c for c in cases if c.get("action_type") == action_type]
    if search:
        s = search.lower()
        cases = [c for c in cases if
                 s in (c.get("customer_mobile") or "").lower() or
                 s in (c.get("partner_name") or "").lower() or
                 s in (c.get("zone") or "").lower() or
                 s in (c.get("lng_nas_id") or "").lower()]

    cases.sort(key=lambda c: (c.get("partner_name") or "", c.get("expiry_dt") or ""), reverse=False)
    return cases


def get_breach1_case(case_id) -> dict | None:
    data = _read_all_cases()
    idx = _find_row_by_id(str(case_id), data)
    if idx == -1:
        return None
    return _row_to_dict(data[idx])


def get_breach1_summary() -> dict:
    rows = _read_all_cases()
    cases = [_row_to_dict(r) for r in rows]
    by_status = {}
    by_email = {"pending": 0, "sent": 0}
    by_source = {}
    partners = set()
    warning_sent = 0
    penalty_done = 0
    for c in cases:
        st = c.get("partner_status") or "Unknown"
        by_status[st] = by_status.get(st, 0) + 1
        es = c.get("email_state") or "pending"
        by_email[es] = by_email.get(es, 0) + 1
        src = c.get("source") or "unknown"
        by_source[src] = by_source.get(src, 0) + 1
        if c.get("partner_name"):
            partners.add(c["partner_name"])
        if c.get("action_type") == "warning" and c.get("email_state") == "sent":
            warning_sent += 1
        if c.get("action_type") == "penalty":
            penalty_done += 1
    return {
        "total": len(cases),
        "by_status": by_status,
        "by_email_state": by_email,
        "partners": len(partners),
        "by_source": by_source,
        "warning_sent": warning_sent,
        "penalty_done": penalty_done,
    }


def get_breach1_dashboard() -> dict:
    rows = _read_all_cases()
    cases = [_row_to_dict(r) for r in rows]
    today = _now_ist()[:10]
    partners = set()
    acted_today = 0
    acted_by_source = {}
    warning_sent = 0
    penalty_email_sent = 0
    penalty_applied = 0
    total_penalty = 0.0
    pending_action = 0
    by_source = {}
    by_email = {"pending": 0, "sent": 0}
    source_detail = {}

    for c in cases:
        src = c.get("source") or "unknown"
        by_source[src] = by_source.get(src, 0) + 1
        if c.get("partner_name"):
            partners.add(c["partner_name"])
        es = c.get("email_state") or "pending"
        by_email[es] = by_email.get(es, 0) + 1

        # Acted today
        if c.get("action_type") and (c.get("updated_at") or "")[:10] == today:
            acted_today += 1
            acted_by_source[src] = acted_by_source.get(src, 0) + 1

        if c.get("action_type") == "warning" and c.get("email_state") == "sent":
            warning_sent += 1
        if c.get("action_type") == "penalty" and c.get("penalty_email_state") == "sent":
            penalty_email_sent += 1
        if c.get("action_type") == "penalty" and (c.get("penalty_state") or "none") != "none":
            penalty_applied += 1
            total_penalty += (c.get("penalty_amount") or -2000)
        if not c.get("action_type"):
            pending_action += 1

        # Source detail
        if src not in source_detail:
            source_detail[src] = {"total": 0, "pending": 0, "warning_sent": 0,
                                   "penalty_sent": 0, "penalty_applied": 0,
                                   "email_sent": 0, "partners": set()}
        sd = source_detail[src]
        sd["total"] += 1
        if not c.get("action_type"):
            sd["pending"] += 1
        if c.get("action_type") == "warning" and c.get("email_state") == "sent":
            sd["warning_sent"] += 1
        if c.get("action_type") == "penalty" and c.get("penalty_email_state") == "sent":
            sd["penalty_sent"] += 1
        if c.get("action_type") == "penalty" and (c.get("penalty_state") or "none") != "none":
            sd["penalty_applied"] += 1
        if c.get("email_state") == "sent":
            sd["email_sent"] += 1
        if c.get("partner_name"):
            sd["partners"].add(c["partner_name"])

    # Convert sets to counts
    for src, sd in source_detail.items():
        sd["partners"] = len(sd["partners"])

    return {
        "total": len(cases),
        "partners": len(partners),
        "acted_today": acted_today,
        "acted_by_source": acted_by_source,
        "warning_sent": warning_sent,
        "penalty_email_sent": penalty_email_sent,
        "penalty_applied": penalty_applied,
        "total_penalty": round(total_penalty, 2),
        "pending_action": pending_action,
        "by_source": by_source,
        "by_email_state": by_email,
        "source_detail": source_detail,
    }


def get_breach1_partners() -> list:
    rows = _read_all_cases()
    names = set()
    col = COL["partner_name"]
    for r in rows:
        padded = r + [""] * (len(HEADERS) - len(r))
        if padded[col]:
            names.add(padded[col])
    return sorted(names)


def get_breach1_zones() -> list:
    rows = _read_all_cases()
    zones = set()
    col = COL["zone"]
    for r in rows:
        padded = r + [""] * (len(HEADERS) - len(r))
        if padded[col]:
            zones.add(padded[col])
    return sorted(zones)


def get_breach1_sources() -> list:
    rows = _read_all_cases()
    sources = set()
    col = COL["source"]
    for r in rows:
        padded = r + [""] * (len(HEADERS) - len(r))
        if padded[col]:
            sources.add(padded[col])
    return sorted(sources)


def set_breach1_action_type(case_ids: list, action_type: str):
    data = _read_all_cases()
    now = _now_ist()
    for rid in case_ids:
        idx = _find_row_by_id(str(rid), data)
        if idx != -1:
            _batch_update_row("B1Cases", idx + 2,
                              {"action_type": action_type, "updated_at": now}, COL)


def set_breach1_partner_email(case_ids: list, email: str):
    data = _read_all_cases()
    now = _now_ist()
    for rid in case_ids:
        idx = _find_row_by_id(str(rid), data)
        if idx != -1:
            _batch_update_row("B1Cases", idx + 2,
                              {"partner_email": email, "updated_at": now}, COL)


def set_breach1_partner_email_by_name(partner_name: str, email: str) -> int:
    data = _read_all_cases()
    now = _now_ist()
    col = COL["partner_name"]
    count = 0
    for i, row in enumerate(data):
        padded = row + [""] * (len(HEADERS) - len(row))
        if padded[col].lower() == partner_name.lower():
            _batch_update_row("B1Cases", i + 2,
                              {"partner_email": email, "updated_at": now}, COL)
            count += 1
    return count


def mark_breach1_email_sent(case_ids: list, case_type: int = 1):
    data = _read_all_cases()
    now = _now_ist()
    for rid in case_ids:
        idx = _find_row_by_id(str(rid), data)
        if idx != -1:
            _batch_update_row("B1Cases", idx + 2, {
                "email_state": "sent",
                "email_sent_at": now,
                "email_case_type": str(case_type),
                "updated_at": now,
            }, COL)


def get_breach1_penalty_cases(partner=None) -> list:
    rows = _read_all_cases()
    cases = [_row_to_dict(r) for r in rows]
    cases = [c for c in cases if c.get("action_type") == "penalty"
             and (c.get("penalty_state") or "none") == "none"]
    if partner:
        cases = [c for c in cases if c.get("partner_name") == partner]
    cases.sort(key=lambda c: c.get("partner_name") or "")
    return cases


def mark_b1_penalty_csv_generated(case_ids: list):
    data = _read_all_cases()
    now = _now_ist()
    for rid in case_ids:
        idx = _find_row_by_id(str(rid), data)
        if idx != -1:
            _batch_update_row("B1Cases", idx + 2, {
                "penalty_state": "csv_generated",
                "updated_at": now,
            }, COL)


def mark_b1_penalty_uploaded(partner_id: str) -> dict:
    data = _read_all_cases()
    now = _now_ist()
    pid_col = COL["partner_id"]
    at_col = COL["action_type"]
    ps_col = COL["penalty_state"]
    matched = []
    for i, row in enumerate(data):
        padded = row + [""] * (len(HEADERS) - len(row))
        if (str(padded[pid_col]) == str(partner_id) and
            padded[at_col] == "penalty" and
            padded[ps_col] == "csv_generated"):
            matched.append(i)
    if not matched:
        return {"matched": False, "partner_id": partner_id}
    for idx in matched:
        _batch_update_row("B1Cases", idx + 2, {
            "penalty_state": "uploaded",
            "updated_at": now,
        }, COL)
    return {"matched": True, "partner_id": partner_id, "count": len(matched)}


def mark_b1_penalty_email_sent(case_ids: list):
    data = _read_all_cases()
    now = _now_ist()
    for rid in case_ids:
        idx = _find_row_by_id(str(rid), data)
        if idx != -1:
            _batch_update_row("B1Cases", idx + 2, {
                "penalty_state": "email_sent",
                "penalty_email_state": "sent",
                "penalty_email_sent_at": now,
                "updated_at": now,
            }, COL)


def log_breach1_email(case_id, partner_name, partner_email, recipient_email,
                      case_type, language, variables_json, subject, body_preview,
                      is_test=False, status="sent", error=None):
    log_id = str(uuid.uuid4())[:8]
    row = [""] * len(ELOG_HEADERS)
    row[ELOG_COL["log_id"]] = log_id
    row[ELOG_COL["case_row_id"]] = str(case_id)
    row[ELOG_COL["partner_name"]] = str(partner_name or "")
    row[ELOG_COL["partner_email"]] = str(partner_email or "")
    row[ELOG_COL["recipient_email"]] = str(recipient_email or "")
    row[ELOG_COL["case_type"]] = str(case_type)
    row[ELOG_COL["language"]] = str(language or "both")
    row[ELOG_COL["variables_json"]] = str(variables_json or "")
    row[ELOG_COL["subject"]] = str(subject or "")
    row[ELOG_COL["body_preview"]] = str(body_preview or "")[:500]
    row[ELOG_COL["status"]] = str(status)
    row[ELOG_COL["is_test"]] = "1" if is_test else "0"
    row[ELOG_COL["sent_at"]] = _now_ist()
    row[ELOG_COL["error"]] = str(error) if error else ""
    _append_row("B1EmailLog", row)


def get_breach1_email_log(limit=100) -> list:
    rows = _read_all_email_logs()
    logs = [_elog_to_dict(r) for r in rows]
    logs.sort(key=lambda x: x.get("sent_at") or "", reverse=True)
    return logs[:limit]
