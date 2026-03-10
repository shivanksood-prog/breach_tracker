"""Breach 2 data layer backed by Google Sheets (replaces SQLite for cases table).

Same function signatures as db.py so app.py can swap imports.
"""

from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
import json
import os
from pathlib import Path

SA_FILE = Path(__file__).parent / "service_account.json"
SHEET_ID = os.environ.get(
    "BREACH2_SHEET_ID", "1tYkZxNSFbAgTa7lfEsdcFEmbp8w6qcQ9KSV7wBo1eOc"
)

HEADERS = [
    "ticket_id", "kapture_ticket_id", "ticket_added_time_ist", "customer_mobile",
    "current_partner_account_id", "current_partner_name", "zone", "partner_mobile",
    "new_install_flag", "install_emp_role", "install_emp_id", "install_name",
    "extra_amount", "technician_name", "voluntary_tip", "state", "detected_at",
    "customer_refunded_at", "customer_comms_at", "partner_penalty_at",
    "refund_payout_link", "previous_state",
]
COL = {h: i for i, h in enumerate(HEADERS)}

VALID_TRANSITIONS = {
    "detected":          ["customer_refunded"],
    "customer_refunded": ["customer_comms"],
    "customer_comms":    ["partner_penalty"],
    "partner_penalty":   [],
}

STATE_TS_COL = {
    "customer_refunded": "customer_refunded_at",
    "customer_comms":    "customer_comms_at",
    "partner_penalty":   "partner_penalty_at",
}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


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


def _read_all() -> list[list]:
    """Read all data rows (excluding header) from the Cases tab."""
    svc = _get_service()
    result = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range="Cases!A2:V10000",
    ).execute()
    return result.get("values", [])


def _row_to_dict(row: list) -> dict:
    """Convert a sheet row to a dict, padding if needed."""
    padded = row + [""] * (len(HEADERS) - len(row))
    obj = {}
    for i, h in enumerate(HEADERS):
        val = padded[i]
        obj[h] = None if val == "" else val
    if obj.get("extra_amount") is not None:
        try:
            obj["extra_amount"] = float(obj["extra_amount"])
        except (ValueError, TypeError):
            obj["extra_amount"] = None
    return obj


def _find_row_index(ticket_id: str, data: list[list] = None) -> int:
    """Return 0-based index in data list, or -1 if not found."""
    if data is None:
        data = _read_all()
    tid = str(ticket_id)
    for i, row in enumerate(data):
        if row and str(row[0]) == tid:
            return i
    return -1


def _update_cell(row_num: int, col_name: str, value):
    """Update a single cell. row_num is 1-based sheet row (data starts at row 2)."""
    svc = _get_service()
    col_idx = COL[col_name]
    # Convert col index to letter (A=0, B=1, ..., V=21)
    col_letter = chr(ord("A") + col_idx)
    cell = f"Cases!{col_letter}{row_num}"
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=cell,
        valueInputOption="RAW",
        body={"values": [[value if value is not None else ""]]},
    ).execute()


def _batch_update_row(row_num: int, updates: dict):
    """Update multiple columns in a single row efficiently."""
    svc = _get_service()
    requests = []
    for col_name, value in updates.items():
        col_idx = COL[col_name]
        col_letter = chr(ord("A") + col_idx)
        cell = f"Cases!{col_letter}{row_num}"
        requests.append({
            "range": cell,
            "values": [[value if value is not None else ""]],
        })
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={
            "valueInputOption": "RAW",
            "data": requests,
        },
    ).execute()


# ── Public API (matches db.py signatures) ────────────────────────────────────

def get_all_cases(state=None, zone=None, search=None) -> list:
    rows = _read_all()
    cases = [_row_to_dict(r) for r in rows]

    if state and state != "all":
        cases = [c for c in cases if c.get("state") == state]
    if zone and zone != "all":
        cases = [c for c in cases if zone.lower() in (c.get("zone") or "").lower()]
    if search:
        s = search.lower()
        cases = [c for c in cases if
                 s in (c.get("ticket_id") or "").lower() or
                 s in (c.get("customer_mobile") or "").lower() or
                 s in (c.get("current_partner_name") or "").lower() or
                 s in (c.get("zone") or "").lower()]

    cases.sort(key=lambda c: c.get("detected_at") or "", reverse=True)
    return cases


def get_case(ticket_id: str):
    data = _read_all()
    idx = _find_row_index(ticket_id, data)
    if idx == -1:
        return None
    case = _row_to_dict(data[idx])
    case["comms_log"] = []  # No comms log in sheets version
    return case


def get_summary() -> dict:
    rows = _read_all()
    cases = [_row_to_dict(r) for r in rows]
    states = ["detected", "customer_refunded", "customer_comms", "partner_penalty"]
    by_state = {s: 0 for s in states}
    total_amount = 0
    for c in cases:
        st = c.get("state")
        if st in by_state:
            by_state[st] += 1
        total_amount += (c.get("extra_amount") or 0)
    return {
        "total": len(cases),
        "by_state": by_state,
        "total_amount": round(total_amount, 2),
    }


def get_all_zones() -> list:
    rows = _read_all()
    zones = set()
    for r in rows:
        padded = r + [""] * (len(HEADERS) - len(r))
        z = padded[COL["zone"]]
        if z:
            zones.add(z)
    return sorted(zones)


def advance_state(ticket_id: str, new_state: str) -> bool:
    data = _read_all()
    idx = _find_row_index(ticket_id, data)
    if idx == -1:
        return False

    case = _row_to_dict(data[idx])
    current = case.get("state") or ""
    valid = VALID_TRANSITIONS.get(current, [])
    if new_state not in valid:
        return False

    row_num = idx + 2  # Sheet row (1-indexed, header is row 1)
    ts = _now_ist()
    updates = {"state": new_state, "previous_state": current}
    ts_col = STATE_TS_COL.get(new_state)
    if ts_col:
        updates[ts_col] = ts

    _batch_update_row(row_num, updates)
    return True


def undo_state(ticket_id: str) -> bool:
    data = _read_all()
    idx = _find_row_index(ticket_id, data)
    if idx == -1:
        return False

    case = _row_to_dict(data[idx])
    prev = case.get("previous_state")
    if not prev:
        return False

    row_num = idx + 2
    current = case.get("state") or ""
    updates = {"state": prev, "previous_state": ""}
    ts_col = STATE_TS_COL.get(current)
    if ts_col:
        updates[ts_col] = ""

    _batch_update_row(row_num, updates)
    return True


def mark_refunded_by_mobile(mobile: str, payout_link_id: str) -> dict:
    """Find case by customer_mobile in 'detected' state, advance to customer_refunded."""
    payout_url = f"https://payout-links.razorpay.com/v1/payout-links/{payout_link_id}/view/#/"
    data = _read_all()

    clean = mobile.lstrip("+")
    if clean.startswith("91") and len(clean) > 10:
        clean = clean[2:]

    for i, row in enumerate(data):
        padded = row + [""] * (len(HEADERS) - len(row))
        mob = str(padded[COL["customer_mobile"]] or "")
        st = str(padded[COL["state"]] or "")
        if st == "detected" and (mob == clean or mob == mobile):
            row_num = i + 2
            tid = str(padded[COL["ticket_id"]])
            ts = _now_ist()
            _batch_update_row(row_num, {
                "state": "customer_refunded",
                "previous_state": "detected",
                "customer_refunded_at": ts,
                "refund_payout_link": payout_url,
            })
            return {"matched": True, "mobile": mobile, "ticket_id": tid, "payout_link": payout_url}

    return {"matched": False, "mobile": mobile}


def mark_penalty_by_upload(partner_id: str) -> dict:
    """Find cases by partner in 'customer_comms' state, advance to partner_penalty."""
    data = _read_all()
    advanced = []

    for i, row in enumerate(data):
        padded = row + [""] * (len(HEADERS) - len(row))
        pid = str(padded[COL["current_partner_account_id"]] or "")
        st = str(padded[COL["state"]] or "")
        if st == "customer_comms" and pid == partner_id:
            row_num = i + 2
            tid = str(padded[COL["ticket_id"]])
            ts = _now_ist()
            _batch_update_row(row_num, {
                "state": "partner_penalty",
                "previous_state": "customer_comms",
                "partner_penalty_at": ts,
            })
            advanced.append(tid)

    if not advanced:
        return {"matched": False, "partner_id": partner_id}
    return {"matched": True, "partner_id": partner_id, "ticket_ids": advanced, "count": len(advanced)}


def upsert_case(data_dict: dict):
    """Insert or update a case row in the sheet."""
    tid = str(data_dict.get("ticket_id", ""))
    if not tid:
        return

    all_data = _read_all()
    idx = _find_row_index(tid, all_data)

    if idx != -1:
        # Update safe fields only
        safe = {
            "ticket_added_time_ist", "customer_mobile", "current_partner_account_id",
            "current_partner_name", "zone", "partner_mobile", "new_install_flag",
            "install_emp_role", "install_emp_id", "install_name", "kapture_ticket_id",
        }
        updates = {k: v for k, v in data_dict.items() if k in safe and k in COL}
        if updates:
            _batch_update_row(idx + 2, updates)
    else:
        # New row
        svc = _get_service()
        new_row = []
        for h in HEADERS:
            if h == "state":
                new_row.append(data_dict.get("state", "detected"))
            elif h == "detected_at":
                new_row.append(data_dict.get("detected_at") or _now_ist())
            else:
                new_row.append(str(data_dict.get(h, "") or ""))
        svc.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range="Cases!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [new_row]},
        ).execute()


def update_kapture_fields(ticket_id: str, extra_amount, technician_name, voluntary_tip, raw_json: str):
    """Update Kapture-enriched fields for a case."""
    data = _read_all()
    idx = _find_row_index(ticket_id, data)
    if idx == -1:
        return
    updates = {}
    if extra_amount is not None:
        updates["extra_amount"] = str(extra_amount)
    if technician_name is not None:
        updates["technician_name"] = str(technician_name)
    if voluntary_tip is not None:
        updates["voluntary_tip"] = str(voluntary_tip)
    if updates:
        _batch_update_row(idx + 2, updates)
