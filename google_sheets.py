"""Google Sheets integration for reading disintermediation cases and partner emails."""

import json
import os
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build

SA_FILE = Path(__file__).parent / "service_account.json"

DISINTERMEDIATION_SHEET_ID = "1UiCQoSDGVEjVbr6qpUU5KB-5wZfg7wygJz2-gOp5eTQ"
PARTNER_SHEET_ID = "1VOKkuHN-lcx0Ps2VRSV_4IfkuG5fwFZb6UyT9gYVPeY"
ESCALATION_SHEET_ID = "1BzV24db7cuetXNqMch10knC9aygr0-jHgZLaeSprWPc"
ROHIT_CALL_TAGGING_SHEET_ID = "1C5HAqbpMxxjF76NHY-6OY0AIr1MML0vG0QfywT5P0zk"
CANCELLED_CALLING_SHEET_ID = "1Bv0Dr6vv3SvQbyZRYPKAGJGCnTcrQcItp-PRcVsgHqI"
CUSTOMER_COMPLAINT_SHEET_ID = "1c75OazHxddw5DLeje5Icaqcrk2TmqF4QxzIzR4UQf_o"

SCOPES_READONLY = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SCOPES_READWRITE = ["https://www.googleapis.com/auth/spreadsheets"]


def _get_service(readonly=True):
    scopes = SCOPES_READONLY if readonly else SCOPES_READWRITE
    # Support loading service account from env var (for cloud deployment)
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    else:
        creds = service_account.Credentials.from_service_account_file(str(SA_FILE), scopes=scopes)
    return build("sheets", "v4", credentials=creds)


def fetch_disintermediation_cases() -> list[dict]:
    """Read all rows from the disintermediation sheet."""
    service = _get_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=DISINTERMEDIATION_SHEET_ID,
        range="Sheet1!A1:U5000",
    ).execute()
    rows = result.get("values", [])
    if len(rows) < 2:
        return []

    headers = rows[0]
    cases = []
    for row in rows[1:]:
        # Pad row to match headers length
        padded = row + [""] * (len(headers) - len(row))
        case = {headers[i]: padded[i] for i in range(len(headers))}
        cases.append(case)
    return cases


def fetch_churn_feb_cases() -> list[dict]:
    """Read all rows from the Feb tab of the 6-month churn sheet."""
    service = _get_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=DISINTERMEDIATION_SHEET_ID,
        range="Feb!A1:V5000",
    ).execute()
    rows = result.get("values", [])
    if len(rows) < 2:
        return []
    headers = rows[0]
    cases = []
    for row in rows[1:]:
        padded = row + [""] * (len(headers) - len(row))
        case = {headers[i]: padded[i] for i in range(len(headers))}
        cases.append(case)
    return cases


def fetch_rohit_call_tagging_cases() -> list[dict]:
    """Read rows from Rohit Call Tagging sheet. Filter: Ops Tagging (P1) == 'Disintermediation'."""
    service = _get_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=ROHIT_CALL_TAGGING_SHEET_ID,
        range="Sheet1!A1:Z5000",
    ).execute()
    rows = result.get("values", [])
    if len(rows) < 2:
        return []
    headers = rows[0]
    cases = []
    for row in rows[1:]:
        padded = row + [""] * (len(headers) - len(row))
        case = {headers[i]: padded[i] for i in range(len(headers))}
        if (case.get("Ops Tagging (P1)") or "").strip().lower() == "disintermediation":
            cases.append(case)
    return cases


def fetch_cancelled_calling_cases() -> list[dict]:
    """Read rows from Cancelled Cx - Rajan tab. Filter: Bucketing == 'Disintermediation'."""
    service = _get_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=CANCELLED_CALLING_SHEET_ID,
        range="'Cancelled Cx - Rajan'!A1:Z5000",
    ).execute()
    rows = result.get("values", [])
    if len(rows) < 2:
        return []
    headers = rows[0]
    cases = []
    for row in rows[1:]:
        padded = row + [""] * (len(headers) - len(row))
        case = {headers[i]: padded[i] for i in range(len(headers))}
        if (case.get("Bucketing") or "").strip().lower() == "disintermediation":
            cases.append(case)
    return cases


def fetch_customer_complaint_cases() -> list[dict]:
    """Read rows from Customer Complaints Final Raw Data tab.
    Sheet has a double header: row 0 = section labels, row 1 = actual column names.
    Filter: Leakage Category == 'disintermediation' (case-insensitive).
    """
    service = _get_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=CUSTOMER_COMPLAINT_SHEET_ID,
        range="'Final Raw Data'!A1:Z5000",
    ).execute()
    rows = result.get("values", [])
    if len(rows) < 3:
        return []
    # rows[0] = section labels (skip), rows[1] = actual column headers
    headers = rows[1]
    cases = []
    for row in rows[2:]:
        padded = row + [""] * (len(headers) - len(row))
        case = {headers[i]: padded[i] for i in range(len(headers))}
        if (case.get("Leakage Category") or "").strip().lower() == "disintermediation":
            cases.append(case)
    return cases


def fetch_partner_emails() -> dict:
    """Return {partner_name_lower: email} from Email_dump tab."""
    service = _get_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=PARTNER_SHEET_ID,
        range="Email_dump!A1:M5000",
    ).execute()
    rows = result.get("values", [])
    if len(rows) < 2:
        return {}

    headers = rows[0]
    emails = {}
    for row in rows[1:]:
        padded = row + [""] * (len(headers) - len(row))
        record = {headers[i]: padded[i] for i in range(len(headers))}
        name = (record.get("Partner Name") or "").strip()
        email = (record.get("Email id of Partner") or record.get("PARTNER_EMAIL") or "").strip()
        partner_id = (record.get("PARTNER_ID") or "").strip()
        if name and email:
            emails[name.lower()] = {"email": email, "partner_id": partner_id, "name": name}
    return emails


def fetch_partner_status_emails() -> dict:
    """Fallback: read emails from 'Partner Status Final' tab. Returns {partner_name_lower: email}."""
    service = _get_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=PARTNER_SHEET_ID,
        range="'Partner Status Final'!A1:Q5000",
    ).execute()
    rows = result.get("values", [])
    if len(rows) < 3:
        return {}

    # Row 0 is metadata, row 1 is headers
    headers = rows[1]
    emails = {}
    for row in rows[2:]:
        padded = row + [""] * (len(headers) - len(row))
        record = {headers[i]: padded[i] for i in range(len(headers))}
        name = (record.get("PARTNER_NAME") or "").strip()
        email = (record.get("Email") or "").strip()
        partner_id = (record.get("PARTNER_ID") or "").strip()
        if name and email:
            emails[name.lower()] = {"email": email, "partner_id": partner_id, "name": name}
    return emails


def get_all_partner_emails() -> dict:
    """Merge emails from both tabs, Email_dump takes priority."""
    status_emails = fetch_partner_status_emails()
    dump_emails = fetch_partner_emails()
    # Merge: dump overrides status
    merged = {**status_emails, **dump_emails}
    return merged


def get_existing_escalation_customers() -> set:
    """Return set of customer mobiles already in the FP1 Escalation sheet."""
    service = _get_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=ESCALATION_SHEET_ID,
        range="'FP1 : Escalation'!C2:C5000",
    ).execute()
    rows = result.get("values", [])
    return {r[0].strip() for r in rows if r and r[0].strip()}


def fetch_fp4_cases() -> list[dict]:
    """Read all rows from the FP4 : Router Misuse tab."""
    service = _get_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=ESCALATION_SHEET_ID,
        range="'FP4 : Router Misuse'!A1:Q5000",
    ).execute()
    rows = result.get("values", [])
    if len(rows) < 2:
        return []

    headers = rows[0]
    cases = []
    for row in rows[1:]:
        padded = row + [""] * (len(headers) - len(row))
        case = {headers[i]: padded[i] for i in range(len(headers))}
        cases.append(case)
    return cases


def get_existing_fp4_customers() -> set:
    """Return set of customer details already in the FP4 sheet."""
    service = _get_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=ESCALATION_SHEET_ID,
        range="'FP4 : Router Misuse'!C2:C5000",
    ).execute()
    rows = result.get("values", [])
    return {r[0].strip() for r in rows if r and r[0].strip()}


def append_fp4_rows(rows: list[list]) -> int:
    """Append rows to FP4 Router Misuse tab. Returns number of rows appended."""
    service = _get_service(readonly=False)
    body = {"values": rows}
    result = service.spreadsheets().values().append(
        spreadsheetId=ESCALATION_SHEET_ID,
        range="'FP4 : Router Misuse'!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()
    return result.get("updates", {}).get("updatedRows", 0)


def append_escalation_rows(rows: list[list]) -> int:
    """Append rows to FP1 Escalation tab. Returns number of rows appended."""
    service = _get_service(readonly=False)
    body = {"values": rows}
    result = service.spreadsheets().values().append(
        spreadsheetId=ESCALATION_SHEET_ID,
        range="'FP1 : Escalation'!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()
    return result.get("updates", {}).get("updatedRows", 0)
