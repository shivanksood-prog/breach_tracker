import csv
import io
import requests

REFUND_HEADERS = [
    "Name of Contact",
    "Payout Link Amount",
    "Contact Phone Number",
    "Contact Email ID",
    "Send Link to Phone Number",
    "Send Link to Mail ID",
    "Contact Type",
    "Payout Purpose",
    "Payout Description",
    "Reference ID(optional)",
    "Internal notes(optional): Title",
    "Internal notes(optional): Description",
]


def generate_refund_csv(cases: list) -> str:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(REFUND_HEADERS)
    for c in cases:
        amount = c.get("extra_amount") or 0
        w.writerow([
            "Wiom User",
            int(amount) if amount else 0,
            c.get("customer_mobile", ""),
            "",
            "No",
            "No",
            "customer",
            "refund",
            "Refund for Extra money taken",
            c.get("ticket_id", ""),
            "",
            "",
        ])
    return out.getvalue()


def generate_penalty_csv(cases: list) -> str:
    """Aggregate by partner, sum amounts (negative)."""
    partners: dict = {}
    for c in cases:
        pid = c.get("current_partner_account_id", "unknown")
        amt = c.get("extra_amount") or 0
        if pid not in partners:
            partners[pid] = {"amount": 0, "name": c.get("current_partner_name", "")}
        partners[pid]["amount"] += amt

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Partner Id", "Amount", "Remark"])
    for pid, data in partners.items():
        w.writerow([pid, -abs(data["amount"]), "Breach Fundamental Principle 2 (PX2026)"])
    return out.getvalue()


def generate_partner_comms_csv(cases: list) -> str:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Partner Mobile", "Technician Name", "Amount"])
    for c in cases:
        w.writerow([
            c.get("partner_mobile", ""),
            c.get("technician_name") or c.get("install_name", ""),
            c.get("extra_amount", 0),
        ])
    return out.getvalue()


def send_to_slack(webhook_url: str, text: str, csv_string: str = None) -> dict:
    if not webhook_url:
        return {"ok": False, "error": "No Slack webhook URL configured"}

    msg_text = text
    if csv_string:
        lines = csv_string.strip().split("\n")
        preview = "\n".join(lines[:8])
        if len(csv_string) <= 2800:
            msg_text += f"\n```\n{csv_string}\n```"
        else:
            msg_text += f"\n```\n{preview}\n... ({len(lines) - 1} rows total)\n```"

    try:
        resp = requests.post(webhook_url, json={"text": msg_text}, timeout=15)
        if resp.status_code == 200:
            return {"ok": True}
        return {"ok": False, "error": resp.text}
    except Exception as e:
        return {"ok": False, "error": str(e)}
