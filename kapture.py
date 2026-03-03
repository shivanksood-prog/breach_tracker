import requests
import json
import time

KAPTURE_URL = "https://wiomin.kapturecrm.com/search-ticket-by-ticket-id.html/v.2.0"
KAPTURE_COOKIE = (
    "JSESSIONID=9251DC2F4B5DB2CB30C7ADC19DEDA863; _KAPTURECRM_SESSION=; "
    "JSESSIONID=8213F0B12C9738CC4FBA9A7110FE727A; JSESSIONRID=3SDmlhjtZ1s1DmlhjtZ; "
    "_KAPTURECRM_SESSION=; _KSID=708ca9a6c5164d77b6dcb32756ad6405.3SDmlhjtZ1s1DmlhjtZ"
)


class KaptureClient:
    def __init__(self, auth_header: str):
        self.headers = {
            "Authorization": auth_header,
            "Content-Type": "application/json",
            "Cookie": KAPTURE_COOKIE,
        }

    def fetch_ticket(self, kapture_ticket_id: str):
        payload = {
            "ticket_ids": str(kapture_ticket_id),
            "history_type": "all",
            "conversation_type": "notes",
            "read_ticket_history_details": "1",
        }
        for attempt in range(3):
            try:
                resp = requests.post(
                    KAPTURE_URL, headers=self.headers, json=payload, timeout=30
                )
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list) and data:
                    return data[0]
                return None
            except Exception as e:
                if attempt == 2:
                    raise
                time.sleep(2)

    def extract_breach_fields(self, raw: dict) -> dict:
        result = {"extra_amount": None, "technician_name": None, "voluntary_tip": None}
        try:
            add_info = raw.get("additional_info", {})
            took_extra = add_info.get("took_extra_cash", {})

            # Extra amount
            for k, v in took_extra.items():
                if any(x in k.lower() for x in ["extra", "amount", "kitna", "pay_kiya"]):
                    try:
                        val = str(v).replace(",", "").strip()
                        if val and val.upper() not in ("N/A", "NA", ""):
                            result["extra_amount"] = float(val)
                    except (ValueError, TypeError):
                        pass
                    break

            # Technician name
            for k, v in took_extra.items():
                if any(x in k.lower() for x in ["naam", "name", "person", "liya"]):
                    val = str(v).strip() if v else ""
                    if val and val.upper() not in ("N/A", "NA", ""):
                        result["technician_name"] = val
                    break

            # Voluntary tip
            for k, v in took_extra.items():
                if any(x in k.lower() for x in ["voluntary", "tip"]):
                    v_str = str(v).lower()
                    result["voluntary_tip"] = "Yes" if ("yes" in v_str or "a." in v_str) else "No"
                    break

        except Exception:
            pass

        return result

    def extract_summary(self, raw: dict) -> dict:
        """Extract a clean summary dict for display in the UI."""
        summary = {}
        try:
            td = raw.get("task_details", {})
            summary["title"] = td.get("title", "")
            summary["status"] = td.get("status", "")
            summary["substatus"] = td.get("substatus", "")
            summary["disposition"] = td.get("disposition", "")
            summary["assigned_to"] = td.get("assignedToName", "")
            summary["sla_status"] = td.get("slaStatus", "")
            summary["priority"] = td.get("priority", "")
            summary["kapture_url"] = td.get("url", "")
            summary["next_follow_up"] = td.get("nextFollowUp", "")

            add_info = raw.get("additional_info", {})
            partner = add_info.get("partner_details", {})
            summary["mapped_partner"] = partner.get("mapped_partner_name", "")
            summary["zone"] = partner.get("zone", "")

            took_extra = add_info.get("took_extra_cash", {})
            summary["form_extra_cash"] = took_extra

            notes = raw.get("conversation_type", {}).get("notes", [])
            summary["latest_note"] = notes[-1]["detail"] if notes else ""
        except Exception:
            pass
        return summary
