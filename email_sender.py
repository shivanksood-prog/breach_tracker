"""Email sending module with SMTP support and template rendering."""

import smtplib
import json
import base64
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import config


# ── FP1 Template — Single template with selectable proof variables ────────────

FP1_TEMPLATE = {
    "name": "FP1 — Disintermediation (Put WIOM Connection)",
    "fundamental_number": {"hi": "मूलभूत सिद्धांत 1", "en": "Fundamental Principle 1"},
    "fundamental_title": {
        "hi": "Wiom से मिले Users को हमेशा Wiom सिस्टम में ही रखें",
        "en": "Keep Wiom Users within the Wiom System",
    },
    "case_reason": {
        "hi": "एक एक्टिव Wiom कनेक्शन को Wiom के बाहर इंटरनेट सेवा ऑफर या प्रमोट की गई।",
        "en": "A non-Wiom internet service was offered or promoted to an active Wiom connection.",
    },
    # All possible proof variables — user toggles which to include
    "proof_variables": {
        "LEAD_DATE": {
            "label": "Lead Date",
            "hi": "लीड डेट: {{LEAD_DATE}}",
            "en": "Lead Date: {{LEAD_DATE}}",
        },
        "INSTALL_DATE": {
            "label": "Install Date",
            "hi": "इंस्टॉल डेट: {{INSTALL_DATE}}",
            "en": "Install Date: {{INSTALL_DATE}}",
        },
        "LAST_SERVICE_DATE": {
            "label": "Service End Date / Expiry Date",
            "hi": "सर्विस अंतिम तिथि: {{LAST_SERVICE_DATE}}",
            "en": "Service End Date: {{LAST_SERVICE_DATE}}",
        },
        "PHONE_NUMBER_FIRST4": {
            "label": "Customer Phone Number (first 4 digits)",
            "hi": "ग्राहक फ़ोन नंबर: {{PHONE_NUMBER_FIRST4}}XXXXXX",
            "en": "Customer Phone Number: {{PHONE_NUMBER_FIRST4}}XXXXXX",
        },
    },
}


def get_template_info() -> dict:
    """Return template metadata for frontend."""
    return {
        "name": FP1_TEMPLATE["name"],
        "fundamental_number": FP1_TEMPLATE["fundamental_number"],
        "fundamental_title": FP1_TEMPLATE["fundamental_title"],
        "case_reason": FP1_TEMPLATE["case_reason"],
        "proof_variables": {
            k: {"label": v["label"], "hi_template": v["hi"], "en_template": v["en"]}
            for k, v in FP1_TEMPLATE["proof_variables"].items()
        },
    }


def _build_proof(selected_vars: list, values: dict, lang: str) -> str:
    """Build proof section from selected variables only."""
    lines = []
    for var_key in selected_vars:
        var_def = FP1_TEMPLATE["proof_variables"].get(var_key)
        if not var_def:
            continue
        template_str = var_def[lang]
        # Replace placeholder with value
        rendered = template_str.replace("{{" + var_key + "}}", str(values.get(var_key, "")))
        lines.append(rendered)
    return "\n".join(lines)


def render_email(language: str, selected_vars: list, values: dict) -> dict:
    """Render email subject and body.
    language: 'hi', 'en', or 'both'
    selected_vars: list of variable keys to include (e.g. ["LAST_SERVICE_DATE", "PHONE_NUMBER_FIRST4"])
    values: dict of variable values (e.g. {"LAST_SERVICE_DATE": "28-Dec-2025", "PHONE_NUMBER_FIRST4": "9911"})
    Returns {"subject": str, "body_text": str, "body_html": str}
    """
    t = FP1_TEMPLATE

    if language == "hi":
        subject = f"महत्वपूर्ण: Wiom {t['fundamental_number']['hi']} का उल्लंघन दर्ज — कृपया तुरंत समीक्षा करें"
        proof = _build_proof(selected_vars, values, "hi")
        body = f"""WIOM सिस्टम नोटिस।

सिस्टम रिकॉर्ड के अनुसार Wiom {t['fundamental_number']['hi']} — {t['fundamental_title']['hi']} से संबंधित उल्लंघन दर्ज हुआ है।

उल्लंघन का विवरण:
{t['case_reason']['hi']}

यह गतिविधि Wiom System: 9 मूलभूत सिद्धांतों के अंतर्गत अनुमत नहीं है।

संदर्भ विवरण:
{proof}

सिस्टम रिव्यू वर्तमान में सक्रिय है।
ऐसी गतिविधियाँ सिस्टम गवर्नेंस नियमों के अनुसार परिणाम ला सकती हैं। कृपया सुनिश्चित करें कि भविष्य में इस प्रकार की स्थिति दोबारा न बने।

यदि आपको लगता है कि यह जानकारी गलत है, तो 48 घंटे के भीतर 7836811111 पर कॉल करें। उपलब्ध रिकॉर्ड के आधार पर पुनः समीक्षा की जाएगी।

पूर्ण और आधिकारिक जानकारी के लिए Partner App में "Wiom Partner System Fundamentals" सेक्शन देखें।
https://partnerapp.wiom.in/wiom-agreement

यह सूचना स्वचालित रूप से सिस्टम द्वारा जारी की गई है।"""

    elif language == "en":
        subject = f"Important: Violation of Wiom {t['fundamental_number']['en']} Identified — Immediate Review Required"
        proof = _build_proof(selected_vars, values, "en")
        body = f"""WIOM System Notice.

System records indicate a violation related to Wiom {t['fundamental_number']['en']} — {t['fundamental_title']['en']}.

Violation Details:
{t['case_reason']['en']}

This activity is not permitted under Wiom System: 9 Fundamental Principles.

Reference Details:
{proof}

A system review is currently active.
Such activities may lead to consequences under system governance rules. Please ensure this situation does not occur again.

If you believe this information is incorrect, call 7836811111 within 48 hours. Available records will be reviewed.

For full and official details, refer to the "Wiom Partner System Fundamentals" section in the Partner App.
https://partnerapp.wiom.in/wiom-agreement

This communication has been automatically generated by the system."""

    else:  # both
        subject = f"महत्वपूर्ण: Wiom {t['fundamental_number']['hi']} का उल्लंघन दर्ज / Important: Violation of Wiom {t['fundamental_number']['en']} Identified"
        proof_hi = _build_proof(selected_vars, values, "hi")
        proof_en = _build_proof(selected_vars, values, "en")
        body = f"""WIOM सिस्टम नोटिस।

सिस्टम रिकॉर्ड के अनुसार Wiom {t['fundamental_number']['hi']} — {t['fundamental_title']['hi']} से संबंधित उल्लंघन दर्ज हुआ है।

उल्लंघन का विवरण:
{t['case_reason']['hi']}

यह गतिविधि Wiom System: 9 मूलभूत सिद्धांतों के अंतर्गत अनुमत नहीं है।

संदर्भ विवरण:
{proof_hi}

सिस्टम रिव्यू वर्तमान में सक्रिय है।
ऐसी गतिविधियाँ सिस्टम गवर्नेंस नियमों के अनुसार परिणाम ला सकती हैं। कृपया सुनिश्चित करें कि भविष्य में इस प्रकार की स्थिति दोबारा न बने।

यदि आपको लगता है कि यह जानकारी गलत है, तो 48 घंटे के भीतर 7836811111 पर कॉल करें। उपलब्ध रिकॉर्ड के आधार पर पुनः समीक्षा की जाएगी।

पूर्ण और आधिकारिक जानकारी के लिए Partner App में "Wiom Partner System Fundamentals" सेक्शन देखें।
https://partnerapp.wiom.in/wiom-agreement

यह सूचना स्वचालित रूप से सिस्टम द्वारा जारी की गई है।

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WIOM System Notice.

System records indicate a violation related to Wiom {t['fundamental_number']['en']} — {t['fundamental_title']['en']}.

Violation Details:
{t['case_reason']['en']}

This activity is not permitted under Wiom System: 9 Fundamental Principles.

Reference Details:
{proof_en}

A system review is currently active.
Such activities may lead to consequences under system governance rules. Please ensure this situation does not occur again.

If you believe this information is incorrect, call 7836811111 within 48 hours. Available records will be reviewed.

For full and official details, refer to the "Wiom Partner System Fundamentals" section in the Partner App.
https://partnerapp.wiom.in/wiom-agreement

This communication has been automatically generated by the system."""

    body_html = _body_to_html(body)
    return {"subject": subject, "body_text": body, "body_html": body_html}


# ── FP4 Template — Router Misuse ─────────────────────────────────────────────

FP4_TEMPLATE = {
    "name": "FP4 — Router Misuse",
    "fundamental_number": {"hi": "मूलभूत सिद्धांत 4", "en": "Fundamental Principle 4"},
    "fundamental_title": {
        "hi": "सिस्टम Data सही रखें और Wiom Devices का सही इस्तेमाल करें",
        "en": "Maintain Accurate System Data and Use Wiom Devices Properly",
    },
    "case_reason": {
        "hi": "NetBox का उपयोग अनुमत Wiom कनेक्शन सर्विसिंग के बाहर किया गया।",
        "en": "NetBox was used outside approved Wiom servicing conditions.",
    },
    "proof_variables": {
        "ROUTER_ID": {
            "label": "Router ID / Netbox ID",
            "hi": "Router ID: {{ROUTER_ID}}",
            "en": "Router ID: {{ROUTER_ID}}",
        },
    },
}


def get_fp4_template_info() -> dict:
    """Return FP4 template metadata for frontend."""
    return {
        "name": FP4_TEMPLATE["name"],
        "fundamental_number": FP4_TEMPLATE["fundamental_number"],
        "fundamental_title": FP4_TEMPLATE["fundamental_title"],
        "case_reason": FP4_TEMPLATE["case_reason"],
        "proof_variables": {
            k: {"label": v["label"], "hi_template": v["hi"], "en_template": v["en"]}
            for k, v in FP4_TEMPLATE["proof_variables"].items()
        },
    }


def _build_fp4_proof(selected_vars: list, values: dict, lang: str) -> str:
    """Build proof section from selected FP4 variables."""
    lines = []
    for var_key in selected_vars:
        var_def = FP4_TEMPLATE["proof_variables"].get(var_key)
        if not var_def:
            continue
        template_str = var_def[lang]
        rendered = template_str.replace("{{" + var_key + "}}", str(values.get(var_key, "")))
        lines.append(rendered)
    return "\n".join(lines)


def render_fp4_email(language: str, selected_vars: list, values: dict) -> dict:
    """Render FP4 email subject and body. Same structure as FP1."""
    t = FP4_TEMPLATE

    if language == "hi":
        subject = f"महत्वपूर्ण: Wiom {t['fundamental_number']['hi']} का उल्लंघन दर्ज — कृपया तुरंत समीक्षा करें"
        proof = _build_fp4_proof(selected_vars, values, "hi")
        body = f"""WIOM सिस्टम नोटिस।

सिस्टम रिकॉर्ड के अनुसार Wiom {t['fundamental_number']['hi']} — {t['fundamental_title']['hi']} से संबंधित उल्लंघन दर्ज हुआ है।

उल्लंघन का विवरण:
{t['case_reason']['hi']}

यह गतिविधि Wiom System: 9 मूलभूत सिद्धांतों के अंतर्गत अनुमत नहीं है।

संदर्भ विवरण:
{proof}

सिस्टम रिव्यू वर्तमान में सक्रिय है।
ऐसी गतिविधियाँ सिस्टम गवर्नेंस नियमों के अनुसार परिणाम ला सकती हैं। कृपया सुनिश्चित करें कि भविष्य में इस प्रकार की स्थिति दोबारा न बने।

यदि आपको लगता है कि यह जानकारी गलत है, तो 48 घंटे के भीतर 7836811111 पर कॉल करें। उपलब्ध रिकॉर्ड के आधार पर पुनः समीक्षा की जाएगी।

पूर्ण और आधिकारिक जानकारी के लिए Partner App में "Wiom Partner System Fundamentals" सेक्शन देखें।
https://partnerapp.wiom.in/wiom-agreement

यह सूचना स्वचालित रूप से सिस्टम द्वारा जारी की गई है।"""

    elif language == "en":
        subject = f"Important: Violation of Wiom {t['fundamental_number']['en']} Identified — Immediate Review Required"
        proof = _build_fp4_proof(selected_vars, values, "en")
        body = f"""WIOM System Notice.

System records indicate a violation related to Wiom {t['fundamental_number']['en']} — {t['fundamental_title']['en']}.

Violation Details:
{t['case_reason']['en']}

This activity is not permitted under Wiom System: 9 Fundamental Principles.

Reference Details:
{proof}

A system review is currently active.
Such activities may lead to consequences under system governance rules. Please ensure this situation does not occur again.

If you believe this information is incorrect, call 7836811111 within 48 hours. Available records will be reviewed.

For full and official details, refer to the "Wiom Partner System Fundamentals" section in the Partner App.
https://partnerapp.wiom.in/wiom-agreement

This communication has been automatically generated by the system."""

    else:  # both
        subject = f"महत्वपूर्ण: Wiom {t['fundamental_number']['hi']} का उल्लंघन दर्ज / Important: Violation of Wiom {t['fundamental_number']['en']} Identified"
        proof_hi = _build_fp4_proof(selected_vars, values, "hi")
        proof_en = _build_fp4_proof(selected_vars, values, "en")
        body = f"""WIOM सिस्टम नोटिस।

सिस्टम रिकॉर्ड के अनुसार Wiom {t['fundamental_number']['hi']} — {t['fundamental_title']['hi']} से संबंधित उल्लंघन दर्ज हुआ है।

उल्लंघन का विवरण:
{t['case_reason']['hi']}

यह गतिविधि Wiom System: 9 मूलभूत सिद्धांतों के अंतर्गत अनुमत नहीं है।

संदर्भ विवरण:
{proof_hi}

सिस्टम रिव्यू वर्तमान में सक्रिय है।
ऐसी गतिविधियाँ सिस्टम गवर्नेंस नियमों के अनुसार परिणाम ला सकती हैं। कृपया सुनिश्चित करें कि भविष्य में इस प्रकार की स्थिति दोबारा न बने।

यदि आपको लगता है कि यह जानकारी गलत है, तो 48 घंटे के भीतर 7836811111 पर कॉल करें। उपलब्ध रिकॉर्ड के आधार पर पुनः समीक्षा की जाएगी।

पूर्ण और आधिकारिक जानकारी के लिए Partner App में "Wiom Partner System Fundamentals" सेक्शन देखें।
https://partnerapp.wiom.in/wiom-agreement

यह सूचना स्वचालित रूप से सिस्टम द्वारा जारी की गई है।

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WIOM System Notice.

System records indicate a violation related to Wiom {t['fundamental_number']['en']} — {t['fundamental_title']['en']}.

Violation Details:
{t['case_reason']['en']}

This activity is not permitted under Wiom System: 9 Fundamental Principles.

Reference Details:
{proof_en}

A system review is currently active.
Such activities may lead to consequences under system governance rules. Please ensure this situation does not occur again.

If you believe this information is incorrect, call 7836811111 within 48 hours. Available records will be reviewed.

For full and official details, refer to the "Wiom Partner System Fundamentals" section in the Partner App.
https://partnerapp.wiom.in/wiom-agreement

This communication has been automatically generated by the system."""

    # Convert to simple HTML
    body_html = _body_to_html(body)
    return {"subject": subject, "body_text": body, "body_html": body_html}


# ── FP2 Template — Extra Cash ────────────────────────────────────────────────

FP2_TEMPLATE = {
    "name": "FP2 — Extra Cash Collection",
    "fundamental_number": {"hi": "मूलभूत सिद्धांत 2", "en": "Fundamental Principle 2"},
    "fundamental_title": {
        "hi": "Wiom Users से कोई अतिरिक्त पैसा न लें",
        "en": "Do Not Collect Any Extra Money from Wiom Users",
    },
    "case_reason": {
        "hi": "ग्राहक से Wiom सिस्टम के बाहर अतिरिक्त राशि ली गई।",
        "en": "Extra money was collected from the customer outside the Wiom system.",
    },
    "proof_variables": {
        "CUSTOMER_MOBILE": {
            "label": "Customer Phone (first 4 digits)",
            "hi": "ग्राहक फ़ोन नंबर: {{CUSTOMER_MOBILE}}XXXXXX",
            "en": "Customer Phone Number: {{CUSTOMER_MOBILE}}XXXXXX",
        },
        "EXTRA_AMOUNT": {
            "label": "Extra Amount Collected",
            "hi": "अतिरिक्त राशि: ₹{{EXTRA_AMOUNT}}",
            "en": "Extra Amount: ₹{{EXTRA_AMOUNT}}",
        },
        "TICKET_ID": {
            "label": "Kapture Ticket ID",
            "hi": "Kapture Ticket: {{TICKET_ID}}",
            "en": "Kapture Ticket: {{TICKET_ID}}",
        },
    },
}


def get_fp2_template_info() -> dict:
    """Return FP2 template metadata for frontend."""
    return {
        "name": FP2_TEMPLATE["name"],
        "fundamental_number": FP2_TEMPLATE["fundamental_number"],
        "fundamental_title": FP2_TEMPLATE["fundamental_title"],
        "case_reason": FP2_TEMPLATE["case_reason"],
        "proof_variables": {
            k: {"label": v["label"], "hi_template": v["hi"], "en_template": v["en"]}
            for k, v in FP2_TEMPLATE["proof_variables"].items()
        },
    }


def _build_fp2_proof(selected_vars: list, values: dict, lang: str) -> str:
    lines = []
    for var_key in selected_vars:
        var_def = FP2_TEMPLATE["proof_variables"].get(var_key)
        if not var_def:
            continue
        template_str = var_def[lang]
        rendered = template_str.replace("{{" + var_key + "}}", str(values.get(var_key, "")))
        lines.append(rendered)
    return "\n".join(lines)


def render_fp2_email(language: str, selected_vars: list, values: dict) -> dict:
    """Render FP2 email subject and body. Same structure as FP1/FP4."""
    t = FP2_TEMPLATE

    if language == "hi":
        subject = f"महत्वपूर्ण: Wiom {t['fundamental_number']['hi']} का उल्लंघन दर्ज — कृपया तुरंत समीक्षा करें"
        proof = _build_fp2_proof(selected_vars, values, "hi")
        body = f"""WIOM सिस्टम नोटिस।

सिस्टम रिकॉर्ड के अनुसार Wiom {t['fundamental_number']['hi']} — {t['fundamental_title']['hi']} से संबंधित उल्लंघन दर्ज हुआ है।

उल्लंघन का विवरण:
{t['case_reason']['hi']}

यह गतिविधि Wiom System: 9 मूलभूत सिद्धांतों के अंतर्गत अनुमत नहीं है।

संदर्भ विवरण:
{proof}

सिस्टम रिव्यू वर्तमान में सक्रिय है।
ऐसी गतिविधियाँ सिस्टम गवर्नेंस नियमों के अनुसार परिणाम ला सकती हैं। कृपया सुनिश्चित करें कि भविष्य में इस प्रकार की स्थिति दोबारा न बने।

यदि आपको लगता है कि यह जानकारी गलत है, तो 48 घंटे के भीतर 7836811111 पर कॉल करें। उपलब्ध रिकॉर्ड के आधार पर पुनः समीक्षा की जाएगी।

पूर्ण और आधिकारिक जानकारी के लिए Partner App में "Wiom Partner System Fundamentals" सेक्शन देखें।
https://partnerapp.wiom.in/wiom-agreement

यह सूचना स्वचालित रूप से सिस्टम द्वारा जारी की गई है।"""

    elif language == "en":
        subject = f"Important: Violation of Wiom {t['fundamental_number']['en']} Identified — Immediate Review Required"
        proof = _build_fp2_proof(selected_vars, values, "en")
        body = f"""WIOM System Notice.

System records indicate a violation related to Wiom {t['fundamental_number']['en']} — {t['fundamental_title']['en']}.

Violation Details:
{t['case_reason']['en']}

This activity is not permitted under Wiom System: 9 Fundamental Principles.

Reference Details:
{proof}

A system review is currently active.
Such activities may lead to consequences under system governance rules. Please ensure this situation does not occur again.

If you believe this information is incorrect, call 7836811111 within 48 hours. Available records will be reviewed.

For full and official details, refer to the "Wiom Partner System Fundamentals" section in the Partner App.
https://partnerapp.wiom.in/wiom-agreement

This communication has been automatically generated by the system."""

    else:  # both
        subject = f"महत्वपूर्ण: Wiom {t['fundamental_number']['hi']} का उल्लंघन दर्ज / Important: Violation of Wiom {t['fundamental_number']['en']} Identified"
        proof_hi = _build_fp2_proof(selected_vars, values, "hi")
        proof_en = _build_fp2_proof(selected_vars, values, "en")
        body = f"""WIOM सिस्टम नोटिस।

सिस्टम रिकॉर्ड के अनुसार Wiom {t['fundamental_number']['hi']} — {t['fundamental_title']['hi']} से संबंधित उल्लंघन दर्ज हुआ है।

उल्लंघन का विवरण:
{t['case_reason']['hi']}

यह गतिविधि Wiom System: 9 मूलभूत सिद्धांतों के अंतर्गत अनुमत नहीं है।

संदर्भ विवरण:
{proof_hi}

सिस्टम रिव्यू वर्तमान में सक्रिय है।
ऐसी गतिविधियाँ सिस्टम गवर्नेंस नियमों के अनुसार परिणाम ला सकती हैं। कृपया सुनिश्चित करें कि भविष्य में इस प्रकार की स्थिति दोबारा न बने।

यदि आपको लगता है कि यह जानकारी गलत है, तो 48 घंटे के भीतर 7836811111 पर कॉल करें। उपलब्ध रिकॉर्ड के आधार पर पुनः समीक्षा की जाएगी।

पूर्ण और आधिकारिक जानकारी के लिए Partner App में "Wiom Partner System Fundamentals" सेक्शन देखें।
https://partnerapp.wiom.in/wiom-agreement

यह सूचना स्वचालित रूप से सिस्टम द्वारा जारी की गई है।

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WIOM System Notice.

System records indicate a violation related to Wiom {t['fundamental_number']['en']} — {t['fundamental_title']['en']}.

Violation Details:
{t['case_reason']['en']}

This activity is not permitted under Wiom System: 9 Fundamental Principles.

Reference Details:
{proof_en}

A system review is currently active.
Such activities may lead to consequences under system governance rules. Please ensure this situation does not occur again.

If you believe this information is incorrect, call 7836811111 within 48 hours. Available records will be reviewed.

For full and official details, refer to the "Wiom Partner System Fundamentals" section in the Partner App.
https://partnerapp.wiom.in/wiom-agreement

This communication has been automatically generated by the system."""

    body_html = _body_to_html(body)
    return {"subject": subject, "body_text": body, "body_html": body_html}


def _body_to_html(body: str) -> str:
    """Convert plain text body to simple HTML."""
    body_html = "<div style='font-family:sans-serif;font-size:14px;line-height:1.6;color:#333'>"
    for line in body.split("\n"):
        if line.strip() == "":
            body_html += "<br>"
        elif line.startswith("━"):
            body_html += "<hr style='border:none;border-top:2px solid #E8235A;margin:24px 0'>"
        elif line.startswith("WIOM"):
            body_html += f"<p style='font-weight:700;font-size:16px;color:#E8235A'>{line}</p>"
        elif ":" in line and not line.startswith("http"):
            parts = line.split(":", 1)
            body_html += f"<p><strong>{parts[0]}:</strong>{parts[1]}</p>"
        elif line.startswith("http"):
            body_html += f"<p><a href='{line}' style='color:#E8235A'>{line}</a></p>"
        else:
            body_html += f"<p>{line}</p>"
    body_html += "</div>"
    return body_html


def _get_service_account_info():
    """Load Google service account credentials from env var or file."""
    import os
    from pathlib import Path
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if sa_json:
        return json.loads(sa_json)
    sa_file = Path(__file__).parent / "service_account.json"
    if sa_file.exists():
        with open(sa_file) as f:
            return json.load(f)
    return None


def _send_via_gmail_api(from_email: str, msg: MIMEMultipart) -> dict:
    """Send email via Gmail API using service account with domain-wide delegation."""
    info = _get_service_account_info()
    if not info:
        return {"ok": False, "error": "No service account configured for Gmail API"}

    from google.oauth2 import service_account as sa
    from googleapiclient.discovery import build

    SCOPES = ['https://www.googleapis.com/auth/gmail.send']
    creds = sa.Credentials.from_service_account_info(info, scopes=SCOPES)
    delegated = creds.with_subject(from_email)

    service = build('gmail', 'v1', credentials=delegated, cache_discovery=False)
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId='me', body={'raw': raw}).execute()
    return {"ok": True}


def _send_via_smtp(smtp_host: str, smtp_port: int, smtp_user: str,
                   smtp_pass: str, msg: MIMEMultipart) -> dict:
    """Send email via SMTP (works locally, blocked on Railway)."""
    if smtp_port == 465:
        with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=10) as server:
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
    else:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
    return {"ok": True}


def send_email(to_email: str, subject: str, body_text: str, body_html: str,
               from_email: str = None, smtp_host: str = None, smtp_port: int = None,
               smtp_user: str = None, smtp_pass: str = None) -> dict:
    """Send email via Gmail API (primary) with SMTP fallback.
    Returns {"ok": bool, "error": str|None}."""
    cfg = config.load()
    smtp_host = smtp_host or cfg.get("smtp_host", "smtp.gmail.com")
    smtp_port = smtp_port or int(cfg.get("smtp_port", 587))
    smtp_user = smtp_user or cfg.get("smtp_user", "")
    smtp_pass = smtp_pass or cfg.get("smtp_password", "")
    from_email = from_email or cfg.get("smtp_from_email") or smtp_user

    if not from_email:
        return {"ok": False, "error": "No sender email configured. Go to Settings."}

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"WIOM System Notice : Immediate Review Required <{from_email}>"
    msg["To"] = to_email
    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    msg.attach(MIMEText(body_html, "html", "utf-8"))

    # Try Gmail API first (works on Railway where SMTP is blocked)
    sa_info = _get_service_account_info()
    if sa_info:
        try:
            return _send_via_gmail_api(from_email, msg)
        except Exception as api_err:
            gmail_err = str(api_err)
            # If Gmail API fails, fall back to SMTP
            if smtp_user and smtp_pass:
                try:
                    return _send_via_smtp(smtp_host, smtp_port, smtp_user, smtp_pass, msg)
                except Exception as smtp_err:
                    return {"ok": False, "error": f"Gmail API failed ({gmail_err}). SMTP fallback also failed: {smtp_err}"}
            return {"ok": False, "error": f"Gmail API failed: {gmail_err}"}

    # No service account — try SMTP directly
    if not smtp_user or not smtp_pass:
        return {"ok": False, "error": "No service account and no SMTP credentials configured. Go to Settings."}
    try:
        return _send_via_smtp(smtp_host, smtp_port, smtp_user, smtp_pass, msg)
    except Exception as e:
        return {"ok": False, "error": str(e)}
