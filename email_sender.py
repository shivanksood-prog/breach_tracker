"""Email sending module with SMTP support and template rendering."""

import smtplib
import json
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


def send_email(to_email: str, subject: str, body_text: str, body_html: str,
               from_email: str = None, smtp_host: str = None, smtp_port: int = None,
               smtp_user: str = None, smtp_pass: str = None) -> dict:
    """Send email via SMTP. Returns {"ok": bool, "error": str|None}."""
    cfg = config.load()
    smtp_host = smtp_host or cfg.get("smtp_host", "smtp.gmail.com")
    smtp_port = smtp_port or int(cfg.get("smtp_port", 587))
    smtp_user = smtp_user or cfg.get("smtp_user", "")
    smtp_pass = smtp_pass or cfg.get("smtp_password", "")
    from_email = from_email or cfg.get("smtp_from_email", smtp_user)

    if not smtp_user or not smtp_pass:
        return {"ok": False, "error": "SMTP credentials not configured. Go to Settings."}

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"WIOM System Notice : Immediate Review Required <{from_email}>"
    msg["To"] = to_email

    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    msg.attach(MIMEText(body_html, "html", "utf-8"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}
