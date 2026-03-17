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


def _build_proof_from_template(template: dict, selected_vars: list, values: dict, lang: str) -> str:
    """Build proof section from selected variables of any template."""
    lines = []
    for var_key in selected_vars:
        var_def = template["proof_variables"].get(var_key)
        if not var_def:
            continue
        template_str = var_def[lang]
        rendered = template_str.replace("{{" + var_key + "}}", str(values.get(var_key, "")))
        lines.append(rendered)
    return "\n".join(lines)


def _build_proof(selected_vars: list, values: dict, lang: str) -> str:
    return _build_proof_from_template(FP1_TEMPLATE, selected_vars, values, lang)


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


# ── FP1 Penalty Template ───────────────────────────────────────────────────

FP1_PENALTY_TEMPLATE = {
    "name": "FP1 — Disintermediation Penalty",
    "fundamental_number": {"hi": "मूलभूत सिद्धांत 1", "en": "Fundamental Principle 1"},
    "fundamental_title": {
        "hi": "Wiom से मिले Users को हमेशा Wiom सिस्टम में ही रखें",
        "en": "Keep Wiom Users within the Wiom System",
    },
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


def get_fp1_penalty_template_info() -> dict:
    """Return FP1 penalty template metadata for frontend."""
    return {
        "name": FP1_PENALTY_TEMPLATE["name"],
        "fundamental_number": FP1_PENALTY_TEMPLATE["fundamental_number"],
        "fundamental_title": FP1_PENALTY_TEMPLATE["fundamental_title"],
        "proof_variables": {
            k: {"label": v["label"], "hi_template": v["hi"], "en_template": v["en"]}
            for k, v in FP1_PENALTY_TEMPLATE["proof_variables"].items()
        },
    }


def render_fp1_penalty_email(language: str, selected_vars: list, values: dict) -> dict:
    """Render FP1 penalty email subject and body."""
    t = FP1_PENALTY_TEMPLATE
    proof_fn = lambda lang: _build_proof_from_template(t, selected_vars, values, lang)

    if language == "hi":
        subject = "महत्वपूर्ण: Wiom मूलभूत सिद्धांत 1 के उल्लंघन पर ₹2000 का दंड लागू किया गया"
        proof = proof_fn("hi")
        body = f"""WIOM सिस्टम नोटिस।

सिस्टम रिकॉर्ड के अनुसार Wiom {t['fundamental_number']['hi']} — {t['fundamental_title']['hi']} से संबंधित उल्लंघन दर्ज हुआ है।

उल्लंघन का विवरण:
एक एक्टिव Wiom कनेक्शन को Wiom के बाहर इंटरनेट सेवा ऑफर या प्रमोट की गई।

यह गतिविधि Wiom System: 9 मूलभूत सिद्धांतों के अंतर्गत अनुमत नहीं है।

संदर्भ विवरण:
{proof}

इस उल्लंघन के परिणामस्वरूप, सिस्टम द्वारा ₹2000 का दंड आपके खाते पर लागू किया गया है।

यदि आपको लगता है कि यह जानकारी गलत है, तो 48 घंटे के भीतर 7836811111 पर कॉल करें। उपलब्ध रिकॉर्ड के आधार पर पुनः समीक्षा की जाएगी।

पूर्ण और आधिकारिक जानकारी के लिए Partner App में "Wiom Partner System Fundamentals" सेक्शन देखें।
https://partnerapp.wiom.in/wiom-agreement

यह सूचना स्वचालित रूप से सिस्टम द्वारा जारी की गई है।"""

    elif language == "en":
        subject = "Important: Rs. 2000 Penalty Applied for Violation of Wiom Fundamental Principle 1"
        proof = proof_fn("en")
        body = f"""WIOM System Notice.

System records indicate a violation related to Wiom {t['fundamental_number']['en']} — {t['fundamental_title']['en']}.

Violation Details:
A non-Wiom internet service was offered or promoted to an active Wiom connection.

This activity is not permitted under Wiom System: 9 Fundamental Principles.

Reference Details:
{proof}

As a result of this violation, a penalty of Rs. 2000 has been applied to your account by the system.

If you believe this information is incorrect, call 7836811111 within 48 hours. Available records will be reviewed.

For full and official details, refer to the "Wiom Partner System Fundamentals" section in the Partner App.
https://partnerapp.wiom.in/wiom-agreement

This communication has been automatically generated by the system."""

    else:  # both
        subject = "महत्वपूर्ण: Wiom मूलभूत सिद्धांत 1 के उल्लंघन पर ₹2000 का दंड लागू किया गया / Important: Rs. 2000 Penalty Applied for Violation of Wiom Fundamental Principle 1"
        proof_hi = proof_fn("hi")
        proof_en = proof_fn("en")
        body = f"""WIOM सिस्टम नोटिस।

सिस्टम रिकॉर्ड के अनुसार Wiom {t['fundamental_number']['hi']} — {t['fundamental_title']['hi']} से संबंधित उल्लंघन दर्ज हुआ है।

उल्लंघन का विवरण:
एक एक्टिव Wiom कनेक्शन को Wiom के बाहर इंटरनेट सेवा ऑफर या प्रमोट की गई।

यह गतिविधि Wiom System: 9 मूलभूत सिद्धांतों के अंतर्गत अनुमत नहीं है।

संदर्भ विवरण:
{proof_hi}

इस उल्लंघन के परिणामस्वरूप, सिस्टम द्वारा ₹2000 का दंड आपके खाते पर लागू किया गया है।

यदि आपको लगता है कि यह जानकारी गलत है, तो 48 घंटे के भीतर 7836811111 पर कॉल करें। उपलब्ध रिकॉर्ड के आधार पर पुनः समीक्षा की जाएगी।

पूर्ण और आधिकारिक जानकारी के लिए Partner App में "Wiom Partner System Fundamentals" सेक्शन देखें।
https://partnerapp.wiom.in/wiom-agreement

यह सूचना स्वचालित रूप से सिस्टम द्वारा जारी की गई है।

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WIOM System Notice.

System records indicate a violation related to Wiom {t['fundamental_number']['en']} — {t['fundamental_title']['en']}.

Violation Details:
A non-Wiom internet service was offered or promoted to an active Wiom connection.

This activity is not permitted under Wiom System: 9 Fundamental Principles.

Reference Details:
{proof_en}

As a result of this violation, a penalty of Rs. 2000 has been applied to your account by the system.

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
    return _build_proof_from_template(FP4_TEMPLATE, selected_vars, values, lang)


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
        "hi": "सभी Payments Wiom सिस्टम के ज़रिए ही Process करें",
        "en": "All Payments Must Be Processed Through the Wiom System",
    },
    "case_reason": {
        "hi": "",  # Inline in body — not used separately
        "en": "",
    },
    "proof_variables": {
        "MASKED_NUMBER": {
            "label": "Customer Phone (masked)",
            "hi": "ग्राहक: {{MASKED_NUMBER}}",
            "en": "Customer: {{MASKED_NUMBER}}",
        },
        "AMOUNT_COLLECTED": {
            "label": "Amount Collected (₹)",
            "hi": "ली गई राशि: ₹{{AMOUNT_COLLECTED}}",
            "en": "Amount Collected: ₹{{AMOUNT_COLLECTED}}",
        },
        "AMOUNT_REFUNDED": {
            "label": "Amount Refunded to Customer (₹)",
            "hi": "ग्राहक को वापस: ₹{{AMOUNT_REFUNDED}}",
            "en": "Refunded to Customer: ₹{{AMOUNT_REFUNDED}}",
        },
        "AMOUNT_RECOVERED": {
            "label": "Amount Recovered from Wallet (₹)",
            "hi": "वॉलेट से वसूल: ₹{{AMOUNT_RECOVERED}}",
            "en": "Recovered from Wallet: ₹{{AMOUNT_RECOVERED}}",
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
    return _build_proof_from_template(FP2_TEMPLATE, selected_vars, values, lang)


def render_fp2_email(language: str, selected_vars: list, values: dict) -> dict:
    """Render FP2 email — custom body with inline violation details per Google Doc."""
    v = {k: str(values.get(k, "")) for k in ("MASKED_NUMBER", "AMOUNT_COLLECTED", "AMOUNT_REFUNDED", "AMOUNT_RECOVERED")}

    if language == "hi":
        subject = "महत्वपूर्ण: Wiom मूलभूत सिद्धांत 2 का उल्लंघन — सिस्टम द्वारा राशि वसूली"
        body = f"""Wiom सिस्टम संदेश

सिस्टम रिकॉर्ड के अनुसार Wiom मूलभूत सिद्धांत 2 — सभी Payments Wiom सिस्टम के ज़रिए ही Process करें से संबंधित उल्लंघन दर्ज हुआ है।

उल्लंघन का विवरण:
आपके या आपके कर्मचारी द्वारा ग्राहक {v['MASKED_NUMBER']} से ₹{v['AMOUNT_COLLECTED']} की राशि ली गई थी।

Wiom सिस्टम नियमों के अनुसार सभी भुगतान केवल Wiom सिस्टम के माध्यम से ही लिए जा सकते हैं। ग्राहक से सीधे भुगतान लेना अनुमत नहीं है।

इस कारण सिस्टम द्वारा:
• ग्राहक को ₹{v['AMOUNT_REFUNDED']} की राशि वापस कर दी गई है
• आपके वॉलेट से ₹{v['AMOUNT_RECOVERED']} की राशि वसूल की गई है

यह गतिविधि Wiom System: 9 मूलभूत सिद्धांतों के अंतर्गत अनुमत नहीं है।

कृपया सुनिश्चित करें कि भविष्य में:
• ग्राहक से कोई भी अतिरिक्त भुगतान सीधे न लिया जाए
• सभी भुगतान केवल Wiom सिस्टम के माध्यम से ही प्रोसेस किए जाएँ

यदि आपको लगता है कि यह जानकारी गलत है, तो 48 घंटे के भीतर 7836811111 पर कॉल करें। उपलब्ध रिकॉर्ड के आधार पर पुनः समीक्षा की जाएगी।

पूर्ण और आधिकारिक जानकारी के लिए Partner App में "Wiom Partner System Fundamentals" सेक्शन देखें।
https://partnerapp.wiom.in/wiom-agreement

यह सूचना स्वचालित रूप से सिस्टम द्वारा जारी की गई है।"""

    elif language == "en":
        subject = "Important: Violation of Wiom Fundamental Principle 2 — Amount Recovered by System"
        body = f"""Wiom System Message

System records indicate a violation related to Wiom Fundamental Principle 2 — All Payments Must Be Processed Through the Wiom System.

Violation Details:
You or your employee collected ₹{v['AMOUNT_COLLECTED']} from customer {v['MASKED_NUMBER']}.

As per Wiom system rules, all payments must be processed only through the Wiom system. Direct collection of money from customers is not permitted.

As a result:
• ₹{v['AMOUNT_REFUNDED']} has been refunded to the customer by the system
• ₹{v['AMOUNT_RECOVERED']} has been recovered from your wallet

This activity is not permitted under Wiom System: 9 Fundamental Principles.

Please ensure that in the future:
• No additional payments are collected directly from customers
• All payments are processed only through the Wiom system

If you believe this information is incorrect, call 7836811111 within 48 hours. Available records will be reviewed.

For full and official details, refer to the "Wiom Partner System Fundamentals" section in the Partner App.
https://partnerapp.wiom.in/wiom-agreement

This communication has been automatically generated by the system."""

    else:  # both
        subject = "महत्वपूर्ण: Wiom मूलभूत सिद्धांत 2 का उल्लंघन — सिस्टम द्वारा राशि वसूली / Important: Violation of Wiom Fundamental Principle 2 — Amount Recovered by System"
        body = f"""Wiom सिस्टम संदेश

सिस्टम रिकॉर्ड के अनुसार Wiom मूलभूत सिद्धांत 2 — सभी Payments Wiom सिस्टम के ज़रिए ही Process करें से संबंधित उल्लंघन दर्ज हुआ है।

उल्लंघन का विवरण:
आपके या आपके कर्मचारी द्वारा ग्राहक {v['MASKED_NUMBER']} से ₹{v['AMOUNT_COLLECTED']} की राशि ली गई थी।

Wiom सिस्टम नियमों के अनुसार सभी भुगतान केवल Wiom सिस्टम के माध्यम से ही लिए जा सकते हैं। ग्राहक से सीधे भुगतान लेना अनुमत नहीं है।

इस कारण सिस्टम द्वारा:
• ग्राहक को ₹{v['AMOUNT_REFUNDED']} की राशि वापस कर दी गई है
• आपके वॉलेट से ₹{v['AMOUNT_RECOVERED']} की राशि वसूल की गई है

यह गतिविधि Wiom System: 9 मूलभूत सिद्धांतों के अंतर्गत अनुमत नहीं है।

कृपया सुनिश्चित करें कि भविष्य में:
• ग्राहक से कोई भी अतिरिक्त भुगतान सीधे न लिया जाए
• सभी भुगतान केवल Wiom सिस्टम के माध्यम से ही प्रोसेस किए जाएँ

यदि आपको लगता है कि यह जानकारी गलत है, तो 48 घंटे के भीतर 7836811111 पर कॉल करें। उपलब्ध रिकॉर्ड के आधार पर पुनः समीक्षा की जाएगी।

पूर्ण और आधिकारिक जानकारी के लिए Partner App में "Wiom Partner System Fundamentals" सेक्शन देखें।
https://partnerapp.wiom.in/wiom-agreement

यह सूचना स्वचालित रूप से सिस्टम द्वारा जारी की गई है।

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Wiom System Message

System records indicate a violation related to Wiom Fundamental Principle 2 — All Payments Must Be Processed Through the Wiom System.

Violation Details:
You or your employee collected ₹{v['AMOUNT_COLLECTED']} from customer {v['MASKED_NUMBER']}.

As per Wiom system rules, all payments must be processed only through the Wiom system. Direct collection of money from customers is not permitted.

As a result:
• ₹{v['AMOUNT_REFUNDED']} has been refunded to the customer by the system
• ₹{v['AMOUNT_RECOVERED']} has been recovered from your wallet

This activity is not permitted under Wiom System: 9 Fundamental Principles.

Please ensure that in the future:
• No additional payments are collected directly from customers
• All payments are processed only through the Wiom system

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


import os

_gmail_access_token_cache = {"token": None, "expires_at": 0}


def _get_gmail_oauth2_config():
    """Get Gmail OAuth2 client credentials (lazy, reads env at call time)."""
    return {
        "client_id": os.environ.get("GMAIL_CLIENT_ID", ""),
        "client_secret": os.environ.get("GMAIL_CLIENT_SECRET", ""),
    }


def _get_gmail_refresh_token():
    """Get Gmail OAuth2 refresh token from env var or config."""
    token = os.environ.get("GMAIL_REFRESH_TOKEN")
    if token:
        return token
    cfg = config.load()
    return cfg.get("gmail_refresh_token", "")


def _send_via_gmail_oauth2(msg: MIMEMultipart, refresh_token: str) -> dict:
    """Send email via Gmail API using OAuth2 refresh token (works on Railway)."""
    import time
    cache = _gmail_access_token_cache
    if cache["token"] and time.time() < cache["expires_at"] - 60:
        access_token = cache["token"]
    else:
        oauth_cfg = _get_gmail_oauth2_config()
        resp = requests.post("https://oauth2.googleapis.com/token", data={
            "client_id": oauth_cfg["client_id"],
            "client_secret": oauth_cfg["client_secret"],
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        })
        token_data = resp.json()
        if "access_token" not in token_data:
            return {"ok": False, "error": f"OAuth2 token refresh failed: {token_data.get('error_description', token_data)}"}
        access_token = token_data["access_token"]
        cache["token"] = access_token
        cache["expires_at"] = time.time() + token_data.get("expires_in", 3600)

    # Send via Gmail API
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    r = requests.post(
        "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        json={"raw": raw},
    )
    if r.status_code == 200:
        return {"ok": True}
    return {"ok": False, "error": f"Gmail API send failed ({r.status_code}): {r.text}"}


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
    """Send email via Gmail API OAuth2 (primary) with SMTP fallback.
    Returns {"ok": bool, "error": str|None}."""
    cfg = config.load()
    smtp_host = smtp_host or cfg.get("smtp_host", "smtp.gmail.com")
    smtp_port = smtp_port or int(cfg.get("smtp_port", 587))
    smtp_user = smtp_user or cfg.get("smtp_user", "")
    smtp_pass = smtp_pass or cfg.get("smtp_password", "")
    from_email = from_email or cfg.get("smtp_from_email") or smtp_user or "partner@wiom.in"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"WIOM System Notice : Immediate Review Required <{from_email}>"
    msg["To"] = to_email
    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    msg.attach(MIMEText(body_html, "html", "utf-8"))

    # Try Gmail API OAuth2 first (works on Railway where SMTP is blocked)
    refresh_token = _get_gmail_refresh_token()
    if refresh_token:
        result = _send_via_gmail_oauth2(msg, refresh_token)
        if result["ok"]:
            return result
        # Gmail API failed, try SMTP fallback
        gmail_err = result.get("error", "")
        if smtp_user and smtp_pass:
            try:
                return _send_via_smtp(smtp_host, smtp_port, smtp_user, smtp_pass, msg)
            except Exception as smtp_err:
                return {"ok": False, "error": f"Gmail API failed ({gmail_err}). SMTP fallback also failed: {smtp_err}"}
        return result

    # No refresh token — try SMTP directly
    if not smtp_user or not smtp_pass:
        return {"ok": False, "error": "No Gmail refresh token and no SMTP credentials configured. Go to Settings."}
    try:
        return _send_via_smtp(smtp_host, smtp_port, smtp_user, smtp_pass, msg)
    except Exception as e:
        return {"ok": False, "error": str(e)}
