import json
from pathlib import Path

SETTINGS_FILE = Path(__file__).parent / "settings.json"

DEFAULTS = {
    "metabase_url": "https://metabase.wiom.in",
    "metabase_api_key": "mb_XPG56dRucXiZ6CG958t9UBV1Y4r1/DY4l7WcsFUD5w4=",
    "metabase_username": "",
    "metabase_password": "",
    "metabase_database_id": "",
    "slack_webhook_url": "",
    "kapture_auth_header": "Basic cGgwYmg3eDJhZWljenZ3aHIxdmdwZ20wcmprcDVycms2ZzZvZTJqZG1pM3ZrdDh3N20=",
    "kapture_cookie": "JSESSIONID=9251DC2F4B5DB2CB30C7ADC19DEDA863; _KAPTURECRM_SESSION=; JSESSIONID=8213F0B12C9738CC4FBA9A7110FE727A; JSESSIONRID=3SDmlhjtZ1s1DmlhjtZ; _KAPTURECRM_SESSION=; _KSID=708ca9a6c5164d77b6dcb32756ad6405.3SDmlhjtZ1s1DmlhjtZ",
}


def load() -> dict:
    if SETTINGS_FILE.exists():
        with open(SETTINGS_FILE) as f:
            data = json.load(f)
        return {**DEFAULTS, **data}
    return dict(DEFAULTS)


def save(cfg: dict):
    current = load()
    current.update(cfg)
    with open(SETTINGS_FILE, "w") as f:
        json.dump(current, f, indent=2)


def get(key: str, default=None):
    return load().get(key, default)
