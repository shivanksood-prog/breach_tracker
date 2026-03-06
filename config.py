import json
import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).parent))
SETTINGS_FILE = DATA_DIR / "settings.json"

DEFAULTS = {
    "metabase_url": os.environ.get("METABASE_URL", "https://metabase.wiom.in"),
    "metabase_api_key": os.environ.get("METABASE_API_KEY", "mb_XPG56dRucXiZ6CG958t9UBV1Y4r1/DY4l7WcsFUD5w4="),
    "metabase_username": os.environ.get("METABASE_USERNAME", ""),
    "metabase_password": os.environ.get("METABASE_PASSWORD", ""),
    "metabase_database_id": os.environ.get("METABASE_DATABASE_ID", ""),
    "slack_webhook_url": os.environ.get("SLACK_WEBHOOK_URL", ""),
    "kapture_auth_header": os.environ.get("KAPTURE_AUTH_HEADER", "Basic cGgwYmg3eDJhZWljenZ3aHIxdmdwZ20wcmprcDVycms2ZzZvZTJqZG1pM3ZrdDh3N20="),
    "kapture_cookie": os.environ.get("KAPTURE_COOKIE", "JSESSIONID=9251DC2F4B5DB2CB30C7ADC19DEDA863; _KAPTURECRM_SESSION=; JSESSIONID=8213F0B12C9738CC4FBA9A7110FE727A; JSESSIONRID=3SDmlhjtZ1s1DmlhjtZ; _KAPTURECRM_SESSION=; _KSID=708ca9a6c5164d77b6dcb32756ad6405.3SDmlhjtZ1s1DmlhjtZ"),
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
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(SETTINGS_FILE, "w") as f:
        json.dump(current, f, indent=2)


def get(key: str, default=None):
    return load().get(key, default)
