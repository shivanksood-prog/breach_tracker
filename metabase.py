import requests
from datetime import datetime, timedelta

def _start_date() -> str:
    return "2026-02-24"


def get_breach_sql() -> str:
    return f"""
WITH installs_and_free_plan AS (
    SELECT *
    FROM (
        SELECT
            CAST(OTP_ISSUED_TIME + interval '330 minute' AS DATE) AS install_date,
            OTP_ISSUED_TIME + interval '330 minute' AS install_time,
            mobile,
            ROW_NUMBER() OVER (
                PARTITION BY idmaker(trum.shard, 0, router_nas_id)
                ORDER BY OTP_EXPIRY_TIME
            ) AS rn
        FROM PROD_DB.PUBLIC.T_ROUTER_USER_MAPPING trum
        LEFT JOIN T_PLAN_CONFIGURATION tpc
            ON tpc.id = trum.SELECTED_PLAN_ID
        WHERE otp = 'DONE'
          AND store_group_id = 0
          AND device_limit > 1
          AND mobile > '5999999999'
    )
    WHERE rn = 1
      AND install_date >= '2026-01-26'
      AND mobile NOT IN ('6900099267','7679376747')
),

mobiles_q1 AS (
    SELECT DISTINCT LEFT(mobile, 10) AS mobile10
    FROM installs_and_free_plan
),

misbehavior_tickets AS (
    SELECT
        ticket_id,
        kapture_ticket_id,
        DATEADD(minute, 330, ticket_added_time) AS ticket_added_time_ist,
        customer_mobile,
        customer_account_id,
        current_partner_account_id,
        current_partner_name,
        zone
    FROM PROD_DB.PUBLIC.service_ticket_model
    WHERE first_title ILIKE 'Partner Misbehavior|Took extra cash%'
),

install_assignee AS (
    SELECT
        tva.mobile,
        tva.account_id,
        tva.event_name,
        tva.added_time as event_time,
        tva.assignee as Install_Emp_ID,
        tva.assignee_name as Install_Name,
        CASE
            WHEN r.rohit_onboarding_date IS NOT NULL THEN 'ROHIT'
            WHEN hb.partner_user_id IS NOT NULL THEN 'OWNER'
            ELSE 'ADMIN'
        END as Install_Emp_Role
    FROM PROD_DB.PUBLIC.TASKVANILLA_AUDIT tva
    LEFT JOIN rohit_model r
        ON tva.assignee = r.rohit_userid
    LEFT JOIN (SELECT DISTINCT partner_user_id, partner_account_id FROM HIERARCHY_BASE) hb
        ON tva.assignee = hb.partner_user_id
       AND tva.account_id = hb.partner_account_id
    WHERE tva.event_name IN ('OTP_VERIFIED', 'RATING')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY tva.mobile
        ORDER BY tva.added_time DESC
    ) = 1
),

partner_details AS (
    SELECT DISTINCT
        partner_account_id,
        partner_name,
        partner_mobile
    FROM PROD_DB.PUBLIC.SUPPLY_MODEL
)

SELECT
    t.ticket_id,
    t.kapture_ticket_id,
    t.ticket_added_time_ist,
    t.customer_mobile,
    t.current_partner_account_id,
    t.current_partner_name,
    t.zone,
    pd.partner_mobile,
    CASE WHEN m.mobile10 IS NOT NULL THEN 1 ELSE 0 END AS new_install_flag,
    ia.Install_Emp_Role,
    ia.Install_Emp_ID,
    ia.Install_Name
FROM misbehavior_tickets t
LEFT JOIN mobiles_q1 m
    ON LEFT(CAST(t.customer_mobile AS STRING), 10) = m.mobile10
LEFT JOIN install_assignee ia
    ON LEFT(CAST(t.customer_mobile AS STRING), 10) = LEFT(CAST(ia.mobile AS STRING), 10)
LEFT JOIN partner_details pd
    ON t.current_partner_account_id = pd.partner_account_id
WHERE t.ticket_added_time_ist >= '{_start_date()}'
ORDER BY t.ticket_added_time_ist DESC
"""


class MetabaseClient:
    def __init__(self, url: str, database_id: str,
                 api_key: str = "", username: str = "", password: str = ""):
        self.url = url.rstrip("/")
        self.database_id = int(database_id) if database_id else None
        self.api_key = api_key.strip()
        self.username = username
        self.password = password
        self._session_token = None

    def _headers(self) -> dict:
        """Return auth headers — API key takes priority over session token."""
        h = {"Content-Type": "application/json"}
        if self.api_key:
            h["X-API-KEY"] = self.api_key
        else:
            h["X-Metabase-Session"] = self._get_session_token()
        return h

    def _get_session_token(self) -> str:
        if self._session_token:
            return self._session_token
        resp = requests.post(
            f"{self.url}/api/session",
            json={"username": self.username, "password": self.password},
            timeout=15,
        )
        resp.raise_for_status()
        self._session_token = resp.json()["id"]
        return self._session_token

    def list_databases(self) -> list:
        resp = requests.get(
            f"{self.url}/api/database",
            headers=self._headers(),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        dbs = data.get("data", data) if isinstance(data, dict) else data
        return [{"id": d["id"], "name": d["name"], "engine": d.get("engine", "")} for d in dbs]

    def run_breach_query(self) -> list:
        sql = get_breach_sql()
        payload = {
            "database": self.database_id,
            "type": "native",
            "native": {"query": sql},
            "parameters": [],
        }
        resp = requests.post(
            f"{self.url}/api/dataset",
            headers=self._headers(),
            json=payload,
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            raise Exception(f"Metabase query error: {data['error']}")

        cols = [c["name"].lower() for c in data["data"]["cols"]]
        rows = data["data"]["rows"]

        col_map = {
            "ticket_id":                  "ticket_id",
            "kapture_ticket_id":          "kapture_ticket_id",
            "ticket_added_time_ist":      "ticket_added_time_ist",
            "customer_mobile":            "customer_mobile",
            "current_partner_account_id": "current_partner_account_id",
            "current_partner_name":       "current_partner_name",
            "zone":                       "zone",
            "partner_mobile":             "partner_mobile",
            "new_install_flag":           "new_install_flag",
            "install_emp_role":           "install_emp_role",
            "install_emp_id":             "install_emp_id",
            "install_name":               "install_name",
        }

        result = []
        for row in rows:
            row_dict = dict(zip(cols, row))
            mapped = {}
            for src, dst in col_map.items():
                mapped[dst] = str(row_dict[src]) if row_dict.get(src) is not None else None
            result.append(mapped)
        return result

    def test_connection(self) -> bool:
        try:
            resp = requests.get(
                f"{self.url}/api/user/current",
                headers=self._headers(),
                timeout=10,
            )
            return resp.status_code == 200
        except Exception:
            return False
