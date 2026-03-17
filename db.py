import os
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

DB_PATH = Path(os.environ.get("DB_PATH", Path(__file__).parent / "breach_tracker.db"))

VALID_TRANSITIONS = {
    "detected":          ["customer_refunded"],
    "customer_refunded": ["customer_comms"],
    "customer_comms":    ["partner_penalty"],
    "partner_penalty":   [],
}

STATE_LABELS = {
    "detected":          "Detected",
    "customer_refunded": "Customer Refunded",
    "customer_comms":    "Customer Comms",
    "partner_penalty":   "Partner Penalty",
}


def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS cases (
                id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id                   TEXT UNIQUE NOT NULL,
                kapture_ticket_id           TEXT,
                ticket_added_time_ist       TEXT,
                customer_mobile             TEXT,
                current_partner_account_id  TEXT,
                current_partner_name        TEXT,
                zone                        TEXT,
                partner_mobile              TEXT,
                new_install_flag            TEXT,
                install_emp_role            TEXT,
                install_emp_id              TEXT,
                install_name                TEXT,
                extra_amount                REAL,
                technician_name             TEXT,
                voluntary_tip               TEXT,
                kapture_raw_json            TEXT,
                state                       TEXT NOT NULL DEFAULT 'detected',
                detected_at                 TEXT,
                customer_refunded_at        TEXT,
                partner_penalty_at          TEXT,
                customer_comms_at           TEXT,
                partner_comms_at            TEXT,
                refund_csv_sent             INTEGER DEFAULT 0,
                refund_csv_sent_at          TEXT,
                penalty_csv_sent            INTEGER DEFAULT 0,
                penalty_csv_sent_at         TEXT,
                previous_state              TEXT,
                created_at                  TEXT DEFAULT (datetime('now')),
                updated_at                  TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS customer_comms_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id         INTEGER NOT NULL REFERENCES cases(id),
                ticket_id       TEXT NOT NULL,
                attempt_number  INTEGER DEFAULT 1,
                called_at       TEXT DEFAULT (datetime('now')),
                agent_name      TEXT,
                connected       TEXT,
                comment         TEXT,
                resolved        INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS breach1_cases (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                lng_nas_id          TEXT,
                customer_mobile     TEXT,
                expiry_dt           TEXT,
                city                TEXT,
                mis_city            TEXT,
                zone                TEXT,
                partner_name        TEXT,
                tenure              TEXT,
                r_oct               TEXT,
                r_nov               TEXT,
                r_dec               TEXT,
                r_jan               TEXT,
                risk_score          TEXT,
                partner_status      TEXT,
                connected           TEXT,
                calling_remarks     TEXT,
                disintermediation   TEXT,
                call_recording      TEXT,
                called_by           TEXT,
                call_timestamp      TEXT,
                calling_status      TEXT,
                partner_email       TEXT,
                email_state         TEXT NOT NULL DEFAULT 'pending',
                email_sent_at       TEXT,
                email_case_type     INTEGER,
                created_at          TEXT DEFAULT (datetime('now')),
                updated_at          TEXT DEFAULT (datetime('now')),
                UNIQUE(lng_nas_id, customer_mobile)
            );

            CREATE TABLE IF NOT EXISTS breach4_cases (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                partner_id          TEXT,
                partner_name        TEXT,
                customer_details    TEXT,
                principle_broken    TEXT,
                device_id           TEXT,
                date_reported       TEXT,
                reporting_channel   TEXT,
                penalty_amount      TEXT,
                penalty_done        TEXT,
                penalty_done_date   TEXT,
                partner_email_comms TEXT,
                email_date          TEXT,
                whatsapp_comms      TEXT,
                whatsapp_date       TEXT,
                partner_mobile      TEXT,
                link                TEXT,
                comments            TEXT,
                partner_email       TEXT,
                email_state         TEXT NOT NULL DEFAULT 'pending',
                email_sent_at       TEXT,
                created_at          TEXT DEFAULT (datetime('now')),
                updated_at          TEXT DEFAULT (datetime('now')),
                UNIQUE(partner_id, device_id, customer_details)
            );

            CREATE TABLE IF NOT EXISTS breach4_email_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id         INTEGER REFERENCES breach4_cases(id),
                partner_name    TEXT,
                partner_email   TEXT,
                recipient_email TEXT,
                language        TEXT DEFAULT 'both',
                variables_json  TEXT,
                subject         TEXT,
                body_preview    TEXT,
                status          TEXT DEFAULT 'sent',
                is_test         INTEGER DEFAULT 0,
                sent_at         TEXT DEFAULT (datetime('now')),
                error           TEXT
            );

            CREATE TABLE IF NOT EXISTS breach1_email_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id         INTEGER REFERENCES breach1_cases(id),
                partner_name    TEXT,
                partner_email   TEXT,
                recipient_email TEXT,
                case_type       INTEGER,
                language        TEXT DEFAULT 'both',
                variables_json  TEXT,
                subject         TEXT,
                body_preview    TEXT,
                status          TEXT DEFAULT 'sent',
                is_test         INTEGER DEFAULT 0,
                sent_at         TEXT DEFAULT (datetime('now')),
                error           TEXT
            );
        """)


def _migrate():
    """Add columns introduced after initial schema."""
    with get_conn() as conn:
        try:
            conn.execute("ALTER TABLE cases ADD COLUMN previous_state TEXT")
        except Exception:
            pass  # already exists
        try:
            conn.execute("ALTER TABLE cases ADD COLUMN refund_payout_link TEXT")
        except Exception:
            pass  # already exists
        # B1 overhaul columns
        try:
            conn.execute("ALTER TABLE breach1_cases ADD COLUMN source TEXT DEFAULT 'churn_logic'")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE breach1_cases ADD COLUMN partner_id TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE breach1_cases ADD COLUMN report_text TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE breach1_cases ADD COLUMN reported_by TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE breach1_cases ADD COLUMN action_type TEXT")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE breach1_cases ADD COLUMN penalty_state TEXT DEFAULT 'none'")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE breach1_cases ADD COLUMN penalty_amount REAL DEFAULT -2000")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE breach1_cases ADD COLUMN penalty_email_state TEXT DEFAULT 'pending'")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE breach1_cases ADD COLUMN penalty_email_sent_at TEXT")
        except Exception:
            pass


def now_ist() -> str:
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M:%S")


def upsert_case(data: dict):
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT * FROM cases WHERE ticket_id = ?", (data["ticket_id"],)
        ).fetchone()

        if existing is None:
            row = dict(data)
            row.setdefault("detected_at", now_ist())
            row.setdefault("state", "detected")
            cols = ", ".join(row.keys())
            placeholders = ", ".join(["?"] * len(row))
            conn.execute(f"INSERT INTO cases ({cols}) VALUES ({placeholders})", list(row.values()))
        else:
            # Only update fields that are safe to overwrite
            safe_keys = {
                "ticket_added_time_ist", "customer_mobile", "current_partner_account_id",
                "current_partner_name", "zone", "partner_mobile", "new_install_flag",
                "install_emp_role", "install_emp_id", "install_name", "kapture_ticket_id",
            }
            updates = {k: v for k, v in data.items() if k in safe_keys}
            if updates:
                updates["updated_at"] = now_ist()
                set_clause = ", ".join(f"{k} = ?" for k in updates)
                conn.execute(
                    f"UPDATE cases SET {set_clause} WHERE ticket_id = ?",
                    list(updates.values()) + [data["ticket_id"]],
                )


def update_kapture_fields(ticket_id: str, extra_amount, technician_name, voluntary_tip, raw_json: str):
    with get_conn() as conn:
        conn.execute(
            """UPDATE cases SET extra_amount=?, technician_name=?, voluntary_tip=?,
               kapture_raw_json=?, updated_at=? WHERE ticket_id=?""",
            (extra_amount, technician_name, voluntary_tip, raw_json, now_ist(), ticket_id),
        )


def advance_state(ticket_id: str, new_state: str) -> bool:
    with get_conn() as conn:
        row = conn.execute("SELECT state FROM cases WHERE ticket_id=?", (ticket_id,)).fetchone()
        if not row:
            return False
        current = row["state"]
        if new_state not in VALID_TRANSITIONS.get(current, []):
            return False
        ts_col = {
            "customer_refunded": "customer_refunded_at",
            "customer_comms":    "customer_comms_at",
            "partner_penalty":   "partner_penalty_at",
        }.get(new_state)
        ts = now_ist()
        if ts_col:
            conn.execute(
                f"UPDATE cases SET state=?, previous_state=?, {ts_col}=?, updated_at=? WHERE ticket_id=?",
                (new_state, current, ts, ts, ticket_id),
            )
        else:
            conn.execute(
                "UPDATE cases SET state=?, previous_state=?, updated_at=? WHERE ticket_id=?",
                (new_state, current, ts, ticket_id),
            )
        return True


def undo_state(ticket_id: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT state, previous_state FROM cases WHERE ticket_id=?", (ticket_id,)
        ).fetchone()
        if not row or not row["previous_state"]:
            return False
        current  = row["state"]
        previous = row["previous_state"]
        # Clear the timestamp of the state we're reverting from
        ts_col = {
            "customer_refunded": "customer_refunded_at",
            "customer_comms":    "customer_comms_at",
            "partner_penalty":   "partner_penalty_at",
        }.get(current)
        ts = now_ist()
        if ts_col:
            conn.execute(
                f"UPDATE cases SET state=?, previous_state=NULL, {ts_col}=NULL, updated_at=? WHERE ticket_id=?",
                (previous, ts, ticket_id),
            )
        else:
            conn.execute(
                "UPDATE cases SET state=?, previous_state=NULL, updated_at=? WHERE ticket_id=?",
                (previous, ts, ticket_id),
            )
        return True


def mark_refund_sent(ticket_ids: list):
    ts = now_ist()
    with get_conn() as conn:
        for tid in ticket_ids:
            conn.execute(
                "UPDATE cases SET refund_csv_sent=1, refund_csv_sent_at=? WHERE ticket_id=?",
                (ts, tid),
            )


def mark_penalty_sent(ticket_ids: list):
    ts = now_ist()
    with get_conn() as conn:
        for tid in ticket_ids:
            conn.execute(
                "UPDATE cases SET penalty_csv_sent=1, penalty_csv_sent_at=? WHERE ticket_id=?",
                (ts, tid),
            )


def mark_refunded_by_mobile(mobile: str, payout_link_id: str) -> dict:
    """Find case by customer_mobile in 'detected' state, advance to 'customer_refunded',
    store payout link URL. Returns match result dict."""
    payout_url = f"https://payout-links.razorpay.com/v1/payout-links/{payout_link_id}/view/#/"
    with get_conn() as conn:
        row = conn.execute(
            "SELECT ticket_id FROM cases WHERE customer_mobile = ? AND state = 'detected'",
            (mobile,),
        ).fetchone()
        if not row:
            return {"matched": False, "mobile": mobile}
        tid = row["ticket_id"]
    # Use existing advance_state to handle the transition
    ok = advance_state(tid, "customer_refunded")
    if ok:
        with get_conn() as conn:
            conn.execute(
                "UPDATE cases SET refund_payout_link = ?, updated_at = ? WHERE ticket_id = ?",
                (payout_url, now_ist(), tid),
            )
    return {"matched": True, "mobile": mobile, "ticket_id": tid, "payout_link": payout_url}


def mark_penalty_by_upload(partner_id: str) -> dict:
    """Find cases by partner in 'customer_comms' state, advance to 'partner_penalty'.
    Returns match result dict."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT ticket_id FROM cases WHERE current_partner_account_id = ? AND state = 'customer_comms'",
            (partner_id,),
        ).fetchall()
        if not rows:
            return {"matched": False, "partner_id": partner_id}
    advanced = []
    for row in rows:
        tid = row["ticket_id"]
        ok = advance_state(tid, "partner_penalty")
        if ok:
            advanced.append(tid)
    return {"matched": len(advanced) > 0, "partner_id": partner_id, "ticket_ids": advanced, "count": len(advanced)}


def get_all_cases(state=None, zone=None, search=None) -> list:
    with get_conn() as conn:
        query = "SELECT * FROM cases WHERE 1=1"
        params = []
        if state and state != "all":
            query += " AND state = ?"
            params.append(state)
        if zone and zone != "all":
            query += " AND zone LIKE ?"
            params.append(f"%{zone}%")
        if search:
            query += " AND (ticket_id LIKE ? OR customer_mobile LIKE ? OR current_partner_name LIKE ? OR zone LIKE ?)"
            s = f"%{search}%"
            params.extend([s, s, s, s])
        query += " ORDER BY detected_at DESC"
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def get_case(ticket_id: str):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM cases WHERE ticket_id=?", (ticket_id,)).fetchone()
        if not row:
            return None
        case = dict(row)
        logs = conn.execute(
            "SELECT * FROM customer_comms_log WHERE ticket_id=? ORDER BY called_at",
            (ticket_id,),
        ).fetchall()
        case["comms_log"] = [dict(l) for l in logs]
        return case


def get_summary() -> dict:
    with get_conn() as conn:
        states = ["detected", "customer_refunded", "customer_comms", "partner_penalty"]
        by_state = {}
        for s in states:
            count = conn.execute("SELECT COUNT(*) FROM cases WHERE state=?", (s,)).fetchone()[0]
            by_state[s] = count
        total = conn.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
        total_amount = conn.execute(
            "SELECT COALESCE(SUM(extra_amount),0) FROM cases"
        ).fetchone()[0]
        return {
            "total": total,
            "by_state": by_state,
            "total_amount": round(total_amount, 2),
        }


def get_pending_comms() -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM cases WHERE state='customer_refunded' ORDER BY detected_at DESC"
        ).fetchall()
        result = []
        for r in rows:
            case = dict(r)
            logs = conn.execute(
                "SELECT * FROM customer_comms_log WHERE ticket_id=? ORDER BY called_at DESC",
                (r["ticket_id"],),
            ).fetchall()
            case["comms_log"] = [dict(l) for l in logs]
            result.append(case)
        return result


def log_comms_attempt(ticket_id: str, agent_name: str, connected: str, comment: str):
    with get_conn() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM customer_comms_log WHERE ticket_id=?", (ticket_id,)
        ).fetchone()[0]
        conn.execute(
            """INSERT INTO customer_comms_log
               (case_id, ticket_id, attempt_number, agent_name, connected, comment, resolved, called_at)
               SELECT id, ?, ?, ?, ?, ?, ?, datetime('now') FROM cases WHERE ticket_id=?""",
            (ticket_id, count + 1, agent_name, connected, comment,
             1 if connected == "Yes" else 0, ticket_id),
        )
    if connected == "Yes":
        case_now = get_case(ticket_id)
        if case_now:
            next_states = VALID_TRANSITIONS.get(case_now["state"], [])
            if next_states:
                advance_state(ticket_id, next_states[0])
    return get_case(ticket_id)


def get_all_zones() -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT zone FROM cases WHERE zone IS NOT NULL AND zone != '' ORDER BY zone"
        ).fetchall()
        return [r[0] for r in rows]


# ── Breach 1 (Disintermediation) ──────────────────────────────────────────────

def upsert_breach1_case(data: dict):
    with get_conn() as conn:
        lng = data.get("lng_nas_id", "")
        mob = data.get("customer_mobile", "")
        existing = conn.execute(
            "SELECT id FROM breach1_cases WHERE lng_nas_id=? AND customer_mobile=?",
            (lng, mob),
        ).fetchone()
        if existing is None:
            cols = ", ".join(data.keys())
            placeholders = ", ".join(["?"] * len(data))
            conn.execute(f"INSERT INTO breach1_cases ({cols}) VALUES ({placeholders})", list(data.values()))
        else:
            safe_keys = {
                "expiry_dt", "city", "mis_city", "zone", "partner_name", "tenure",
                "r_oct", "r_nov", "r_dec", "r_jan", "risk_score", "partner_status",
                "connected", "calling_remarks", "disintermediation", "call_recording",
                "called_by", "call_timestamp", "calling_status", "partner_email",
                "source", "partner_id", "report_text", "reported_by",
            }
            updates = {k: v for k, v in data.items() if k in safe_keys}
            if updates:
                updates["updated_at"] = now_ist()
                set_clause = ", ".join(f"{k} = ?" for k in updates)
                conn.execute(
                    f"UPDATE breach1_cases SET {set_clause} WHERE id = ?",
                    list(updates.values()) + [existing["id"]],
                )


def get_breach1_cases(partner=None, zone=None, status=None, email_state=None, search=None, source=None, action_type=None) -> list:
    with get_conn() as conn:
        query = "SELECT * FROM breach1_cases WHERE 1=1"
        params = []
        if partner and partner != "all":
            query += " AND partner_name = ?"
            params.append(partner)
        if zone and zone != "all":
            query += " AND zone LIKE ?"
            params.append(f"%{zone}%")
        if status and status != "all":
            query += " AND partner_status = ?"
            params.append(status)
        if email_state and email_state != "all":
            query += " AND email_state = ?"
            params.append(email_state)
        if source and source != "all":
            query += " AND source = ?"
            params.append(source)
        if action_type and action_type != "all":
            query += " AND action_type = ?"
            params.append(action_type)
        if search:
            query += " AND (customer_mobile LIKE ? OR partner_name LIKE ? OR zone LIKE ? OR lng_nas_id LIKE ?)"
            s = f"%{search}%"
            params.extend([s, s, s, s])
        query += " ORDER BY partner_name, expiry_dt DESC"
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def get_breach1_case(case_id: int):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM breach1_cases WHERE id=?", (case_id,)).fetchone()
        return dict(row) if row else None


def get_breach1_summary() -> dict:
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM breach1_cases").fetchone()[0]
        by_status = {}
        for row in conn.execute(
            "SELECT partner_status, COUNT(*) as cnt FROM breach1_cases GROUP BY partner_status"
        ).fetchall():
            by_status[row["partner_status"]] = row["cnt"]
        by_email = {}
        for row in conn.execute(
            "SELECT email_state, COUNT(*) as cnt FROM breach1_cases GROUP BY email_state"
        ).fetchall():
            by_email[row["email_state"]] = row["cnt"]
        partners = conn.execute("SELECT COUNT(DISTINCT partner_name) FROM breach1_cases").fetchone()[0]
        by_source = {}
        for row in conn.execute(
            "SELECT source, COUNT(*) as cnt FROM breach1_cases GROUP BY source"
        ).fetchall():
            by_source[row["source"]] = row["cnt"]
        warning_sent = conn.execute(
            "SELECT COUNT(*) FROM breach1_cases WHERE action_type='warning' AND email_state='sent'"
        ).fetchone()[0]
        penalty_done = conn.execute(
            "SELECT COUNT(*) FROM breach1_cases WHERE action_type='penalty' AND penalty_state='email_sent'"
        ).fetchone()[0]
        return {
            "total": total, "by_status": by_status, "by_email_state": by_email,
            "partners": partners, "by_source": by_source,
            "warning_sent": warning_sent, "penalty_done": penalty_done,
        }


def get_breach1_partners() -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT partner_name FROM breach1_cases WHERE partner_name IS NOT NULL ORDER BY partner_name"
        ).fetchall()
        return [r[0] for r in rows]


def get_breach1_zones() -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT zone FROM breach1_cases WHERE zone IS NOT NULL AND zone != '' ORDER BY zone"
        ).fetchall()
        return [r[0] for r in rows]


def mark_breach1_email_sent(case_ids: list, case_type: int):
    ts = now_ist()
    with get_conn() as conn:
        for cid in case_ids:
            conn.execute(
                "UPDATE breach1_cases SET email_state='sent', email_sent_at=?, email_case_type=?, updated_at=? WHERE id=?",
                (ts, case_type, ts, cid),
            )


def log_breach1_email(case_id, partner_name, partner_email, recipient_email,
                      case_type, language, variables_json, subject, body_preview,
                      is_test=False, status="sent", error=None):
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO breach1_email_log
               (case_id, partner_name, partner_email, recipient_email, case_type,
                language, variables_json, subject, body_preview, status, is_test, error, sent_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (case_id, partner_name, partner_email, recipient_email, case_type,
             language, variables_json, subject, body_preview, status, 1 if is_test else 0,
             error, now_ist()),
        )


def get_breach1_email_log(limit=100) -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM breach1_email_log ORDER BY sent_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def set_breach1_action_type(case_ids: list, action_type: str):
    """Set action_type ('warning' or 'penalty') on selected cases."""
    ts = now_ist()
    with get_conn() as conn:
        for cid in case_ids:
            conn.execute(
                "UPDATE breach1_cases SET action_type=?, updated_at=? WHERE id=?",
                (action_type, ts, cid),
            )


def get_breach1_penalty_cases(partner=None) -> list:
    """Get cases where action_type='penalty' and penalty_state='none'."""
    with get_conn() as conn:
        query = "SELECT * FROM breach1_cases WHERE action_type='penalty' AND penalty_state='none'"
        params = []
        if partner and partner != "all":
            query += " AND partner_name = ?"
            params.append(partner)
        query += " ORDER BY partner_name"
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def mark_b1_penalty_csv_generated(case_ids: list):
    ts = now_ist()
    with get_conn() as conn:
        for cid in case_ids:
            conn.execute(
                "UPDATE breach1_cases SET penalty_state='csv_generated', updated_at=? WHERE id=?",
                (ts, cid),
            )


def mark_b1_penalty_uploaded(partner_id: str) -> dict:
    """Advance penalty_state from csv_generated to uploaded for cases matching partner_id."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id FROM breach1_cases WHERE partner_id=? AND action_type='penalty' AND penalty_state='csv_generated'",
            (partner_id,),
        ).fetchall()
        if not rows:
            return {"matched": False, "partner_id": partner_id}
        ts = now_ist()
        for row in rows:
            conn.execute(
                "UPDATE breach1_cases SET penalty_state='uploaded', updated_at=? WHERE id=?",
                (ts, row["id"]),
            )
        return {"matched": True, "partner_id": partner_id, "count": len(rows)}


def mark_b1_penalty_email_sent(case_ids: list):
    ts = now_ist()
    with get_conn() as conn:
        for cid in case_ids:
            conn.execute(
                "UPDATE breach1_cases SET penalty_state='email_sent', penalty_email_state='sent', penalty_email_sent_at=?, updated_at=? WHERE id=?",
                (ts, ts, cid),
            )


def get_breach1_sources() -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT source FROM breach1_cases WHERE source IS NOT NULL ORDER BY source"
        ).fetchall()
        return [r[0] for r in rows]


# ── Breach 4 (Router Misuse) ────────────────────────────────────────────────

def upsert_breach4_case(data: dict):
    with get_conn() as conn:
        pid = data.get("partner_id", "")
        dev = data.get("device_id", "")
        cust = data.get("customer_details", "")
        existing = conn.execute(
            "SELECT id FROM breach4_cases WHERE partner_id=? AND device_id=? AND customer_details=?",
            (pid, dev, cust),
        ).fetchone()
        if existing is None:
            cols = ", ".join(data.keys())
            placeholders = ", ".join(["?"] * len(data))
            conn.execute(f"INSERT INTO breach4_cases ({cols}) VALUES ({placeholders})", list(data.values()))
        else:
            safe_keys = {
                "partner_name", "principle_broken", "date_reported", "reporting_channel",
                "penalty_amount", "penalty_done", "penalty_done_date",
                "partner_email_comms", "email_date", "whatsapp_comms", "whatsapp_date",
                "partner_mobile", "link", "comments", "partner_email",
            }
            updates = {k: v for k, v in data.items() if k in safe_keys}
            if updates:
                updates["updated_at"] = now_ist()
                set_clause = ", ".join(f"{k} = ?" for k in updates)
                conn.execute(
                    f"UPDATE breach4_cases SET {set_clause} WHERE id = ?",
                    list(updates.values()) + [existing["id"]],
                )


def get_breach4_cases(partner=None, email_state=None, search=None) -> list:
    with get_conn() as conn:
        query = "SELECT * FROM breach4_cases WHERE 1=1"
        params = []
        if partner and partner != "all":
            query += " AND partner_name = ?"
            params.append(partner)
        if email_state and email_state != "all":
            query += " AND email_state = ?"
            params.append(email_state)
        if search:
            query += " AND (customer_details LIKE ? OR partner_name LIKE ? OR device_id LIKE ? OR partner_id LIKE ?)"
            s = f"%{search}%"
            params.extend([s, s, s, s])
        query += " ORDER BY partner_name, date_reported DESC"
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def get_breach4_case(case_id: int):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM breach4_cases WHERE id=?", (case_id,)).fetchone()
        return dict(row) if row else None


def get_breach4_summary() -> dict:
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM breach4_cases").fetchone()[0]
        by_email = {}
        for row in conn.execute(
            "SELECT email_state, COUNT(*) as cnt FROM breach4_cases GROUP BY email_state"
        ).fetchall():
            by_email[row["email_state"]] = row["cnt"]
        partners = conn.execute("SELECT COUNT(DISTINCT partner_name) FROM breach4_cases").fetchone()[0]
        penalty_done = conn.execute("SELECT COUNT(*) FROM breach4_cases WHERE penalty_done='Yes'").fetchone()[0]
        return {"total": total, "by_email_state": by_email, "partners": partners, "penalty_done": penalty_done}


def get_breach4_partners() -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT partner_name FROM breach4_cases WHERE partner_name IS NOT NULL AND partner_name != '' ORDER BY partner_name"
        ).fetchall()
        return [r[0] for r in rows]


def mark_breach4_email_sent(case_ids: list):
    ts = now_ist()
    with get_conn() as conn:
        for cid in case_ids:
            conn.execute(
                "UPDATE breach4_cases SET email_state='sent', email_sent_at=?, updated_at=? WHERE id=?",
                (ts, ts, cid),
            )


def log_breach4_email(case_id, partner_name, partner_email, recipient_email,
                      language, variables_json, subject, body_preview,
                      is_test=False, status="sent", error=None):
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO breach4_email_log
               (case_id, partner_name, partner_email, recipient_email,
                language, variables_json, subject, body_preview, status, is_test, error, sent_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (case_id, partner_name, partner_email, recipient_email,
             language, variables_json, subject, body_preview, status, 1 if is_test else 0,
             error, now_ist()),
        )


def get_breach4_email_log(limit=100) -> list:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM breach4_email_log ORDER BY sent_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
