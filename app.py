import json
import os
import queue
import threading
import functools
from flask import Flask, jsonify, request, render_template, Response, stream_with_context
from flask_cors import CORS

import db
import sheets_db
import config
import actions

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}},
     allow_headers=["Content-Type", "Authorization"],
     supports_credentials=True)
db.init_db()
db._migrate()

# ── Password protection ──────────────────────────────────────────────────────
APP_USER = os.environ.get("APP_USER", "wiom")
APP_PASS = os.environ.get("APP_PASS", "wiom2026")


def check_auth(username, password):
    return username == APP_USER and password == APP_PASS


def auth_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return Response(
                "Login required.", 401,
                {"WWW-Authenticate": 'Basic realm="WIOM Breach Tracker"'},
            )
        return f(*args, **kwargs)
    return decorated


@app.before_request
def require_login():
    if request.method == 'OPTIONS':
        return  # Let Flask-CORS handle preflight
    auth = request.authorization
    if not auth or not check_auth(auth.username, auth.password):
        resp = Response("Login required.", 401,
                        {"WWW-Authenticate": 'Basic realm="WIOM Breach Tracker"'})
        # Add CORS headers so cross-origin callers can read the error
        origin = request.headers.get('Origin', '*')
        resp.headers['Access-Control-Allow-Origin'] = origin
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp

# ── SSE broadcast hub ─────────────────────────────────────────────────────────
_sse_clients: list[queue.Queue] = []
_sse_lock = threading.Lock()


def _sse_broadcast(data: dict):
    msg = "data: " + json.dumps(data) + "\n\n"
    with _sse_lock:
        dead = []
        for q in _sse_clients:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _sse_clients.remove(q)


@app.route("/api/stream")
def sse_stream():
    def generate():
        q = queue.Queue(maxsize=100)
        with _sse_lock:
            _sse_clients.append(q)
        try:
            yield "data: " + json.dumps({"type": "connected"}) + "\n\n"
            while True:
                try:
                    msg = q.get(timeout=25)
                    yield msg
                except queue.Empty:
                    yield "data: " + json.dumps({"type": "ping"}) + "\n\n"
        except GeneratorExit:
            pass
        finally:
            with _sse_lock:
                try:
                    _sse_clients.remove(q)
                except ValueError:
                    pass

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


# ── Background scheduler ──────────────────────────────────────────────────────
def _run_sync(cfg: dict) -> dict:
    """Core sync logic shared by scheduler and manual /api/sync.
    Returns {new_cases, updated, total_fetched, errors, slack_sent}."""
    from metabase import MetabaseClient
    from kapture import KaptureClient

    client = MetabaseClient(
        cfg["metabase_url"], cfg.get("metabase_database_id", ""),
        api_key=cfg.get("metabase_api_key", ""),
        username=cfg.get("metabase_username", ""),
        password=cfg.get("metabase_password", ""),
    )
    kap  = KaptureClient(cfg.get("kapture_auth_header", ""))
    rows = client.run_breach_query()

    new_cases_list = []
    updated_count  = 0
    errors         = []

    for row in rows:
        tid = str(row.get("ticket_id", ""))
        if not tid:
            continue
        existing = sheets_db.get_case(tid)
        sheets_db.upsert_case(row)

        if existing is None:
            # Enrich new case with Kapture right away
            if row.get("kapture_ticket_id"):
                try:
                    raw = kap.fetch_ticket(str(row["kapture_ticket_id"]))
                    if raw:
                        fields = kap.extract_breach_fields(raw)
                        sheets_db.update_kapture_fields(
                            tid, fields["extra_amount"], fields["technician_name"],
                            fields["voluntary_tip"], json.dumps(raw),
                        )
                except Exception as e:
                    errors.append({"ticket_id": tid, "error": str(e)})
            new_cases_list.append(sheets_db.get_case(tid))
        else:
            # Re-fetch Kapture for cases still missing amount
            case_now = sheets_db.get_case(tid)
            if case_now and not case_now.get("extra_amount") and row.get("kapture_ticket_id"):
                try:
                    raw = kap.fetch_ticket(str(row["kapture_ticket_id"]))
                    if raw:
                        fields = kap.extract_breach_fields(raw)
                        sheets_db.update_kapture_fields(
                            tid, fields["extra_amount"], fields["technician_name"],
                            fields["voluntary_tip"], json.dumps(raw),
                        )
                except Exception as e:
                    errors.append({"ticket_id": tid, "error": str(e)})
            updated_count += 1

    if new_cases_list:
        # Push SSE event to all open browsers
        _sse_broadcast({
            "type":       "new_cases",
            "count":      len(new_cases_list),
            "ticket_ids": [c["ticket_id"] for c in new_cases_list if c],
        })

    return {
        "new_cases":     len(new_cases_list),
        "updated":       updated_count,
        "total_fetched": len(rows),
        "errors":        errors,
    }


try:
    from apscheduler.schedulers.background import BackgroundScheduler

    def _sync_b1b4():
        """Sync B1/B4 from Google Sheets into SQLite."""
        try:
            from google_sheets import fetch_disintermediation_cases, get_all_partner_emails, fetch_fp4_cases
        except Exception as e:
            app.logger.error(f"B1/B4 import error: {e}")
            return
        # Fetch partner emails once for both B1 and B4
        try:
            emails = get_all_partner_emails()
        except Exception as e:
            app.logger.error(f"Partner emails fetch error: {e}")
            emails = {}
        # B1
        try:
            cases = fetch_disintermediation_cases()
            cases = [c for c in cases if (c.get("Disintermediation") or "").strip().lower() == "yes"]
            for c in cases:
                partner = (c.get("PARTNER_NAME") or "").strip()
                email_info = emails.get(partner.lower(), {})
                data = {
                    "lng_nas_id": c.get("LNG_NAS_ID", ""), "customer_mobile": c.get("MOBILE", ""),
                    "expiry_dt": c.get("EXPIRY_DT", ""), "city": c.get("CITY", ""),
                    "mis_city": c.get("MIS_CITY", ""), "zone": c.get("ZONE", ""),
                    "partner_name": partner, "tenure": c.get("TENURE", ""),
                    "r_oct": c.get("R total (Oct)", ""), "r_nov": c.get("R total (Nov)", ""),
                    "r_dec": c.get("R total (Dec)", ""), "r_jan": c.get("R total (Jan)", ""),
                    "risk_score": c.get("Risk score on wallet activity (Dec+Jan) (Scale 0-3)", ""),
                    "partner_status": c.get("Status", ""), "connected": c.get("Connected", ""),
                    "calling_remarks": c.get("Calling Remarks", ""),
                    "disintermediation": c.get("Disintermediation", ""),
                    "call_recording": c.get("Call Recording", ""), "called_by": c.get("Called By", ""),
                    "call_timestamp": c.get("Call Timestamp (Date)", ""),
                    "calling_status": c.get("Calling Status", ""),
                    "partner_email": email_info.get("email", ""),
                }
                db.upsert_breach1_case(data)
            app.logger.info(f"B1 sync: {len(cases)} cases")
        except Exception as e:
            app.logger.error(f"B1 sync error: {e}")
        # B4
        try:
            cases = fetch_fp4_cases()
            for c in cases:
                partner_name = (c.get("Partner Name") or "").strip()
                partner_id = (c.get("Partner Id") or "").strip()
                email_info = emails.get(partner_name.lower(), {})
                data = {
                    "partner_id": partner_id, "partner_name": partner_name,
                    "customer_details": (c.get("Customer Details") or "").strip(),
                    "principle_broken": (c.get("Principle Broken") or "").strip(),
                    "device_id": (c.get("Device Id") or "").strip(),
                    "date_reported": (c.get("Date Reported") or "").strip(),
                    "reporting_channel": (c.get("Reporting Channel") or "").strip(),
                    "penalty_amount": (c.get("Penalty Amount") or "").strip(),
                    "penalty_done": (c.get("Penalty Done") or "").strip(),
                    "penalty_done_date": (c.get("Penalty Done Date") or "").strip(),
                    "partner_email_comms": (c.get("Partner Email Comms Done") or "").strip(),
                    "email_date": (c.get("Email Date") or "").strip(),
                    "whatsapp_comms": (c.get("Partner Text/Whatsapp Comms Done") or "").strip(),
                    "whatsapp_date": (c.get("Whatsapp Date") or "").strip(),
                    "partner_mobile": (c.get("Partner Mobile") or "").strip(),
                    "link": (c.get("Link") or "").strip(),
                    "comments": (c.get("Comments") or "").strip(),
                    "partner_email": email_info.get("email", ""),
                }
                if not data["partner_id"] and not data["partner_name"] and not data["device_id"]:
                    continue
                db.upsert_breach4_case(data)
            app.logger.info(f"B4 sync: {len(cases)} cases")
        except Exception as e:
            app.logger.error(f"B4 sync error: {e}")

    def _auto_sync():
        cfg = config.load()
        if cfg.get("metabase_url") and (cfg.get("metabase_api_key") or cfg.get("metabase_username")) and cfg.get("metabase_database_id"):
            try:
                _run_sync(cfg)
            except Exception as e:
                app.logger.error(f"Auto-sync B2 error: {e}")
        _sync_b1b4()

    def _slack_pending_digest():
        cfg     = config.load()
        webhook = cfg.get("slack_webhook_url", "")
        if not webhook:
            return
        pending = sheets_db.get_all_cases(state="detected")
        if not pending:
            return
        total = sum(c.get("extra_amount") or 0 for c in pending)
        lines = [f"<!channel>\n:rotating_light: *BREACH 2 — {len(pending)} Pending Case(s)* (30-min digest)\n"]
        for c in pending:
            amt     = f"₹{int(c['extra_amount'])}" if c.get("extra_amount") else "TBD"
            install = "🔧 Install" if str(c.get("new_install_flag")) == "1" else "📋 Other"
            lines.append(
                f"• `{c['ticket_id']}` | {c.get('current_partner_name','—')} | "
                f"{c.get('zone','—')} | *{amt}* | {install}"
            )
        if total:
            lines.append(f"\n*Total pending: ₹{int(total)}*")
        csv_str = actions.generate_refund_csv(pending)
        try:
            actions.send_to_slack(webhook, "\n".join(lines), csv_str)
        except Exception as e:
            app.logger.error(f"Slack digest error: {e}")

    _notified_b2_tids = set()

    def _slack_new_b2_alert():
        try:
            webhook = config.load().get("slack_webhook_url", "")
            if not webhook:
                return
            cases = sheets_db.get_all_cases(state="detected")
            new_cases = [c for c in cases if c.get("ticket_id") not in _notified_b2_tids]
            if not new_cases:
                return
            lines = [f"<!channel>\n:new: *{len(new_cases)} New Breach 2 Case(s) Detected*\n"]
            for c in new_cases:
                amt = f"₹{int(c['extra_amount'])}" if c.get("extra_amount") else "TBD"
                lines.append(
                    f"• `{c.get('kapture_ticket_id') or c['ticket_id']}` | "
                    f"{c.get('current_partner_name', '—')} | {c.get('zone', '—')} | *{amt}*"
                )
                _notified_b2_tids.add(c["ticket_id"])
            actions.send_to_slack(webhook, "\n".join(lines))
        except Exception as e:
            app.logger.error(f"Slack new B2 alert error: {e}")

    from datetime import datetime, timedelta
    scheduler = BackgroundScheduler(daemon=True)
    # Defer first _auto_sync by 2 min so it doesn't overlap with the startup job
    scheduler.add_job(_auto_sync, "interval", minutes=2, id="auto_sync",
                      next_run_time=datetime.now() + timedelta(minutes=2))
    scheduler.add_job(_slack_pending_digest, "interval", minutes=30, id="slack_digest")
    scheduler.add_job(_slack_new_b2_alert, "interval", minutes=15, id="b2_new_alert")
    # Run B1/B4 sync immediately on startup so SQLite is populated after deploy
    scheduler.add_job(_sync_b1b4, id="b1b4_startup")
    scheduler.start()
except ImportError:
    pass

# ── Pages ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ── Cases ─────────────────────────────────────────────────────────────────────

@app.route("/api/cases")
def get_cases():
    state  = request.args.get("state", "all")
    zone   = request.args.get("zone", "all")
    search = request.args.get("search", "").strip() or None
    cases  = sheets_db.get_all_cases(state=state, zone=zone, search=search)
    return jsonify(cases)


@app.route("/api/cases/summary")
def get_summary():
    return jsonify(sheets_db.get_summary())


@app.route("/api/cases/zones")
def get_zones():
    return jsonify(sheets_db.get_all_zones())


@app.route("/api/cases/<ticket_id>")
def get_case(ticket_id):
    case = sheets_db.get_case(ticket_id)
    if not case:
        return jsonify({"error": "Not found"}), 404
    return jsonify(case)


# ── Sync ──────────────────────────────────────────────────────────────────────

@app.route("/api/sync", methods=["POST"])
def sync():
    cfg = config.load()
    if not cfg.get("metabase_url"):
        return jsonify({"error": "Metabase not configured. Go to Settings."}), 400
    if not cfg.get("metabase_database_id"):
        return jsonify({"error": "Database ID not set. Go to Settings → Discover Databases."}), 400
    try:
        result = _run_sync(cfg)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cases/<ticket_id>/fetch-kapture", methods=["POST"])
def fetch_kapture_single(ticket_id):
    cfg  = config.load()
    case = sheets_db.get_case(ticket_id)
    if not case:
        return jsonify({"error": "Case not found"}), 404
    kapture_id = case.get("kapture_ticket_id")
    if not kapture_id:
        return jsonify({"error": "No Kapture ticket ID on this case"}), 400
    try:
        from kapture import KaptureClient
        kap = KaptureClient(cfg.get("kapture_auth_header", ""))
        raw = kap.fetch_ticket(str(kapture_id))
        if raw:
            fields = kap.extract_breach_fields(raw)
            sheets_db.update_kapture_fields(
                ticket_id, fields["extra_amount"], fields["technician_name"],
                fields["voluntary_tip"], json.dumps(raw),
            )
        return jsonify(sheets_db.get_case(ticket_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Actions ───────────────────────────────────────────────────────────────────



# ── Customer Comms ────────────────────────────────────────────────────────────

@app.route("/api/customer-comms")
def get_customer_comms():
    return jsonify(sheets_db.get_pending_comms())


@app.route("/api/customer-comms/<ticket_id>/log", methods=["POST"])
def log_comms(ticket_id):
    body = request.json or {}
    notes = body.get("notes", "")
    ok = sheets_db.advance_state(ticket_id, "customer_comms", comms_notes=notes)
    if ok:
        return jsonify({"ok": True, "case": sheets_db.get_case(ticket_id)})
    return jsonify({"error": "Cannot advance state"}), 400


@app.route("/api/cases/<ticket_id>/advance", methods=["POST"])
def advance_case(ticket_id):
    body      = request.json or {}
    new_state = body.get("state")
    if not new_state:
        return jsonify({"error": "Missing state"}), 400
    ok = sheets_db.advance_state(ticket_id, new_state)
    if ok:
        return jsonify({"ok": True, "case": sheets_db.get_case(ticket_id)})
    return jsonify({"error": "Invalid state transition"}), 400


@app.route("/api/cases/<ticket_id>/undo", methods=["POST"])
def undo_case(ticket_id):
    ok = sheets_db.undo_state(ticket_id)
    if ok:
        return jsonify({"ok": True, "case": sheets_db.get_case(ticket_id)})
    return jsonify({"error": "Nothing to undo"}), 400




# ── CSV Downloads ─────────────────────────────────────────────────────────────

@app.route("/api/download/refund-csv")
def dl_refund():
    cases   = sheets_db.get_all_cases(state="detected")
    csv_str = actions.generate_refund_csv(cases)
    return Response(csv_str, mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=refund_cases.csv"})


@app.route("/api/csv/refund", methods=["POST"])
def csv_refund():
    tids  = (request.json or {}).get("ticket_ids", [])
    cases = ([sheets_db.get_case(t) for t in tids] if tids
             else sheets_db.get_all_cases(state="detected"))
    cases = [c for c in cases if c]
    return jsonify({"csv": actions.generate_refund_csv(cases), "count": len(cases)})


@app.route("/api/csv/penalty", methods=["POST"])
def csv_penalty():
    tids  = (request.json or {}).get("ticket_ids", [])
    cases = ([sheets_db.get_case(t) for t in tids] if tids
             else sheets_db.get_all_cases(state="customer_comms"))
    cases = [c for c in cases if c]
    xlsx_bytes = actions.generate_penalty_xlsx(cases)
    return Response(xlsx_bytes,
                    mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={"Content-Disposition": "attachment; filename=partner_penalty.xlsx"})


@app.route("/api/csv/partner-comms", methods=["POST"])
def csv_partner_comms_bulk():
    tids  = (request.json or {}).get("ticket_ids", [])
    cases = ([sheets_db.get_case(t) for t in tids] if tids
             else sheets_db.get_all_cases(state="partner_penalty"))
    cases = [c for c in cases if c]
    return jsonify({"csv": actions.generate_partner_comms_csv(cases), "count": len(cases)})


@app.route("/api/download/penalty-csv")
def dl_penalty():
    cases = sheets_db.get_all_cases(state="customer_comms")
    xlsx_bytes = actions.generate_penalty_xlsx(cases)
    return Response(xlsx_bytes,
                    mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={"Content-Disposition": "attachment; filename=partner_penalty.xlsx"})


@app.route("/api/download/partner-comms-csv")
def dl_partner_comms():
    cases   = sheets_db.get_all_cases(state="partner_penalty")
    csv_str = actions.generate_partner_comms_csv(cases)
    return Response(csv_str, mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=partner_comms.csv"})


def _calc_tat_minutes(t1_str, t2_str):
    """Calculate TAT in minutes between two timestamp strings."""
    try:
        from datetime import datetime as _dt
        t1 = _dt.strptime(t1_str.replace("T", " ")[:19], "%Y-%m-%d %H:%M:%S")
        t2 = _dt.strptime(t2_str.replace("T", " ")[:19], "%Y-%m-%d %H:%M:%S")
        return round((t2 - t1).total_seconds() / 60, 1)
    except Exception:
        return None


# ── Refund Upload ────────────────────────────────────────────────────────────

@app.route("/api/upload/refund-status", methods=["POST"])
def upload_refund_status():
    import csv
    import io
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No file uploaded"}), 400
    try:
        text = f.read().decode("utf-8-sig")
    except Exception:
        return jsonify({"error": "Could not read file as UTF-8"}), 400
    reader = csv.DictReader(io.StringIO(text))
    matched = []
    unmatched = []
    for row in reader:
        phone = (row.get("Contact Phone Number") or "").strip()
        payout_id = (row.get("Payout Link ID") or "").strip()
        if not phone or not payout_id:
            continue
        # Normalize: strip leading +91 or 91 prefix to get 10-digit number
        clean = phone.lstrip("+")
        if clean.startswith("91") and len(clean) > 10:
            clean = clean[2:]
        result = sheets_db.mark_refunded_by_mobile(clean, payout_id)
        if result["matched"]:
            matched.append(result)
        else:
            # Also try with original phone value
            if clean != phone:
                result2 = sheets_db.mark_refunded_by_mobile(phone, payout_id)
                if result2["matched"]:
                    matched.append(result2)
                    continue
            unmatched.append({"mobile": phone, "payout_link_id": payout_id})
    # ── Slack notification for matched refund cases ──
    if matched:
        try:
            slack_lines = ["<!channel>\n*Refund Processed — Customer Comms Pending*\n"]
            for m in matched:
                tid = m.get("ticket_id", "")
                case_data = sheets_db.get_case(tid)
                if not case_data:
                    continue
                kapture_id = case_data.get("kapture_ticket_id") or "N/A"
                partner_name = case_data.get("current_partner_name") or "N/A"
                mobile = case_data.get("customer_mobile") or "N/A"
                extra_amt = case_data.get("extra_amount")
                amt_str = f"₹{extra_amt}" if extra_amt is not None else "N/A"
                t1 = case_data.get("ticket_added_time_ist") or ""
                t2 = case_data.get("customer_refunded_at") or ""
                tat = _calc_tat_minutes(t1, t2)
                tat_str = f"{tat} min" if tat is not None else "N/A"
                slack_lines.append(
                    f"• Kapture {kapture_id} | {partner_name} | {mobile} | {amt_str} | TAT {tat_str}"
                )
            webhook_url = config.get("slack_webhook_url", "")
            if webhook_url and len(slack_lines) > 1:
                actions.send_to_slack(webhook_url, "\n".join(slack_lines))
        except Exception:
            pass  # Don't break upload response on Slack failure
    return jsonify({
        "ok": True,
        "matched_count": len(matched),
        "unmatched_count": len(unmatched),
        "matched": matched,
        "unmatched": unmatched,
    })


@app.route("/api/upload/penalty-status", methods=["POST"])
def upload_penalty_status():
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No file uploaded"}), 400

    rows = []
    fname = (f.filename or "").lower()
    if fname.endswith(".xlsx") or fname.endswith(".xls"):
        # Excel upload
        from openpyxl import load_workbook
        import io
        wb = load_workbook(io.BytesIO(f.read()), read_only=True)
        ws = wb.active
        headers = [str(c.value or "").strip() for c in next(ws.iter_rows(min_row=1, max_row=1))]
        for row in ws.iter_rows(min_row=2, values_only=True):
            rows.append(dict(zip(headers, row)))
        wb.close()
    else:
        # CSV fallback
        import csv, io
        try:
            text = f.read().decode("utf-8-sig")
        except Exception:
            return jsonify({"error": "Could not read file"}), 400
        rows = list(csv.DictReader(io.StringIO(text)))

    matched = []
    unmatched = []
    for row in rows:
        partner_id = str(row.get("AccountId") or row.get("Partner Id") or "").strip()
        if not partner_id:
            continue
        # Skip rows where Process Status is not "Yes"
        status = str(row.get("Process Status") or "").strip().lower()
        if status and status != "yes":
            unmatched.append({"partner_id": partner_id, "reason": row.get("Reason", "")})
            continue
        result = sheets_db.mark_penalty_by_upload(partner_id)
        if result["matched"]:
            matched.append(result)
        else:
            unmatched.append({"partner_id": partner_id})
    return jsonify({
        "ok": True,
        "matched_count": len(matched),
        "unmatched_count": len(unmatched),
        "matched": matched,
        "unmatched": unmatched,
    })


@app.route("/api/cases/<ticket_id>/confirm-comms", methods=["POST"])
def confirm_comms(ticket_id):
    body = request.json or {}
    notes = body.get("notes", "")
    ok = sheets_db.advance_state(ticket_id, "customer_comms", comms_notes=notes)
    if ok:
        return jsonify({"ok": True, "case": sheets_db.get_case(ticket_id)})
    return jsonify({"error": "Cannot confirm comms for this case"}), 400


# ── Visibility Dashboard ──────────────────────────────────────────────────────

@app.route("/api/cases/visibility")
def visibility_matrix():
    try:
        return jsonify(sheets_db.get_visibility_matrix())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Fraud Tracker ─────────────────────────────────────────────────────────────

@app.route("/api/cases/repeat-customers")
def repeat_customers():
    return jsonify(sheets_db.get_repeat_customers())


# ── FP2 Partner Email ─────────────────────────────────────────────────────────

@app.route("/api/breach2/template")
def breach2_template():
    from email_sender import get_fp2_template_info
    return jsonify(get_fp2_template_info())


@app.route("/api/breach2/preview-email", methods=["POST"])
def breach2_preview():
    from email_sender import render_fp2_email
    body = request.json or {}
    language = body.get("language", "both")
    selected_vars = body.get("selected_vars", [])
    values = body.get("values", {})
    try:
        result = render_fp2_email(language, selected_vars, values)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/breach2/send-email", methods=["POST"])
def breach2_send_email():
    from email_sender import render_fp2_email, send_email
    from google_sheets import get_all_partner_emails
    body = request.json or {}
    ticket_ids = body.get("ticket_ids", [])
    language = body.get("language", "both")
    selected_vars = body.get("selected_vars", [])
    values = body.get("values", {})
    test_email = body.get("test_email", "").strip()
    is_test = bool(test_email)

    partner_emails = get_all_partner_emails()
    results = []
    for tid in ticket_ids:
        case = sheets_db.get_case(tid)
        if not case:
            results.append({"ticket_id": tid, "ok": False, "error": "Case not found"})
            continue

        vars_filled = dict(values)
        # Auto-fill FP2 variables from case data
        if not vars_filled.get("MASKED_NUMBER"):
            mobile = case.get("customer_mobile", "")
            vars_filled["MASKED_NUMBER"] = (mobile[:4] + "XXXXXX") if len(mobile) >= 4 else mobile
        if not vars_filled.get("AMOUNT_COLLECTED"):
            vars_filled["AMOUNT_COLLECTED"] = str(int(float(case.get("extra_amount") or 0)))
        if not vars_filled.get("AMOUNT_REFUNDED"):
            vars_filled["AMOUNT_REFUNDED"] = str(int(float(case.get("extra_amount") or 0)))
        if not vars_filled.get("AMOUNT_RECOVERED"):
            vars_filled["AMOUNT_RECOVERED"] = str(int(float(case.get("extra_amount") or 0)))

        rendered = render_fp2_email(language, selected_vars, vars_filled)
        partner_name = (case.get("current_partner_name") or "").strip()
        email_info = partner_emails.get(partner_name.lower(), {})
        recipient = test_email if is_test else email_info.get("email", "")

        if not recipient:
            results.append({"ticket_id": tid, "ok": False, "error": "No partner email"})
            continue

        send_result = send_email(recipient, rendered["subject"], rendered["body_text"], rendered["body_html"])
        results.append({"ticket_id": tid, "partner": partner_name, "recipient": recipient, **send_result})

    return jsonify({"results": results, "total": len(results),
                    "sent": sum(1 for r in results if r.get("ok"))})


# ── Breach 1 (Disintermediation) ──────────────────────────────────────────────

@app.route("/api/breach1/sync", methods=["POST"])
def breach1_sync():
    """Import disintermediation cases from Google Sheet and match partner emails."""
    try:
        from google_sheets import fetch_disintermediation_cases, get_all_partner_emails
        cases = fetch_disintermediation_cases()
        emails = get_all_partner_emails()

        # Only import cases confirmed as disintermediation
        cases = [c for c in cases if (c.get("Disintermediation") or "").strip().lower() == "yes"]

        new_count = 0
        updated_count = 0
        for c in cases:
            partner = (c.get("PARTNER_NAME") or "").strip()
            email_info = emails.get(partner.lower(), {})
            data = {
                "lng_nas_id": c.get("LNG_NAS_ID", ""),
                "customer_mobile": c.get("MOBILE", ""),
                "expiry_dt": c.get("EXPIRY_DT", ""),
                "city": c.get("CITY", ""),
                "mis_city": c.get("MIS_CITY", ""),
                "zone": c.get("ZONE", ""),
                "partner_name": partner,
                "tenure": c.get("TENURE", ""),
                "r_oct": c.get("R total (Oct)", ""),
                "r_nov": c.get("R total (Nov)", ""),
                "r_dec": c.get("R total (Dec)", ""),
                "r_jan": c.get("R total (Jan)", ""),
                "risk_score": c.get("Risk score on wallet activity (Dec+Jan) (Scale 0-3)", ""),
                "partner_status": c.get("Status", ""),
                "connected": c.get("Connected", ""),
                "calling_remarks": c.get("Calling Remarks", ""),
                "disintermediation": c.get("Disintermediation", ""),
                "call_recording": c.get("Call Recording", ""),
                "called_by": c.get("Called By", ""),
                "call_timestamp": c.get("Call Timestamp (Date)", ""),
                "calling_status": c.get("Calling Status", ""),
                "partner_email": email_info.get("email", ""),
            }
            existing = db.get_breach1_cases(search=data["lng_nas_id"])
            existing = [e for e in existing if e["customer_mobile"] == data["customer_mobile"]]
            if not existing:
                new_count += 1
            else:
                updated_count += 1
            db.upsert_breach1_case(data)

        return jsonify({"ok": True, "new_cases": new_count, "updated": updated_count, "total_fetched": len(cases)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/breach1/cases")
def breach1_cases():
    partner = request.args.get("partner", "all")
    zone = request.args.get("zone", "all")
    status = request.args.get("status", "all")
    email_state = request.args.get("email_state", "all")
    search = request.args.get("search", "").strip() or None
    return jsonify(db.get_breach1_cases(partner=partner, zone=zone, status=status,
                                        email_state=email_state, search=search))


@app.route("/api/breach1/cases/<int:case_id>")
def breach1_case(case_id):
    case = db.get_breach1_case(case_id)
    if not case:
        return jsonify({"error": "Not found"}), 404
    return jsonify(case)


@app.route("/api/breach1/summary")
def breach1_summary():
    return jsonify(db.get_breach1_summary())


@app.route("/api/breach1/partners")
def breach1_partners():
    return jsonify(db.get_breach1_partners())


@app.route("/api/breach1/zones")
def breach1_zones():
    return jsonify(db.get_breach1_zones())


@app.route("/api/breach1/template")
def breach1_template():
    from email_sender import get_template_info
    return jsonify(get_template_info())


@app.route("/api/breach1/preview-email", methods=["POST"])
def breach1_preview():
    from email_sender import render_email
    body = request.json or {}
    language = body.get("language", "both")
    selected_vars = body.get("selected_vars", [])
    values = body.get("values", {})
    try:
        result = render_email(language, selected_vars, values)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/breach1/send-email", methods=["POST"])
def breach1_send_email():
    from email_sender import render_email, send_email
    body = request.json or {}
    case_ids = body.get("case_ids", [])
    language = body.get("language", "both")
    selected_vars = body.get("selected_vars", [])
    values = body.get("values", {})
    test_email = body.get("test_email", "").strip()
    is_test = bool(test_email)

    results = []
    for cid in case_ids:
        case = db.get_breach1_case(cid)
        if not case:
            results.append({"case_id": cid, "ok": False, "error": "Case not found"})
            continue

        # Auto-fill from case data where possible
        vars_filled = dict(values)
        if "LAST_SERVICE_DATE" in selected_vars and not vars_filled.get("LAST_SERVICE_DATE"):
            vars_filled["LAST_SERVICE_DATE"] = case.get("expiry_dt", "")
        if "PHONE_NUMBER_FIRST4" in selected_vars and not vars_filled.get("PHONE_NUMBER_FIRST4"):
            mobile = case.get("customer_mobile", "")
            vars_filled["PHONE_NUMBER_FIRST4"] = mobile[:4] if len(mobile) >= 4 else mobile

        rendered = render_email(language, selected_vars, vars_filled)
        recipient = test_email if is_test else (case.get("partner_email") or "")

        if not recipient:
            results.append({"case_id": cid, "ok": False, "error": "No partner email"})
            continue

        send_result = send_email(recipient, rendered["subject"], rendered["body_text"], rendered["body_html"])

        db.log_breach1_email(
            case_id=cid,
            partner_name=case.get("partner_name", ""),
            partner_email=case.get("partner_email", ""),
            recipient_email=recipient,
            case_type=1,
            language=language,
            variables_json=json.dumps(vars_filled),
            subject=rendered["subject"],
            body_preview=rendered["body_text"][:500],
            is_test=is_test,
            status="sent" if send_result["ok"] else "failed",
            error=send_result.get("error"),
        )

        if send_result["ok"] and not is_test:
            db.mark_breach1_email_sent([cid], 1)

        results.append({"case_id": cid, **send_result})

    return jsonify({"results": results, "total": len(results),
                    "sent": sum(1 for r in results if r.get("ok"))})


@app.route("/api/breach1/email-log")
def breach1_email_log():
    return jsonify(db.get_breach1_email_log())


@app.route("/api/breach1/escalation-preview")
def breach1_escalation_preview():
    """Preview rows to append to escalation Google Sheet."""
    try:
        from google_sheets import get_existing_escalation_customers, get_all_partner_emails
        existing_mobiles = get_existing_escalation_customers()
        emails = get_all_partner_emails()
        cases = db.get_breach1_cases()

        rows = []
        skipped = 0
        for c in sorted(cases, key=lambda x: (x.get("partner_name", ""), x.get("customer_mobile", ""))):
            mobile = (c.get("customer_mobile") or "").strip()
            if mobile in existing_mobiles:
                skipped += 1
                continue

            email_info = emails.get((c.get("partner_name") or "").lower(), {})
            partner_id = email_info.get("partner_id", "")
            partner_email = c.get("partner_email") or email_info.get("email", "")

            detection_date = c.get("call_timestamp") or ""
            email_done = "Yes" if c.get("email_state") == "sent" else "No"
            email_date = (c.get("email_sent_at") or "")[:10] if c.get("email_sent_at") else ""

            row = [
                partner_id,
                c.get("partner_name", ""),
                mobile,
                "Breach Rule 1 System Fundamentals",
                detection_date,
                "6month churn logic",
                "",      # Penalty Amount
                "No",    # Penalty Done
                "",      # Penalty Done Date
                email_done,
                email_date,
                "No",    # WhatsApp
                "",      # WhatsApp Date
                "",      # Partner Mobile
                c.get("partner_name", ""),
                partner_email,
                "",      # Link
                "",      # Comments
            ]
            rows.append(row)

        return jsonify({"ok": True, "rows": rows, "skipped": skipped, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/breach1/escalation-push", methods=["POST"])
def breach1_escalation_push():
    """Append rows to FP1 Escalation Google Sheet."""
    try:
        from google_sheets import append_escalation_rows, get_existing_escalation_customers, get_all_partner_emails
        existing_mobiles = get_existing_escalation_customers()
        emails = get_all_partner_emails()
        cases = db.get_breach1_cases()

        rows = []
        for c in sorted(cases, key=lambda x: (x.get("partner_name", ""), x.get("customer_mobile", ""))):
            mobile = (c.get("customer_mobile") or "").strip()
            if mobile in existing_mobiles:
                continue

            email_info = emails.get((c.get("partner_name") or "").lower(), {})
            partner_id = email_info.get("partner_id", "")
            partner_email = c.get("partner_email") or email_info.get("email", "")

            detection_date = c.get("call_timestamp") or ""
            email_done = "Yes" if c.get("email_state") == "sent" else "No"
            email_date = (c.get("email_sent_at") or "")[:10] if c.get("email_sent_at") else ""

            row = [
                partner_id,
                c.get("partner_name", ""),
                mobile,
                "Breach Rule 1 System Fundamentals",
                detection_date,
                "6month churn logic",
                "",
                "No",
                "",
                email_done,
                email_date,
                "No",
                "",
                "",
                c.get("partner_name", ""),
                partner_email,
                "",
                "",
            ]
            rows.append(row)

        if not rows:
            return jsonify({"ok": True, "appended": 0, "message": "All cases already in sheet"})

        appended = append_escalation_rows(rows)
        return jsonify({"ok": True, "appended": len(rows), "message": f"{len(rows)} row(s) added"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Breach 4 (Router Misuse) ──────────────────────────────────────────────────

@app.route("/api/breach4/sync", methods=["POST"])
def breach4_sync():
    """Import FP4 cases from Google Sheet."""
    try:
        from google_sheets import fetch_fp4_cases, get_all_partner_emails
        cases = fetch_fp4_cases()
        emails = get_all_partner_emails()

        new_count = 0
        updated_count = 0
        for c in cases:
            partner_name = (c.get("Partner Name") or "").strip()
            partner_id = (c.get("Partner Id") or "").strip()
            email_info = emails.get(partner_name.lower(), {})
            data = {
                "partner_id": partner_id,
                "partner_name": partner_name,
                "customer_details": (c.get("Customer Details") or "").strip(),
                "principle_broken": (c.get("Principle Broken") or "").strip(),
                "device_id": (c.get("Device Id") or "").strip(),
                "date_reported": (c.get("Date Reported") or "").strip(),
                "reporting_channel": (c.get("Reporting Channel") or "").strip(),
                "penalty_amount": (c.get("Penalty Amount") or "").strip(),
                "penalty_done": (c.get("Penalty Done") or "").strip(),
                "penalty_done_date": (c.get("Penalty Done Date") or "").strip(),
                "partner_email_comms": (c.get("Partner Email Comms Done") or "").strip(),
                "email_date": (c.get("Email Date") or "").strip(),
                "whatsapp_comms": (c.get("Partner Text/Whatsapp Comms Done") or "").strip(),
                "whatsapp_date": (c.get("Whatsapp Date") or "").strip(),
                "partner_mobile": (c.get("Partner Mobile") or "").strip(),
                "link": (c.get("Link") or "").strip(),
                "comments": (c.get("Comments") or "").strip(),
                "partner_email": email_info.get("email", ""),
            }
            # Skip rows with no real data
            if not data["partner_id"] and not data["partner_name"] and not data["device_id"]:
                continue

            existing = db.get_breach4_cases(search=data["device_id"])
            existing = [e for e in existing if e["partner_id"] == data["partner_id"]
                        and e["customer_details"] == data["customer_details"]]
            if not existing:
                new_count += 1
            else:
                updated_count += 1
            db.upsert_breach4_case(data)

        return jsonify({"ok": True, "new_cases": new_count, "updated": updated_count, "total_fetched": len(cases)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/breach4/cases")
def breach4_cases():
    partner = request.args.get("partner", "all")
    email_state = request.args.get("email_state", "all")
    search = request.args.get("search", "").strip() or None
    return jsonify(db.get_breach4_cases(partner=partner, email_state=email_state, search=search))


@app.route("/api/breach4/cases/<int:case_id>")
def breach4_case(case_id):
    case = db.get_breach4_case(case_id)
    if not case:
        return jsonify({"error": "Not found"}), 404
    return jsonify(case)


@app.route("/api/breach4/summary")
def breach4_summary():
    return jsonify(db.get_breach4_summary())


@app.route("/api/breach4/partners")
def breach4_partners():
    return jsonify(db.get_breach4_partners())


@app.route("/api/breach4/template")
def breach4_template():
    from email_sender import get_fp4_template_info
    return jsonify(get_fp4_template_info())


@app.route("/api/breach4/preview-email", methods=["POST"])
def breach4_preview():
    from email_sender import render_fp4_email
    body = request.json or {}
    language = body.get("language", "both")
    selected_vars = body.get("selected_vars", [])
    values = body.get("values", {})
    try:
        result = render_fp4_email(language, selected_vars, values)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/breach4/send-email", methods=["POST"])
def breach4_send_email():
    from email_sender import render_fp4_email, send_email
    body = request.json or {}
    case_ids = body.get("case_ids", [])
    language = body.get("language", "both")
    selected_vars = body.get("selected_vars", [])
    values = body.get("values", {})
    test_email = body.get("test_email", "").strip()
    is_test = bool(test_email)

    results = []
    for cid in case_ids:
        case = db.get_breach4_case(cid)
        if not case:
            results.append({"case_id": cid, "ok": False, "error": "Case not found"})
            continue

        vars_filled = dict(values)
        if "ROUTER_ID" in selected_vars and not vars_filled.get("ROUTER_ID"):
            vars_filled["ROUTER_ID"] = case.get("device_id", "")

        rendered = render_fp4_email(language, selected_vars, vars_filled)
        recipient = test_email if is_test else (case.get("partner_email") or "")

        if not recipient:
            results.append({"case_id": cid, "ok": False, "error": "No partner email"})
            continue

        send_result = send_email(recipient, rendered["subject"], rendered["body_text"], rendered["body_html"])

        db.log_breach4_email(
            case_id=cid,
            partner_name=case.get("partner_name", ""),
            partner_email=case.get("partner_email", ""),
            recipient_email=recipient,
            language=language,
            variables_json=json.dumps(vars_filled),
            subject=rendered["subject"],
            body_preview=rendered["body_text"][:500],
            is_test=is_test,
            status="sent" if send_result["ok"] else "failed",
            error=send_result.get("error"),
        )

        if send_result["ok"] and not is_test:
            db.mark_breach4_email_sent([cid])

        results.append({"case_id": cid, **send_result})

    return jsonify({"results": results, "total": len(results),
                    "sent": sum(1 for r in results if r.get("ok"))})


@app.route("/api/breach4/email-log")
def breach4_email_log():
    return jsonify(db.get_breach4_email_log())


@app.route("/api/breach4/escalation-preview")
def breach4_escalation_preview():
    """Preview rows to update in FP4 Google Sheet."""
    try:
        from google_sheets import get_existing_fp4_customers
        existing = get_existing_fp4_customers()
        cases = db.get_breach4_cases()

        rows = []
        skipped = 0
        for c in sorted(cases, key=lambda x: (x.get("partner_name", ""), x.get("customer_details", ""))):
            cust = (c.get("customer_details") or "").strip()
            email_done = "Yes" if c.get("email_state") == "sent" else c.get("partner_email_comms", "No")
            email_date = (c.get("email_sent_at") or "")[:10] if c.get("email_sent_at") else c.get("email_date", "")

            row = [
                c.get("partner_id", ""),
                c.get("partner_name", ""),
                cust,
                c.get("principle_broken", ""),
                c.get("device_id", ""),
                c.get("date_reported", ""),
                c.get("reporting_channel", ""),
                c.get("penalty_amount", ""),
                c.get("penalty_done", ""),
                c.get("penalty_done_date", ""),
                email_done,
                email_date,
                c.get("whatsapp_comms", ""),
                c.get("whatsapp_date", ""),
                c.get("partner_mobile", ""),
                c.get("link", ""),
                c.get("comments", ""),
            ]
            rows.append(row)

        return jsonify({"ok": True, "rows": rows, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/breach4/escalation-push", methods=["POST"])
def breach4_escalation_push():
    """Overwrite FP4 sheet data rows with current local data."""
    try:
        from google_sheets import _get_service, ESCALATION_SHEET_ID
        cases = db.get_breach4_cases()

        rows = []
        for c in sorted(cases, key=lambda x: (x.get("partner_name", ""), x.get("customer_details", ""))):
            email_done = "Yes" if c.get("email_state") == "sent" else c.get("partner_email_comms", "No")
            email_date = (c.get("email_sent_at") or "")[:10] if c.get("email_sent_at") else c.get("email_date", "")

            row = [
                c.get("partner_id", ""),
                c.get("partner_name", ""),
                c.get("customer_details", ""),
                c.get("principle_broken", ""),
                c.get("device_id", ""),
                c.get("date_reported", ""),
                c.get("reporting_channel", ""),
                c.get("penalty_amount", ""),
                c.get("penalty_done", ""),
                c.get("penalty_done_date", ""),
                email_done,
                email_date,
                c.get("whatsapp_comms", ""),
                c.get("whatsapp_date", ""),
                c.get("partner_mobile", ""),
                c.get("link", ""),
                c.get("comments", ""),
            ]
            rows.append(row)

        if not rows:
            return jsonify({"ok": True, "appended": 0, "message": "No cases to push"})

        service = _get_service(readonly=False)
        # Clear existing data rows (keep header)
        service.spreadsheets().values().clear(
            spreadsheetId=ESCALATION_SHEET_ID,
            range="'FP4 : Router Misuse'!A2:Q5000",
        ).execute()
        # Write updated rows
        body = {"values": rows}
        service.spreadsheets().values().update(
            spreadsheetId=ESCALATION_SHEET_ID,
            range="'FP4 : Router Misuse'!A2",
            valueInputOption="USER_ENTERED",
            body=body,
        ).execute()

        return jsonify({"ok": True, "appended": len(rows), "message": f"{len(rows)} row(s) updated"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Settings ──────────────────────────────────────────────────────────────────

@app.route("/api/settings")
def get_settings():
    cfg  = config.load()
    safe = dict(cfg)
    if safe.get("metabase_password"):
        safe["metabase_password"] = "••••••••"
    return jsonify(safe)


@app.route("/api/settings", methods=["POST"])
def save_settings():
    body = request.json or {}
    if body.get("metabase_password") == "••••••••":
        body.pop("metabase_password")
    config.save(body)
    return jsonify({"ok": True})


@app.route("/api/settings/test-metabase", methods=["POST"])
def test_metabase():
    cfg = config.load()
    try:
        from metabase import MetabaseClient
        client = MetabaseClient(
            cfg["metabase_url"], cfg.get("metabase_database_id", ""),
            api_key=cfg.get("metabase_api_key", ""),
            username=cfg.get("metabase_username", ""),
            password=cfg.get("metabase_password", ""),
        )
        ok = client.test_connection()
        return jsonify({"ok": ok})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/settings/databases", methods=["GET"])
def list_databases():
    cfg = config.load()
    try:
        from metabase import MetabaseClient
        client = MetabaseClient(
            cfg["metabase_url"], cfg.get("metabase_database_id", ""),
            api_key=cfg.get("metabase_api_key", ""),
            username=cfg.get("metabase_username", ""),
            password=cfg.get("metabase_password", ""),
        )
        dbs = client.list_databases()
        return jsonify({"ok": True, "databases": dbs})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/settings/test-slack", methods=["POST"])
def test_slack():
    cfg    = config.load()
    result = actions.send_to_slack(
        cfg.get("slack_webhook_url", ""),
        ":white_check_mark: WIOM Breach Tracker — Slack test successful!",
    )
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=False, port=5050, use_reloader=False, threaded=True)
