/**
 * WIOM Breach Tracker — Google Apps Script API
 *
 * SETUP:
 * 1. Create a new Google Sheet with a tab named "Cases"
 *    Row 1 headers (A-V):
 *    ticket_id | kapture_ticket_id | ticket_added_time_ist | customer_mobile |
 *    current_partner_account_id | current_partner_name | zone | partner_mobile |
 *    new_install_flag | install_emp_role | install_emp_id | install_name |
 *    extra_amount | technician_name | voluntary_tip | state | detected_at |
 *    customer_refunded_at | customer_comms_at | partner_penalty_at |
 *    refund_payout_link | previous_state
 *
 * 2. Extensions → Apps Script → paste this code
 * 3. Deploy → New deployment → Web app
 *    - Execute as: Me
 *    - Who has access: Anyone
 * 4. Copy the URL and paste into the static site CONFIG
 */

const SHEET_NAME = "Cases";
const HEADERS = [
  "ticket_id","kapture_ticket_id","ticket_added_time_ist","customer_mobile",
  "current_partner_account_id","current_partner_name","zone","partner_mobile",
  "new_install_flag","install_emp_role","install_emp_id","install_name",
  "extra_amount","technician_name","voluntary_tip","state","detected_at",
  "customer_refunded_at","customer_comms_at","partner_penalty_at",
  "refund_payout_link","previous_state"
];

const COL = {};
HEADERS.forEach((h, i) => COL[h] = i);

const VALID_TRANSITIONS = {
  "detected":          ["customer_refunded"],
  "customer_refunded": ["customer_comms"],
  "customer_comms":    ["partner_penalty"],
  "partner_penalty":   [],
};

const STATE_TS_COL = {
  "customer_refunded": "customer_refunded_at",
  "customer_comms":    "customer_comms_at",
  "partner_penalty":   "partner_penalty_at",
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function nowIST() {
  const d = new Date();
  d.setMinutes(d.getMinutes() + d.getTimezoneOffset() + 330);
  return Utilities.formatDate(d, "Asia/Kolkata", "yyyy-MM-dd HH:mm:ss");
}

function resp(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function getSheet() {
  return SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEET_NAME);
}

function getAllData(sheet) {
  const last = sheet.getLastRow();
  if (last < 2) return [];
  return sheet.getRange(2, 1, last - 1, HEADERS.length).getValues();
}

function rowToObj(row) {
  const obj = {};
  HEADERS.forEach((h, i) => { obj[h] = row[i] === "" ? null : row[i]; });
  if (obj.extra_amount !== null) obj.extra_amount = Number(obj.extra_amount);
  return obj;
}

function findRow(sheet, ticketId) {
  const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, 1).getValues();
  for (let i = 0; i < data.length; i++) {
    if (String(data[i][0]) === String(ticketId)) return i + 2;
  }
  return -1;
}

function getCaseAtRow(sheet, row) {
  const vals = sheet.getRange(row, 1, 1, HEADERS.length).getValues()[0];
  return rowToObj(vals);
}

function setCellByCol(sheet, row, colName, value) {
  sheet.getRange(row, COL[colName] + 1).setValue(value);
}

// ── doGet ────────────────────────────────────────────────────────────────────

function doGet(e) {
  const action = (e.parameter.action || "getCases");
  const sheet = getSheet();

  if (action === "getCases") {
    return resp(getCases(sheet, e.parameter));
  } else if (action === "getSummary") {
    return resp(getSummary(sheet));
  } else if (action === "getZones") {
    return resp(getZones(sheet));
  } else if (action === "getCase") {
    return resp(getCase(sheet, e.parameter.ticket_id));
  }
  return resp({ error: "Unknown action: " + action });
}

// ── doPost ───────────────────────────────────────────────────────────────────

function doPost(e) {
  const data = JSON.parse(e.postData.contents);
  const action = data.action;
  const sheet = getSheet();

  if (action === "advanceState") {
    return resp(advanceState(sheet, data.ticket_id, data.new_state));
  } else if (action === "undoState") {
    return resp(undoState(sheet, data.ticket_id));
  } else if (action === "confirmComms") {
    return resp(advanceState(sheet, data.ticket_id, "customer_comms"));
  } else if (action === "bulkConfirmComms") {
    return resp(bulkConfirmComms(sheet, data.ticket_ids || []));
  } else if (action === "uploadRefundStatus") {
    return resp(uploadRefundStatus(sheet, data.entries || []));
  } else if (action === "uploadPenaltyStatus") {
    return resp(uploadPenaltyStatus(sheet, data.entries || []));
  } else if (action === "upsertCase") {
    return resp(upsertCase(sheet, data.caseData || {}));
  }
  return resp({ error: "Unknown action: " + action });
}

// ── Read actions ─────────────────────────────────────────────────────────────

function getCases(sheet, params) {
  const rows = getAllData(sheet);
  let cases = rows.map(rowToObj);

  const state = params.state || "all";
  const zone = params.zone || "all";
  const search = (params.search || "").trim().toLowerCase();

  if (state && state !== "all") {
    cases = cases.filter(c => c.state === state);
  }
  if (zone && zone !== "all") {
    cases = cases.filter(c => (c.zone || "").toLowerCase().includes(zone.toLowerCase()));
  }
  if (search) {
    cases = cases.filter(c =>
      (c.ticket_id || "").toLowerCase().includes(search) ||
      (c.customer_mobile || "").toLowerCase().includes(search) ||
      (c.current_partner_name || "").toLowerCase().includes(search) ||
      (c.zone || "").toLowerCase().includes(search)
    );
  }
  cases.sort((a, b) => (b.detected_at || "").localeCompare(a.detected_at || ""));
  return cases;
}

function getSummary(sheet) {
  const cases = getAllData(sheet).map(rowToObj);
  const states = ["detected", "customer_refunded", "customer_comms", "partner_penalty"];
  const by_state = {};
  states.forEach(s => { by_state[s] = 0; });
  let total_amount = 0;
  cases.forEach(c => {
    if (by_state[c.state] !== undefined) by_state[c.state]++;
    total_amount += (c.extra_amount || 0);
  });
  return { total: cases.length, by_state: by_state, total_amount: Math.round(total_amount * 100) / 100 };
}

function getZones(sheet) {
  const cases = getAllData(sheet).map(rowToObj);
  const zones = new Set();
  cases.forEach(c => { if (c.zone) zones.add(c.zone); });
  return Array.from(zones).sort();
}

function getCase(sheet, ticketId) {
  const row = findRow(sheet, ticketId);
  if (row === -1) return { error: "Not found" };
  return getCaseAtRow(sheet, row);
}

// ── Write actions ────────────────────────────────────────────────────────────

function advanceState(sheet, ticketId, newState) {
  const row = findRow(sheet, ticketId);
  if (row === -1) return { error: "Case not found" };

  const c = getCaseAtRow(sheet, row);
  const valid = VALID_TRANSITIONS[c.state] || [];
  if (valid.indexOf(newState) === -1) {
    return { error: "Invalid transition: " + c.state + " -> " + newState };
  }

  const ts = nowIST();
  setCellByCol(sheet, row, "state", newState);
  setCellByCol(sheet, row, "previous_state", c.state);

  const tsCol = STATE_TS_COL[newState];
  if (tsCol) setCellByCol(sheet, row, tsCol, ts);

  return { ok: true, case: getCaseAtRow(sheet, row) };
}

function undoState(sheet, ticketId) {
  const row = findRow(sheet, ticketId);
  if (row === -1) return { error: "Case not found" };

  const c = getCaseAtRow(sheet, row);
  if (!c.previous_state) return { error: "Nothing to undo" };

  const tsCol = STATE_TS_COL[c.state];
  if (tsCol) setCellByCol(sheet, row, tsCol, "");

  setCellByCol(sheet, row, "state", c.previous_state);
  setCellByCol(sheet, row, "previous_state", "");

  return { ok: true, case: getCaseAtRow(sheet, row) };
}

function bulkConfirmComms(sheet, ticketIds) {
  let done = 0;
  ticketIds.forEach(tid => {
    const r = advanceState(sheet, tid, "customer_comms");
    if (r.ok) done++;
  });
  return { ok: true, count: done };
}

function uploadRefundStatus(sheet, entries) {
  const allData = getAllData(sheet);
  const matched = [];
  const unmatched = [];

  entries.forEach(entry => {
    let phone = (entry.mobile || "").trim();
    const payoutId = (entry.payout_link_id || "").trim();
    if (!phone || !payoutId) return;

    let clean = phone.replace(/^\+/, "");
    if (clean.startsWith("91") && clean.length > 10) clean = clean.substring(2);

    let foundRow = -1;
    for (let i = 0; i < allData.length; i++) {
      const mob = String(allData[i][COL.customer_mobile] || "");
      const st = String(allData[i][COL.state] || "");
      if (st === "detected" && (mob === clean || mob === phone)) {
        foundRow = i + 2;
        break;
      }
    }

    if (foundRow === -1) {
      unmatched.push({ mobile: phone, payout_link_id: payoutId });
      return;
    }

    const payoutUrl = "https://payout-links.razorpay.com/v1/payout-links/" + payoutId + "/view/#/";
    const ts = nowIST();
    setCellByCol(sheet, foundRow, "state", "customer_refunded");
    setCellByCol(sheet, foundRow, "previous_state", "detected");
    setCellByCol(sheet, foundRow, "customer_refunded_at", ts);
    setCellByCol(sheet, foundRow, "refund_payout_link", payoutUrl);

    matched.push({ mobile: phone, ticket_id: String(allData[foundRow - 2][COL.ticket_id]) });
  });

  return { ok: true, matched_count: matched.length, unmatched_count: unmatched.length, matched: matched, unmatched: unmatched };
}

function uploadPenaltyStatus(sheet, entries) {
  const allData = getAllData(sheet);
  const matched = [];
  const unmatched = [];

  entries.forEach(entry => {
    const pid = (entry.partner_id || "").trim();
    if (!pid) return;

    let found = false;
    for (let i = 0; i < allData.length; i++) {
      const aPid = String(allData[i][COL.current_partner_account_id] || "");
      const st = String(allData[i][COL.state] || "");
      if (st === "customer_comms" && aPid === pid) {
        const row = i + 2;
        const ts = nowIST();
        setCellByCol(sheet, row, "state", "partner_penalty");
        setCellByCol(sheet, row, "previous_state", "customer_comms");
        setCellByCol(sheet, row, "partner_penalty_at", ts);
        found = true;
      }
    }

    if (found) matched.push({ partner_id: pid });
    else unmatched.push({ partner_id: pid });
  });

  return { ok: true, matched_count: matched.length, unmatched_count: unmatched.length };
}

function upsertCase(sheet, data) {
  const tid = String(data.ticket_id || "");
  if (!tid) return { error: "No ticket_id" };

  const row = findRow(sheet, tid);
  if (row !== -1) {
    // Update safe fields only
    const safe = ["ticket_added_time_ist","customer_mobile","current_partner_account_id",
      "current_partner_name","zone","partner_mobile","new_install_flag",
      "install_emp_role","install_emp_id","install_name","kapture_ticket_id",
      "extra_amount","technician_name","voluntary_tip"];
    safe.forEach(k => {
      if (data[k] !== undefined) setCellByCol(sheet, row, k, data[k]);
    });
    return { ok: true, updated: true };
  }

  // New case
  const newRow = [];
  HEADERS.forEach(h => {
    if (h === "state") newRow.push(data.state || "detected");
    else if (h === "detected_at") newRow.push(data.detected_at || nowIST());
    else newRow.push(data[h] || "");
  });
  sheet.appendRow(newRow);
  return { ok: true, inserted: true };
}
