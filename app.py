import sqlite3, json, urllib.request, urllib.parse, io, csv
from flask import Flask, request, jsonify, render_template, g
from datetime import datetime

app = Flask(__name__)
DB_PATH = "finance.db"
OWNERS = ["אני", "בן/בת זוג", "ילד 1", "ילד 2", "ילד 3"]
RESOURCE_ID = "a30dcbea-a1d2-482c-ae29-8f781f5025fb"
GOV_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop("db", None)
    if db: db.close()

def init_db():
    with sqlite3.connect(DB_PATH) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS policies (
            id INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT NOT NULL,
            type TEXT NOT NULL, institute TEXT, policy_number TEXT,
            start_month TEXT, fee REAL, amount REAL DEFAULT 0,
            update_month TEXT, track TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')))""")
        try: db.execute("ALTER TABLE policies ADD COLUMN track TEXT")
        except: pass
        db.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
        db.execute("INSERT OR IGNORE INTO settings (key,value) VALUES ('owners',?)", (json.dumps(OWNERS),))
        db.commit()

@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/settings")
def get_settings():
    db = get_db()
    row = db.execute("SELECT value FROM settings WHERE key='owners'").fetchone()
    return jsonify({"owners": json.loads(row["value"]) if row else OWNERS})

@app.route("/api/settings/owners", methods=["PUT"])
def update_owners():
    db = get_db()
    db.execute("INSERT OR REPLACE INTO settings (key,value) VALUES ('owners',?)",
               (json.dumps(request.json.get("owners", OWNERS)),))
    db.commit()
    return jsonify({"ok": True})

@app.route("/api/policies")
def get_policies():
    db = get_db()
    return jsonify([dict(r) for r in db.execute("SELECT * FROM policies ORDER BY owner, type").fetchall()])

@app.route("/api/policies", methods=["POST"])
def add_policy():
    d = request.json; db = get_db()
    cur = db.execute("""INSERT INTO policies
        (owner,type,institute,policy_number,start_month,fee,amount,update_month,track,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (d.get("owner"),d.get("type"),d.get("institute"),d.get("policy_number"),
         d.get("start_month"),d.get("fee"),d.get("amount"),d.get("update_month"),
         d.get("track"),datetime.now().isoformat()))
    db.commit()
    return jsonify(dict(db.execute("SELECT * FROM policies WHERE id=?", (cur.lastrowid,)).fetchone())), 201

@app.route("/api/policies/<int:pid>", methods=["PUT"])
def update_policy(pid):
    d = request.json; db = get_db()
    db.execute("""UPDATE policies SET owner=?,type=?,institute=?,policy_number=?,
        start_month=?,fee=?,amount=?,update_month=?,track=?,updated_at=? WHERE id=?""",
        (d.get("owner"),d.get("type"),d.get("institute"),d.get("policy_number"),
         d.get("start_month"),d.get("fee"),d.get("amount"),d.get("update_month"),
         d.get("track"),datetime.now().isoformat(),pid))
    db.commit()
    return jsonify(dict(db.execute("SELECT * FROM policies WHERE id=?", (pid,)).fetchone()))

@app.route("/api/policies/<int:pid>", methods=["DELETE"])
def delete_policy(pid):
    db = get_db()
    db.execute("DELETE FROM policies WHERE id=?", (pid,))
    db.commit()
    return jsonify({"ok": True})

@app.route("/api/yields/debug")
def debug_yields():
    q = request.args.get("q", "כלל").strip()
    results = {}

    # plain_q - show first 5 records with all fields decoded
    try:
        params = urllib.parse.urlencode({"resource_id": RESOURCE_ID, "q": q, "limit": 5})
        url = f"https://data.gov.il/api/3/action/datastore_search?{params}"
        req = urllib.request.Request(url, headers=GOV_HEADERS)
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read())
        results["total"] = data.get("result", {}).get("total", 0)
        results["first_5_fund_names"] = [r.get("FUND_NAME") for r in data.get("result", {}).get("records", [])]
        results["first_5_corps"] = [r.get("MANAGING_CORPORATION") for r in data.get("result", {}).get("records", [])]
    except Exception as e:
        results["error"] = str(e)

    # Also try searching with MANAGING_CORPORATION filter using exact name from results
    return jsonify(results)


GOV_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

# Known field mapping from gemelnet API
YIELD_FIELDS = {
    "name":    "FUND_NAME",
    "corp":    "MANAGING_CORPORATION",
    "type":    "FUND_CLASSIFICATION",
    "spec":    "SPECIALIZATION",
    "period":  "REPORT_PERIOD",
    "monthly": "MONTHLY_YIELD",
    "ytd":     "YEAR_TO_DATE_YIELD",
    "y3":      "YIELD_TRAILING_3_YRS",
    "y5":      "YIELD_TRAILING_5_YRS",
    "fee":     "AVG_ANNUAL_MANAGEMENT_FEE",
    "assets":  "TOTAL_ASSETS",
}

@app.route("/api/yields")
def get_yields():
    """
    Two-step search:
    1. plain q to find exact MANAGING_CORPORATION name
    2. filter by corp (exact) to get all their funds
    3. if track provided, filter FUND_NAME to only rows containing track terms
    """
    q = request.args.get("q", "").strip()
    track = request.args.get("track", "").strip()
    if not q:
        return jsonify({"error": "missing q"}), 400

    # Step 1: find exact MANAGING_CORPORATION name
    # Fetch a few records and pick the corp whose name best matches q
    try:
        params = urllib.parse.urlencode({"resource_id": RESOURCE_ID, "q": q, "limit": 10})
        req = urllib.request.Request(
            f"https://data.gov.il/api/3/action/datastore_search?{params}", headers=GOV_HEADERS)
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read())
        records = data.get("result", {}).get("records", [])
        if not records:
            return jsonify({"records": [], "count": 0, "corp": None})
        # Pick the corp name that contains q (case-insensitive)
        q_lower = q.lower()
        corp_name = ""
        for r in records:
            c = r.get("MANAGING_CORPORATION") or ""
            if q_lower in c.lower() or c.lower() in q_lower:
                corp_name = c
                break
        if not corp_name:
            corp_name = records[0].get("MANAGING_CORPORATION", "")
    except Exception as e:
        return jsonify({"error": str(e), "records": []}), 200

    # Step 2: get all funds of this corp
    try:
        params = urllib.parse.urlencode({
            "resource_id": RESOURCE_ID,
            "filters": json.dumps({"MANAGING_CORPORATION": corp_name}),
            "limit": 500,
            "sort": "REPORT_PERIOD desc"
        })
        req = urllib.request.Request(
            f"https://data.gov.il/api/3/action/datastore_search?{params}", headers=GOV_HEADERS)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        all_records = data.get("result", {}).get("records", [])
    except Exception as e:
        return jsonify({"error": str(e), "records": []}), 200

    # Keep most recent record per FUND_NAME
    latest = {}
    for r in all_records:
        name = r.get("FUND_NAME") or ""
        period = r.get("REPORT_PERIOD") or 0
        if name not in latest or period > (latest[name].get("REPORT_PERIOD") or 0):
            latest[name] = r

    results = list(latest.values())

    # Step 3: track is required — return nothing if not provided
    if not track:
        return jsonify({"records": [], "count": 0, "corp": corp_name, "note": "no_track"})

    track_lower = track.strip().lower()
    track_words = [w for w in track_lower.split() if len(w) > 1]

    def score(fund_name):
        """Return match score: 3=exact, 2=substring, 1=all-words, 0=no match"""
        fn = (fund_name or "").strip().lower()
        if fn == track_lower:
            return 3
        if track_lower in fn or fn in track_lower:
            return 2
        if track_words and all(w in fn for w in track_words):
            return 1
        return 0

    scored = [(score(r.get("FUND_NAME")), r) for r in results]
    best_score = max((s for s, _ in scored), default=0)

    if best_score == 0:
        results = []
    else:
        results = [r for s, r in scored if s == best_score]

    results.sort(key=lambda r: r.get("FUND_NAME") or "")
    return jsonify({"records": results, "count": len(results), "corp": corp_name})

if __name__ == "__main__":
    init_db()
    print("\n✅  דשבורד פיננסי רץ בכתובת: http://localhost:5001\n")
    app.run(debug=False, port=5001)
