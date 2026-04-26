import json, urllib.request, urllib.parse, os, logging
from flask import Flask, request, jsonify, render_template, g
from datetime import datetime
import pg8000.native

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

OWNERS = ["אני", "בן/בת זוג", "ילד 1", "ילד 2", "ילד 3"]
RESOURCE_ID = "a30dcbea-a1d2-482c-ae29-8f781f5025fb"
GOV_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
DATABASE_URL = os.environ.get("DATABASE_URL", "")

_db_initialized = False

def parse_db_url(url):
    """Parse postgresql://user:pass@host:port/dbname"""
    import urllib.parse as up
    r = up.urlparse(url)
    return {
        "host": r.hostname,
        "port": r.port or 5432,
        "user": r.username,
        "password": r.password,
        "database": r.path.lstrip("/"),
        "ssl_context": True
    }

def get_conn():
    p = parse_db_url(DATABASE_URL)
    return pg8000.native.Connection(**p)

def init_db():
    app.logger.info(f"Connecting to DB...")
    conn = get_conn()
    conn.run("""CREATE TABLE IF NOT EXISTS policies (
        id SERIAL PRIMARY KEY, owner TEXT NOT NULL,
        type TEXT NOT NULL, institute TEXT, policy_number TEXT,
        start_month TEXT, fee REAL, amount REAL DEFAULT 0,
        update_month TEXT, track TEXT, fund_id TEXT,
        created_at TEXT DEFAULT now()::text,
        updated_at TEXT DEFAULT now()::text)""")
    # migrate existing tables
    try:
        conn.run("ALTER TABLE policies ADD COLUMN fund_id TEXT")
    except Exception:
        pass
    conn.run("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    conn.run("INSERT INTO settings (key,value) VALUES (:k,:v) ON CONFLICT (key) DO NOTHING",
             k='owners', v=json.dumps(OWNERS))
    conn.close()
    app.logger.info("DB initialized OK")

@app.before_request
def ensure_db():
    global _db_initialized
    if not _db_initialized:
        try:
            init_db()
            _db_initialized = True
        except Exception as e:
            app.logger.error(f"DB init failed: {e}")

def get_db():
    if "db" not in g:
        g.db = get_conn()
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop("db", None)
    if db:
        try: db.close()
        except: pass

@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/settings")
def get_settings():
    db = get_db()
    rows = db.run("SELECT value FROM settings WHERE key='owners'")
    return jsonify({"owners": json.loads(rows[0][0]) if rows else OWNERS})

@app.route("/api/settings/owners", methods=["PUT"])
def update_owners():
    db = get_db()
    db.run("INSERT INTO settings (key,value) VALUES ('owners',:v) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
           v=json.dumps(request.json.get("owners", OWNERS)))
    return jsonify({"ok": True})

@app.route("/api/policies")
def get_policies():
    db = get_db()
    rows = db.run("SELECT id,owner,type,institute,policy_number,start_month,fee,amount,update_month,track,fund_id,created_at,updated_at FROM policies ORDER BY owner, type")
    cols = ["id","owner","type","institute","policy_number","start_month","fee","amount","update_month","track","fund_id","created_at","updated_at"]
    return jsonify([dict(zip(cols,r)) for r in rows])

@app.route("/api/policies", methods=["POST"])
def add_policy():
    d = request.json; db = get_db()
    rows = db.run("""INSERT INTO policies (owner,type,institute,policy_number,start_month,fee,amount,update_month,track,fund_id,updated_at)
        VALUES (:owner,:type,:institute,:policy_number,:start_month,:fee,:amount,:update_month,:track,:fund_id,:updated_at)
        RETURNING id,owner,type,institute,policy_number,start_month,fee,amount,update_month,track,fund_id,created_at,updated_at""",
        owner=d.get("owner"), type=d.get("type"), institute=d.get("institute"),
        policy_number=d.get("policy_number"), start_month=d.get("start_month"),
        fee=d.get("fee"), amount=d.get("amount"), update_month=d.get("update_month"),
        track=d.get("track"), fund_id=d.get("fund_id"), updated_at=datetime.now().isoformat())
    cols = ["id","owner","type","institute","policy_number","start_month","fee","amount","update_month","track","fund_id","created_at","updated_at"]
    return jsonify(dict(zip(cols, rows[0]))), 201

@app.route("/api/policies/<int:pid>", methods=["PUT"])
def update_policy(pid):
    d = request.json; db = get_db()
    rows = db.run("""UPDATE policies SET owner=:owner,type=:type,institute=:institute,policy_number=:policy_number,
        start_month=:start_month,fee=:fee,amount=:amount,update_month=:update_month,track=:track,fund_id=:fund_id,updated_at=:updated_at
        WHERE id=:id RETURNING id,owner,type,institute,policy_number,start_month,fee,amount,update_month,track,fund_id,created_at,updated_at""",
        owner=d.get("owner"), type=d.get("type"), institute=d.get("institute"),
        policy_number=d.get("policy_number"), start_month=d.get("start_month"),
        fee=d.get("fee"), amount=d.get("amount"), update_month=d.get("update_month"),
        track=d.get("track"), fund_id=d.get("fund_id"), updated_at=datetime.now().isoformat(), id=pid)
    cols = ["id","owner","type","institute","policy_number","start_month","fee","amount","update_month","track","fund_id","created_at","updated_at"]
    return jsonify(dict(zip(cols, rows[0])))
    cols = ["id","owner","type","institute","policy_number","start_month","fee","amount","update_month","track","created_at","updated_at"]
    return jsonify(dict(zip(cols, rows[0])))

@app.route("/api/policies/<int:pid>", methods=["DELETE"])
def delete_policy(pid):
    db = get_db()
    db.run("DELETE FROM policies WHERE id=:id", id=pid)
    return jsonify({"ok": True})

@app.route("/api/yields")
def get_yields():
    q = request.args.get("q","").strip()
    track = request.args.get("track","").strip()
    fund_id = request.args.get("fund_id","").strip()
    if not q: return jsonify({"error":"missing q"}), 400

    # If fund_id provided, use exact FUND_ID filter — most accurate
    if fund_id:
        try:
            params = urllib.parse.urlencode({
                "resource_id": RESOURCE_ID,
                "filters": json.dumps({"FUND_ID": int(fund_id)}),
                "limit": 5,
                "sort": "REPORT_PERIOD desc"
            })
            req = urllib.request.Request(f"https://data.gov.il/api/3/action/datastore_search?{params}", headers=GOV_HEADERS)
            with urllib.request.urlopen(req, timeout=12) as resp: data = json.loads(resp.read())
            records = data.get("result",{}).get("records",[])
            if records:
                # keep only most recent
                latest = max(records, key=lambda r: r.get("REPORT_PERIOD") or 0)
                return jsonify({"records":[latest], "count":1, "corp": latest.get("MANAGING_CORPORATION","")})
        except Exception as e:
            pass  # fall through to name-based search

    try:
        params = urllib.parse.urlencode({"resource_id":RESOURCE_ID,"q":q,"limit":10})
        req = urllib.request.Request(f"https://data.gov.il/api/3/action/datastore_search?{params}",headers=GOV_HEADERS)
        with urllib.request.urlopen(req,timeout=12) as resp: data=json.loads(resp.read())
        records = data.get("result",{}).get("records",[])
        if not records: return jsonify({"records":[],"count":0,"corp":None})
        q_lower = q.lower(); corp_name=""
        for r in records:
            c = r.get("MANAGING_CORPORATION") or ""
            if q_lower in c.lower() or c.lower() in q_lower: corp_name=c; break
        if not corp_name: corp_name=records[0].get("MANAGING_CORPORATION","")
    except Exception as e: return jsonify({"error":str(e),"records":[]}), 200

    try:
        params = urllib.parse.urlencode({"resource_id":RESOURCE_ID,"filters":json.dumps({"MANAGING_CORPORATION":corp_name}),"limit":500,"sort":"REPORT_PERIOD desc"})
        req = urllib.request.Request(f"https://data.gov.il/api/3/action/datastore_search?{params}",headers=GOV_HEADERS)
        with urllib.request.urlopen(req,timeout=15) as resp: data=json.loads(resp.read())
        all_records = data.get("result",{}).get("records",[])
    except Exception as e: return jsonify({"error":str(e),"records":[]}), 200

    latest={}
    for r in all_records:
        name=r.get("FUND_NAME") or ""; period=r.get("REPORT_PERIOD") or 0
        if name not in latest or period>(latest[name].get("REPORT_PERIOD") or 0): latest[name]=r
    results=list(latest.values())

    if not track: return jsonify({"records":[],"count":0,"corp":corp_name,"note":"no_track"})

    track_lower=track.strip().lower()
    track_words=[w for w in track_lower.split() if len(w)>1]
    def score(fn):
        fn=(fn or "").strip().lower()
        if fn==track_lower: return 3
        if track_lower in fn or fn in track_lower: return 2
        if track_words and all(w in fn for w in track_words): return 1
        return 0
    scored=[(score(r.get("FUND_NAME")),r) for r in results]
    best=max((s for s,_ in scored),default=0)
    results=[r for s,r in scored if s==best] if best>0 else []
    results.sort(key=lambda r:r.get("FUND_NAME") or "")
    return jsonify({"records":results,"count":len(results),"corp":corp_name})

if __name__ == "__main__":
    init_db()
    app.run(debug=False, port=int(os.environ.get("PORT", 5001)))
