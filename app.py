import json, urllib.request, urllib.parse, os, logging
from flask import Flask, request, jsonify, render_template, g
from datetime import datetime
import psycopg2
import psycopg2.extras

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

OWNERS = ["אני", "בן/בת זוג", "ילד 1", "ילד 2", "ילד 3"]
RESOURCE_ID = "a30dcbea-a1d2-482c-ae29-8f781f5025fb"
GOV_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
DATABASE_URL = os.environ.get("DATABASE_URL", "")

_db_initialized = False

def init_db():
    app.logger.info(f"Connecting to DB: {DATABASE_URL[:30]}...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS policies (
            id SERIAL PRIMARY KEY, owner TEXT NOT NULL,
            type TEXT NOT NULL, institute TEXT, policy_number TEXT,
            start_month TEXT, fee REAL, amount REAL DEFAULT 0,
            update_month TEXT, track TEXT,
            created_at TEXT DEFAULT now()::text,
            updated_at TEXT DEFAULT now()::text)""")
    cur.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    cur.execute("INSERT INTO settings (key,value) VALUES ('owners',%s) ON CONFLICT (key) DO NOTHING",
                (json.dumps(OWNERS),))
    conn.commit()
    cur.close()
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
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        g.db = conn
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop("db", None)
    if db: db.close()

def row_to_dict(row, cursor):
    cols = [desc[0] for desc in cursor.description]
    return dict(zip(cols, row))

def rows_to_dicts(rows, cursor):
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, r)) for r in rows]

@app.route("/")
def index(): return render_template("index.html")

@app.route("/api/settings")
def get_settings():
    db = get_db(); cur = db.cursor()
    cur.execute("SELECT value FROM settings WHERE key='owners'")
    row = cur.fetchone(); cur.close()
    return jsonify({"owners": json.loads(row[0]) if row else OWNERS})

@app.route("/api/settings/owners", methods=["PUT"])
def update_owners():
    db = get_db(); cur = db.cursor()
    cur.execute("INSERT INTO settings (key,value) VALUES ('owners',%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
               (json.dumps(request.json.get("owners", OWNERS)),))
    db.commit(); cur.close()
    return jsonify({"ok": True})

@app.route("/api/policies")
def get_policies():
    db = get_db(); cur = db.cursor()
    cur.execute("SELECT * FROM policies ORDER BY owner, type")
    result = rows_to_dicts(cur.fetchall(), cur); cur.close()
    return jsonify(result)

@app.route("/api/policies", methods=["POST"])
def add_policy():
    d = request.json; db = get_db(); cur = db.cursor()
    cur.execute("""INSERT INTO policies (owner,type,institute,policy_number,start_month,fee,amount,update_month,track,updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
        (d.get("owner"),d.get("type"),d.get("institute"),d.get("policy_number"),
         d.get("start_month"),d.get("fee"),d.get("amount"),d.get("update_month"),
         d.get("track"),datetime.now().isoformat()))
    result = row_to_dict(cur.fetchone(), cur); db.commit(); cur.close()
    return jsonify(result), 201

@app.route("/api/policies/<int:pid>", methods=["PUT"])
def update_policy(pid):
    d = request.json; db = get_db(); cur = db.cursor()
    cur.execute("""UPDATE policies SET owner=%s,type=%s,institute=%s,policy_number=%s,
        start_month=%s,fee=%s,amount=%s,update_month=%s,track=%s,updated_at=%s WHERE id=%s RETURNING *""",
        (d.get("owner"),d.get("type"),d.get("institute"),d.get("policy_number"),
         d.get("start_month"),d.get("fee"),d.get("amount"),d.get("update_month"),
         d.get("track"),datetime.now().isoformat(),pid))
    result = row_to_dict(cur.fetchone(), cur); db.commit(); cur.close()
    return jsonify(result)

@app.route("/api/policies/<int:pid>", methods=["DELETE"])
def delete_policy(pid):
    db = get_db(); cur = db.cursor()
    cur.execute("DELETE FROM policies WHERE id=%s", (pid,))
    db.commit(); cur.close()
    return jsonify({"ok": True})

@app.route("/api/yields")
def get_yields():
    q = request.args.get("q","").strip()
    track = request.args.get("track","").strip()
    if not q: return jsonify({"error":"missing q"}), 400

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
