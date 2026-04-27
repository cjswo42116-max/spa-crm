#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""다희 스파 비서 v3 — 원페이지 AI 비서"""

import os, io, sqlite3, requests, pandas as pd
import anthropic as _anthropic
from datetime import datetime, date
from flask import Flask, render_template, request, jsonify, g
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(__file__), "assistant.db")


# ─── DB ───────────────────────────────────────────────────────────────────────

def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_db():
    db = getattr(g, "_db", None)
    if db is None:
        db = g._db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_db(e):
    db = getattr(g, "_db", None)
    if db: db.close()

def init_db():
    conn = db_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS customers (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL UNIQUE,
            last_visit      TEXT,
            balance         INTEGER DEFAULT 0,
            expiry          TEXT DEFAULT '무제한',
            customer_type   TEXT DEFAULT '정액권자',
            phone           TEXT DEFAULT '',
            notes           TEXT DEFAULT '',
            follow_up_date  TEXT,
            avg_visit_cycle INTEGER DEFAULT 0,
            created_at      TEXT DEFAULT (datetime('now','localtime')),
            updated_at      TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS deductions (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id    INTEGER,
            customer_name  TEXT,
            amount         INTEGER,
            balance_before INTEGER DEFAULT 0,
            balance_after  INTEGER DEFAULT 0,
            sms_sent       INTEGER DEFAULT 0,
            sms_sent_at    TEXT,
            created_at     TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS sales_history (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_name TEXT,
            visit_date    TEXT,
            amount        INTEGER DEFAULT 0,
            menu          TEXT DEFAULT '',
            UNIQUE(customer_name, visit_date, amount)
        );
        CREATE TABLE IF NOT EXISTS alert_log (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type    TEXT,
            customer_name TEXT,
            message       TEXT,
            sent_at       TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS wiki (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            title      TEXT NOT NULL,
            content    TEXT NOT NULL DEFAULT '',
            category   TEXT DEFAULT '일반',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );
    """)
    conn.commit()
    conn.close()


# ─── Slack ────────────────────────────────────────────────────────────────────

def _webhook():
    conn = db_conn()
    r = conn.execute("SELECT value FROM settings WHERE key='slack_webhook'").fetchone()
    conn.close()
    return r["value"] if r else None

def send_slack(msg: str, urgent: bool = False) -> bool:
    url = _webhook()
    if not url: return False
    icon = ":rotating_light:" if urgent else ":bell:"
    try:
        r = requests.post(url, json={"text": f"{icon} {msg}"}, timeout=5)
        return r.status_code == 200
    except Exception:
        return False


# ─── Scheduler Jobs ───────────────────────────────────────────────────────────

def job_morning():
    """09:00 매일 — 미방문·만료·방문주기·후속관리 알림"""
    conn = db_conn()
    today = date.today()
    alerts = []

    for c in conn.execute("SELECT * FROM customers").fetchall():
        name = c["name"]

        # 15일 미방문
        if c["last_visit"]:
            try:
                last = datetime.strptime(c["last_visit"], "%Y-%m-%d").date()
                days = (today - last).days
                if days >= 15:
                    atype = "dormant"
                    if not _alert_sent_today(conn, name, atype, today):
                        msg = f"*{name}* {days}일 미방문 | 잔여 {(c['balance'] or 0):,}원"
                        if send_slack(msg, urgent=True):
                            _log_alert(conn, atype, name, msg)
                            alerts.append(msg)

                # 방문주기 초과
                cyc = c["avg_visit_cycle"] or 0
                if cyc > 0 and days > cyc:
                    atype = "overdue_cycle"
                    if not _alert_sent_today(conn, name, atype, today):
                        msg = f"*{name}* 평균 방문주기({cyc}일) 초과 ({days}일 경과)"
                        if send_slack(msg, urgent=True):
                            _log_alert(conn, atype, name, msg)
            except ValueError:
                pass

        # 만료 임박
        exp = (c["expiry"] or "").strip()
        if exp and exp not in ("무제한", "", "None"):
            try:
                dl = (datetime.strptime(exp, "%Y-%m-%d").date() - today).days
                for th, label in [(90,"3개월"),(60,"2개월"),(30,"1개월"),(15,"15일")]:
                    if 0 <= dl - th <= 1:
                        atype = f"expiry_{th}d"
                        if not _alert_sent_today(conn, name, atype, today):
                            msg = f"*{name}* 회원권 {label} 후 만료 (D-{dl})"
                            if send_slack(msg):
                                _log_alert(conn, atype, name, msg)
            except ValueError:
                pass

        # 후속관리 날짜
        fu = c["follow_up_date"]
        if fu:
            try:
                fu_date = datetime.strptime(fu, "%Y-%m-%d").date()
                if fu_date == today:
                    atype = "followup"
                    if not _alert_sent_today(conn, name, atype, today):
                        notes_preview = (c["notes"] or "")[:50]
                        msg = f"*[후속관리]* {name} 님 오늘 연락 예정\n메모: {notes_preview}"
                        if send_slack(msg, urgent=True):
                            _log_alert(conn, atype, name, msg)
            except ValueError:
                pass

    conn.commit()
    conn.close()
    return alerts

def job_weekly_upload_reminder():
    """월요일 09:00 — 매출 파일 업로드 리마인더"""
    if date.today().weekday() == 0:  # Monday
        send_slack("*[주간 리마인더]* 이번 주 매출 파일을 앱에 업로드해주세요! :file_folder:")

def job_evening_sms():
    """18:00 매일 — 문자 미발송 알림"""
    conn = db_conn()
    today = date.today().isoformat()
    rows = conn.execute(
        "SELECT customer_name, amount FROM deductions WHERE sms_sent=0 AND date(created_at)=?",
        (today,)
    ).fetchall()
    if rows:
        lines = "\n".join(f"• {r['customer_name']} ({r['amount']:,}원)" for r in rows)
        send_slack(f"*퇴근 전 확인!* 오늘 차감 후 문자 미발송 {len(rows)}건:\n{lines}", urgent=True)
    conn.close()

def _alert_sent_today(conn, name, atype, today):
    return conn.execute(
        "SELECT 1 FROM alert_log WHERE customer_name=? AND alert_type=? AND date(sent_at)=?",
        (name, atype, today.isoformat())
    ).fetchone()

def _log_alert(conn, atype, name, msg):
    conn.execute(
        "INSERT INTO alert_log (alert_type, customer_name, message) VALUES (?,?,?)",
        (atype, name, msg)
    )


# ─── CSV 파싱: 텍스트 기반 헤더 탐지 ─────────────────────────────────────────

def _decode(file_bytes: bytes) -> tuple[str, str]:
    """인코딩 자동 감지, (text, encoding) 반환"""
    for enc in ("utf-8-sig", "cp949", "euc-kr", "utf-8"):
        try:
            return file_bytes.decode(enc), enc
        except UnicodeDecodeError:
            continue
    raise ValueError("인코딩 인식 실패")

def parse_members_csv(file_bytes: bytes) -> pd.DataFrame:
    """
    핸드SOS 정액권보유고객 CSV 파싱
    구조: 상단 2줄 요약 → 3줄째 헤더행 → 4줄째 구분행(drop) → 5줄~부터 실제 데이터
    13개 컬럼: 성명|가족번호|담당자|최종구매일|최종방문일|최근메뉴|결제액|충전액|사용액|잔여액|만료액|사용기간|비고
    """
    COLS = ["성명","가족번호","담당자","최종구매일","최종방문일",
            "최근메뉴","결제액","충전액","사용액","잔여액","만료액","사용기간","비고"]

    text, enc = _decode(file_bytes)

    # ── 방법 1: skiprows=2 고정 (핸드SOS 표준 구조) ──────────────────────────
    try:
        df = pd.read_csv(io.StringIO(text), skiprows=2, header=0, dtype=str)
        # 첫 번째 데이터행이 구분행/빈행이면 제거
        if len(df) > 0:
            first = df.iloc[0]
            if first.astype(str).str.strip().eq("").all() or \
               "성명" in str(first.values[0]) or \
               first.isnull().all():
                df = df.drop(df.index[0]).reset_index(drop=True)
        # 13컬럼이면 정확한 이름 지정
        if len(df.columns) >= 13:
            df.columns = COLS[:len(df.columns)]
        else:
            df.columns = [str(c).strip() for c in df.columns]
        df = _clean_members(df)
        if len(df) > 0:
            return df
    except Exception:
        pass

    # ── 방법 2: '성명' 행 탐색 (fallback) ────────────────────────────────────
    lines = [l for l in text.splitlines() if l.strip()]
    header_idx = None
    for i, line in enumerate(lines[:20]):
        if "성명" in line:
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("'성명' 컬럼을 찾을 수 없습니다. 파일 형식을 확인해주세요.")

    csv_text = "\n".join(lines[header_idx:])
    df = pd.read_csv(io.StringIO(csv_text), dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    # 위치 기반 컬럼 매핑
    cols = list(df.columns)
    rename = {}
    if "최종방문일" not in cols and len(cols) > 4:
        rename[cols[4]] = "최종방문일"
    for unnamed, target, pos in [("Unnamed: 9","잔여액",9), ("Unnamed: 11","사용기간",11)]:
        if target not in cols:
            if unnamed in cols: rename[unnamed] = target
            elif len(cols) > pos: rename[cols[pos]] = target
    if rename:
        df = df.rename(columns=rename)

    return _clean_members(df)

def _clean_members(df: pd.DataFrame) -> pd.DataFrame:
    """성명 기준 데이터 정제"""
    if "성명" not in df.columns:
        return df
    df = df[df["성명"].notna()]
    df = df[df["성명"].astype(str).str.strip().ne("")]
    df = df[~df["성명"].astype(str).str.contains("성명|합계|소계|nan|성 명", na=False)]
    df = df.reset_index(drop=True)
    return df

def parse_sales_csv(file_bytes: bytes) -> pd.DataFrame:
    """
    핸드SOS 매출상세조회 CSV
    '고객명'·'날짜'·'결제액' 컬럼 추출
    """
    text, enc = _decode(file_bytes)
    lines = [l for l in text.splitlines() if l.strip()]

    header_idx = None
    for i, line in enumerate(lines[:25]):
        # 고객명 또는 날짜가 있는 행을 헤더로
        if "고객명" in line or "날짜" in line or "거래일" in line:
            header_idx = i
            break
    if header_idx is None:
        header_idx = min(14, len(lines)-1)  # fallback

    csv_text = "\n".join(lines[header_idx:])
    df = pd.read_csv(io.StringIO(csv_text), dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    for src, dst in [
        (["고객명","성명","이름"], "고객명"),
        (["날짜","거래일","결제일","방문일"], "날짜"),
        (["결제액","결제금액","금액","매출액"], "금액"),
    ]:
        for c in src:
            if c in df.columns:
                df = df.rename(columns={c: dst})
                break

    return df


# ─── 방문주기 계산 ─────────────────────────────────────────────────────────────

def calculate_visit_cycles(df: pd.DataFrame) -> dict[str, int]:
    """sales_history 데이터프레임 → {이름: 평균방문주기(일)}"""
    if "고객명" not in df.columns or "날짜" not in df.columns:
        return {}
    df = df.copy()
    df["날짜_p"] = pd.to_datetime(df["날짜"], errors="coerce")
    df = df.dropna(subset=["날짜_p","고객명"])
    result = {}
    for name, grp in df.groupby("고객명"):
        dates = sorted(grp["날짜_p"].tolist())
        if len(dates) < 2:
            continue
        diffs = [(dates[i+1]-dates[i]).days for i in range(len(dates)-1)]
        result[str(name).strip()] = round(sum(diffs)/len(diffs))
    return result


# ─── 날짜·숫자 정규화 ─────────────────────────────────────────────────────────

def norm_date(raw) -> str | None:
    s = str(raw).strip()
    if s in ("nan","","None","0","NaT"): return None
    for fmt in ("%Y-%m-%d","%Y/%m/%d","%Y.%m.%d","%m/%d/%Y","%Y%m%d"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError: pass
    return None

def norm_int(raw, default=0) -> int:
    s = str(raw).replace(",","").replace("원","").strip()
    try: return int(float(s))
    except: return default


# ─── 메시지 생성 ──────────────────────────────────────────────────────────────

def msg_deduct(name, amount, remaining):
    return (f"안녕하세요 {name}님 오늘도 이용해주셔서 감사합니다 :two_hearts:\n"
            f"{amount:,}원 차감되었고 잔여 {remaining:,}원 남아있어요.\n"
            f"다음에 또 편하게 오세요 :herb:")

def msg_jachsal(name, days, balance):
    month = date.today().month
    phrases = {(3,4,5):"따뜻한 봄바람이 부는 요즘",(6,7,8):"뜨거운 여름이지만",
               (9,10,11):"선선한 가을이 온 요즘",(12,1,2):"쌀쌀한 날씨가 계속되는 요즘"}
    phrase = next((v for k,v in phrases.items() if month in k), "요즘")
    bal_str = f" 잔여 {balance:,}원도 남아있으니" if balance > 0 else ""
    return (f"안녕하세요 {name}님 {phrase} 잘 지내시죠? :two_hearts:\n"
            f"마지막 방문 후 {days}일이 지났는데 보고싶어서 연락드렸어요{bal_str}\n"
            f"시간 되실 때 편하게 오셔서 힐링하고 가세요 :herb:")


# ─── Routes: 기본 ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/settings", methods=["GET","POST"])
def api_settings():
    db = get_db()
    if request.method == "POST":
        for k,v in (request.json or {}).items():
            db.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (k,v))
        db.commit()
        return jsonify({"ok": True})
    rows = db.execute("SELECT key,value FROM settings").fetchall()
    return jsonify({r["key"]:r["value"] for r in rows})

@app.route("/api/slack/test", methods=["POST"])
def api_slack_test():
    ok = send_slack("다희 스파 비서 v3 연결 성공! :white_check_mark:")
    return jsonify({"ok": ok, "error": None if ok else "Webhook URL 확인"})

@app.route("/api/alerts/check", methods=["POST"])
def api_alerts_check():
    n = len(job_morning())
    return jsonify({"ok": True, "sent": n})

@app.route("/api/alerts/log")
def api_alerts_log():
    db = get_db()
    rows = db.execute("SELECT * FROM alert_log ORDER BY sent_at DESC LIMIT 30").fetchall()
    return jsonify([dict(r) for r in rows])


# ─── Routes: 고객 CRUD ────────────────────────────────────────────────────────

@app.route("/api/customers", methods=["GET"])
def api_customers_get():
    db = get_db()
    today = date.today()
    customers = []
    for r in db.execute("SELECT * FROM customers ORDER BY name").fetchall():
        c = dict(r)
        if c["last_visit"]:
            try:
                last = datetime.strptime(c["last_visit"], "%Y-%m-%d").date()
                c["days_since_visit"] = (today - last).days
            except: c["days_since_visit"] = None
        else: c["days_since_visit"] = None

        exp = (c["expiry"] or "").strip()
        if exp and exp not in ("무제한","","None"):
            try: c["days_to_expiry"] = (datetime.strptime(exp, "%Y-%m-%d").date() - today).days
            except: c["days_to_expiry"] = None
        else: c["days_to_expiry"] = None

        # 방문주기 초과 여부
        cyc = c["avg_visit_cycle"] or 0
        dsv = c["days_since_visit"] or 0
        c["overdue_cycle"] = (cyc > 0 and dsv > cyc)
        c["jachsal_msg"] = msg_jachsal(c["name"], dsv, c["balance"] or 0) if dsv >= 15 else ""
        customers.append(c)
    return jsonify(customers)

@app.route("/api/customers", methods=["POST"])
def api_customers_post():
    db = get_db()
    d = request.json
    db.execute(
        "INSERT OR REPLACE INTO customers (name,last_visit,balance,expiry,customer_type,phone,notes,follow_up_date) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (d["name"],d.get("last_visit"),d.get("balance",0),d.get("expiry","무제한"),
         d.get("customer_type","정액권자"),d.get("phone",""),d.get("notes",""),d.get("follow_up_date"))
    )
    db.commit()
    return jsonify({"ok": True})

@app.route("/api/customers/<int:cid>", methods=["PUT"])
def api_customers_put(cid):
    db = get_db()
    d = request.json
    db.execute(
        "UPDATE customers SET name=?,last_visit=?,balance=?,expiry=?,customer_type=?,"
        "phone=?,notes=?,follow_up_date=?,updated_at=datetime('now','localtime') WHERE id=?",
        (d["name"],d.get("last_visit"),d.get("balance",0),d.get("expiry","무제한"),
         d.get("customer_type","정액권자"),d.get("phone",""),d.get("notes",""),
         d.get("follow_up_date") or None, cid)
    )
    db.commit()
    return jsonify({"ok": True})

@app.route("/api/customers/<int:cid>", methods=["DELETE"])
def api_customers_delete(cid):
    db = get_db()
    db.execute("DELETE FROM customers WHERE id=?", (cid,))
    db.commit()
    return jsonify({"ok": True})

@app.route("/api/customers/<int:cid>/visit", methods=["POST"])
def api_visit(cid):
    db = get_db()
    today = date.today().isoformat()
    db.execute("UPDATE customers SET last_visit=?,updated_at=datetime('now','localtime') WHERE id=?", (today,cid))
    db.commit()
    return jsonify({"ok": True, "last_visit": today})

@app.route("/api/customers/<int:cid>/notes", methods=["POST"])
def api_notes(cid):
    db = get_db()
    d = request.json
    db.execute(
        "UPDATE customers SET notes=?,follow_up_date=?,updated_at=datetime('now','localtime') WHERE id=?",
        (d.get("notes",""), d.get("follow_up_date") or None, cid)
    )
    db.commit()
    return jsonify({"ok": True})


# ─── Routes: 차감 ─────────────────────────────────────────────────────────────

@app.route("/api/customers/<int:cid>/deduct", methods=["POST"])
def api_deduct(cid):
    db = get_db()
    amount = int((request.json or {}).get("amount", 0))
    if amount <= 0:
        return jsonify({"error": "금액을 입력해주세요"}), 400
    c = db.execute("SELECT * FROM customers WHERE id=?", (cid,)).fetchone()
    if not c:
        return jsonify({"error": "고객 없음"}), 404
    before = c["balance"] or 0
    after  = max(0, before - amount)
    today  = date.today().isoformat()
    db.execute("UPDATE customers SET balance=?,last_visit=?,updated_at=datetime('now','localtime') WHERE id=?",
               (after, today, cid))
    did = db.execute(
        "INSERT INTO deductions (customer_id,customer_name,amount,balance_before,balance_after) VALUES (?,?,?,?,?)",
        (cid, c["name"], amount, before, after)
    ).lastrowid
    db.commit()
    return jsonify({"ok":True,"deduction_id":did,"balance_before":before,
                    "balance_after":after,"sms_message":msg_deduct(c["name"],amount,after),
                    "customer_name":c["name"]})

@app.route("/api/deductions/<int:did>/sms_sent", methods=["POST"])
def api_deduction_sms(did):
    db = get_db()
    db.execute("UPDATE deductions SET sms_sent=1,sms_sent_at=datetime('now','localtime') WHERE id=?", (did,))
    db.commit()
    return jsonify({"ok": True})

@app.route("/api/deductions/pending")
def api_deductions_pending():
    db = get_db()
    rows = db.execute(
        "SELECT * FROM deductions WHERE sms_sent=0 AND date(created_at)=? ORDER BY created_at DESC",
        (date.today().isoformat(),)
    ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/sms/sent", methods=["POST"])
def api_sms_sent():
    return jsonify({"ok": True})


# ─── Routes: CSV 업로드 ───────────────────────────────────────────────────────

@app.route("/api/upload/members", methods=["POST"])
def api_upload_members():
    f = request.files.get("file")
    if not f: return jsonify({"error":"파일 없음"}), 400
    try: df = parse_members_csv(f.read())
    except ValueError as e: return jsonify({"error": str(e)}), 400

    db = get_db()
    upserted = 0
    for _, row in df.iterrows():
        name = str(row.get("성명","")).strip()
        if not name or name == "nan": continue
        last_visit = norm_date(row.get("최종방문일"))
        balance    = norm_int(row.get("잔여액",0))
        exp_raw    = str(row.get("사용기간","무제한")).strip()
        expiry     = "무제한" if exp_raw in ("nan","","0","무제한") else (norm_date(exp_raw) or exp_raw)

        ex = db.execute("SELECT id FROM customers WHERE name=?", (name,)).fetchone()
        if ex:
            db.execute(
                "UPDATE customers SET last_visit=?,balance=?,expiry=?,"
                "updated_at=datetime('now','localtime') WHERE name=?",
                (last_visit, balance, expiry, name)
            )
        else:
            db.execute(
                "INSERT INTO customers (name,last_visit,balance,expiry) VALUES (?,?,?,?)",
                (name, last_visit, balance, expiry)
            )
        upserted += 1

    db.commit()
    cols = list(df.columns)
    sample = df[[c for c in ["성명","최종방문일","잔여액","사용기간"] if c in df.columns]].head(6)
    return jsonify({"ok":True,"upserted":upserted,"columns":cols[:15],
                    "sample":sample.to_dict("records")})

@app.route("/api/upload/sales", methods=["POST"])
def api_upload_sales():
    """2년치 매출 CSV → sales_history + 방문주기 계산"""
    f = request.files.get("file")
    if not f: return jsonify({"error":"파일 없음"}), 400
    try: df = parse_sales_csv(f.read())
    except Exception as e: return jsonify({"error":str(e)}), 400

    db = get_db()
    inserted = 0

    if "고객명" in df.columns and "날짜" in df.columns:
        for _, row in df.iterrows():
            name = str(row.get("고객명","")).strip()
            d    = norm_date(row.get("날짜"))
            amt  = norm_int(row.get("금액",0))
            if not name or name == "nan" or not d: continue
            try:
                db.execute(
                    "INSERT OR IGNORE INTO sales_history (customer_name,visit_date,amount) VALUES (?,?,?)",
                    (name, d, amt)
                )
                inserted += 1
            except Exception:
                pass
        db.commit()

        # 방문주기 재계산
        cycles = calculate_visit_cycles(df)
        for name, cyc in cycles.items():
            db.execute("UPDATE customers SET avg_visit_cycle=? WHERE name=?", (cyc, name))
        db.commit()

    return jsonify({"ok":True,"rows_inserted":inserted,
                    "cycles_updated":len(cycles) if "cycles" in dir() else 0})


# ─── Routes: 대시보드 & 매출 요약 ─────────────────────────────────────────────

@app.route("/api/dashboard")
def api_dashboard():
    db = get_db()
    today = date.today()
    customers = [dict(r) for r in db.execute("SELECT * FROM customers").fetchall()]
    dormant_15, overdue_cycle, expiry_soon = [], [], []

    for c in customers:
        if c["last_visit"]:
            try:
                last = datetime.strptime(c["last_visit"], "%Y-%m-%d").date()
                c["days_since_visit"] = (today - last).days
                if c["days_since_visit"] >= 15:
                    c["jachsal_msg"] = msg_jachsal(c["name"], c["days_since_visit"], c["balance"] or 0)
                    dormant_15.append(c)
                cyc = c["avg_visit_cycle"] or 0
                if cyc > 0 and c["days_since_visit"] > cyc:
                    overdue_cycle.append(c)
            except ValueError:
                c["days_since_visit"] = None
        else:
            c["days_since_visit"] = None

        exp = (c["expiry"] or "").strip()
        if exp and exp not in ("무제한","","None"):
            try:
                dl = (datetime.strptime(exp, "%Y-%m-%d").date() - today).days
                c["days_to_expiry"] = dl
                if 0 <= dl <= 90: expiry_soon.append(c)
            except ValueError:
                pass

    dormant_15.sort(key=lambda x: x["days_since_visit"] or 0, reverse=True)
    overdue_cycle.sort(key=lambda x: (x["days_since_visit"] or 0) - (x["avg_visit_cycle"] or 0), reverse=True)

    pending_sms = db.execute(
        "SELECT COUNT(*) as n FROM deductions WHERE sms_sent=0 AND date(created_at)=?",
        (today.isoformat(),)
    ).fetchone()["n"]

    recent_alerts = [dict(r) for r in db.execute(
        "SELECT * FROM alert_log ORDER BY sent_at DESC LIMIT 20"
    ).fetchall()]

    return jsonify({
        "total_customers":   len(customers),
        "dormant_count":     len(dormant_15),
        "overdue_count":     len(overdue_cycle),
        "expiry_soon_count": len(expiry_soon),
        "total_balance":     sum(c["balance"] or 0 for c in customers),
        "pending_sms":       pending_sms,
        "dormant_15":        dormant_15,
        "overdue_cycle":     overdue_cycle,
        "expiry_soon":       expiry_soon,
        "recent_alerts":     recent_alerts,
    })

@app.route("/api/sales/summary")
def api_sales_summary():
    """월별 방문 건수 및 매출 (차트용)"""
    db = get_db()
    rows = db.execute("""
        SELECT substr(visit_date,1,7) as month,
               COUNT(*) as visits,
               SUM(amount) as revenue
        FROM sales_history
        WHERE visit_date IS NOT NULL
        GROUP BY month
        ORDER BY month DESC
        LIMIT 24
    """).fetchall()
    data = [dict(r) for r in rows]
    data.reverse()
    return jsonify(data)


# ─── Routes: 위키 ─────────────────────────────────────────────────────────────

@app.route("/api/wiki", methods=["GET"])
def api_wiki_get():
    db = get_db()
    rows = db.execute("SELECT * FROM wiki ORDER BY category, title").fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/api/wiki", methods=["POST"])
def api_wiki_post():
    db = get_db()
    d = request.json or {}
    db.execute(
        "INSERT INTO wiki (title, content, category) VALUES (?,?,?)",
        (d.get("title","").strip(), d.get("content",""), d.get("category","일반"))
    )
    db.commit()
    return jsonify({"ok": True})

@app.route("/api/wiki/<int:wid>", methods=["PUT"])
def api_wiki_put(wid):
    db = get_db()
    d = request.json or {}
    db.execute(
        "UPDATE wiki SET title=?, content=?, category=?, updated_at=datetime('now','localtime') WHERE id=?",
        (d.get("title","").strip(), d.get("content",""), d.get("category","일반"), wid)
    )
    db.commit()
    return jsonify({"ok": True})

@app.route("/api/wiki/<int:wid>", methods=["DELETE"])
def api_wiki_delete(wid):
    db = get_db()
    db.execute("DELETE FROM wiki WHERE id=?", (wid,))
    db.commit()
    return jsonify({"ok": True})


# ─── Routes: LLM 채팅 ─────────────────────────────────────────────────────────

@app.route("/api/chat", methods=["POST"])
def api_chat():
    d = request.json or {}
    message = d.get("message", "").strip()
    if not message:
        return jsonify({"error": "메시지를 입력해주세요"}), 400

    db = get_db()
    key_row = db.execute("SELECT value FROM settings WHERE key='claude_api_key'").fetchone()
    api_key = (key_row["value"] if key_row else None) or os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return jsonify({"error": "Claude API 키가 설정되지 않았습니다. 설정에서 API 키를 입력해주세요."}), 400

    wiki_rows = db.execute("SELECT title, category, content FROM wiki ORDER BY category, title").fetchall()
    wiki_text = ""
    if wiki_rows:
        wiki_text = "\n\n## 위키 (운영 지식베이스)\n"
        for w in wiki_rows:
            wiki_text += f"\n### [{w['category']}] {w['title']}\n{w['content']}\n"

    today = date.today()
    customers = db.execute("SELECT * FROM customers").fetchall()
    total_balance = sum(c["balance"] or 0 for c in customers)
    dormant, expiring = [], []
    for c in customers:
        if c["last_visit"]:
            try:
                days = (today - datetime.strptime(c["last_visit"], "%Y-%m-%d").date()).days
                if days >= 15:
                    dormant.append(f"{c['name']}({days}일)")
            except ValueError:
                pass
        exp = (c["expiry"] or "").strip()
        if exp and exp not in ("무제한", "", "None"):
            try:
                dl = (datetime.strptime(exp, "%Y-%m-%d").date() - today).days
                if 0 <= dl <= 30:
                    expiring.append(f"{c['name']}(D-{dl})")
            except ValueError:
                pass

    crm_ctx = (
        f"\n\n## 현재 CRM 현황 (오늘: {today})\n"
        f"- 전체 고객 수: {len(customers)}명\n"
        f"- 총 잔여액: {total_balance:,}원\n"
        f"- 15일 미방문 ({len(dormant)}명): {', '.join(dormant[:10]) or '없음'}\n"
        f"- 30일 내 만료 ({len(expiring)}명): {', '.join(expiring[:10]) or '없음'}\n"
    )

    system_prompt = (
        "당신은 다희 스파의 AI 비서입니다. 스파 운영, 고객 관리, 매출 분석에 대한 질문에 답변합니다. "
        "한국어로 친절하고 간결하게 답변하세요."
        + wiki_text
        + crm_ctx
    )

    try:
        client = _anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=system_prompt,
            messages=[{"role": "user", "content": message}]
        )
        return jsonify({"ok": True, "reply": resp.content[0].text})
    except _anthropic.AuthenticationError:
        return jsonify({"error": "API 키가 올바르지 않습니다"}), 401
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── Scheduler ────────────────────────────────────────────────────────────────

def start_scheduler():
    sched = BackgroundScheduler(timezone="Asia/Seoul")
    sched.add_job(job_morning,          "cron", hour=9,  minute=0, id="morning")
    sched.add_job(job_evening_sms,      "cron", hour=18, minute=0, id="evening_sms")
    sched.add_job(job_weekly_upload_reminder, "cron", day_of_week="mon", hour=9, minute=5, id="weekly")
    sched.start()
    atexit.register(lambda: sched.shutdown(wait=False))


# ─── Bootstrap ────────────────────────────────────────────────────────────────

init_db()
if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    start_scheduler()

if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    print("=" * 52)
    print("  다희 스파 비서 v3 시작!")
    print("  http://localhost:5000")
    print("  폰: http://192.168.35.219:5000")
    print("=" * 52)
    app.run(debug=True, port=5000, host="0.0.0.0")
