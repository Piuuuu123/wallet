"""
👛 小钱袋 · 情侣版同步后端
===========================
轻量同步服务：FastAPI + SQLite，零外部依赖（标准库 sqlite3）。

设计要点
--------
- 一个「家庭」(household) = 一份共享账本空间，用 6 位邀请码加入。
- 成员身份：本机生成 memberId（uuid），无需密码；用 householdId + memberId 作为凭证。
- 同步模型：last-write-wins，按记录的 updatedAt 时间戳做增量拉取。
  客户端每条本地记录带 uid（全局唯一，含创建设备前缀）+ updatedAt + deleted 标记。
- 冲突：同一 uid 以 updatedAt 大者为准；删除用墓碑（deleted=1）保证对方也能删掉。

API
---
POST   /api/household/create   {nickname}                -> {householdId, inviteCode, memberId, ...}
POST   /api/household/join     {inviteCode, nickname}    -> {householdId, memberId, ...}
GET    /api/household/state    ?hh=&me=&since=           -> 全量/增量状态
POST   /api/sync               {hh, me, since, changes}  -> 服务端合并后返回 since 之后的记录
GET    /api/health
"""

import json
import os
import sqlite3
import time
import uuid
import random
import string
from contextlib import closing
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

DB_PATH = os.environ.get("XMQ_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.db"))
STATIC_DIR = os.environ.get("XMQ_STATIC", os.path.join(os.path.dirname(os.path.abspath(__file__)), "static"))

# 云平台挂载的持久化磁盘（如 Render Disk = /var/data）
# 若存在该目录，数据库自动放进去，重启不丢数据。
_DATA_DIR = os.environ.get("XMQ_DATA_DIR", "/var/data")
if os.path.isdir(_DATA_DIR) and "XMQ_DB" not in os.environ:
    DB_PATH = os.path.join(_DATA_DIR, "data.db")
os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)

app = FastAPI(title="小钱袋·情侣版同步服务", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------- 数据库
def db():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=8000")
    return conn


def init_db():
    with closing(db()) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS households (
                id          TEXT PRIMARY KEY,
                invite_code TEXT UNIQUE NOT NULL,
                name        TEXT NOT NULL DEFAULT '我们的家',
                created_at  INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS members (
                id           TEXT PRIMARY KEY,
                household_id TEXT NOT NULL,
                nickname     TEXT NOT NULL,
                emoji        TEXT NOT NULL DEFAULT '🙂',
                color        TEXT NOT NULL DEFAULT '#FFB627',
                joined_at    INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS records (
                hh         TEXT NOT NULL,
                uid        TEXT NOT NULL,
                kind       TEXT NOT NULL,      -- tx | goal | recurring | challenge | template | cat | budget ...
                payload    TEXT NOT NULL,      -- JSON
                updated_at INTEGER NOT NULL,   -- 毫秒时间戳（客户端写入）
                deleted    INTEGER NOT NULL DEFAULT 0,
                server_at  INTEGER NOT NULL,   -- 服务端落库时间，单调递增用于增量游标
                PRIMARY KEY (hh, uid)
            );
            CREATE INDEX IF NOT EXISTS idx_records_cursor ON records(hh, server_at);
            """
        )
        conn.commit()


def gen_invite_code() -> str:
    # 去掉易混字符 0/O/1/I
    alphabet = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
    return "".join(random.choice(alphabet) for _ in range(6))


def unique_invite_code(conn) -> str:
    for _ in range(20):
        code = gen_invite_code()
        row = conn.execute("SELECT 1 FROM households WHERE invite_code=?", (code,)).fetchone()
        if not row:
            return code
    raise RuntimeError("无法生成邀请码")


def now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------- 模型
class CreateReq(BaseModel):
    nickname: str = "我"
    emoji: str = "🙂"
    color: str = "#FFB627"
    householdName: str = "我们的家"


class JoinReq(BaseModel):
    inviteCode: str
    nickname: str = "TA"
    emoji: str = "🐱"
    color: str = "#4ECDC4"


class Change(BaseModel):
    uid: str
    kind: str
    payload: Dict[str, Any] = {}
    updatedAt: float = 0
    deleted: bool = False


class SyncReq(BaseModel):
    hh: str
    me: str
    since: int = 0          # 服务端游标（server_at），0 表示全量
    changes: List[Change] = []


# ---------------------------------------------------------------- 鉴权
def require_member(hh: str, me: str):
    with closing(db()) as conn:
        h = conn.execute("SELECT * FROM households WHERE id=?", (hh,)).fetchone()
        if not h:
            raise HTTPException(404, "家庭不存在")
        m = conn.execute("SELECT * FROM members WHERE id=? AND household_id=?", (me, hh)).fetchone()
        if not m:
            raise HTTPException(403, "成员身份无效")
        return h, m


def member_dict(r) -> dict:
    return {"id": r["id"], "nickname": r["nickname"], "emoji": r["emoji"],
            "color": r["color"], "joinedAt": r["joined_at"]}


# ---------------------------------------------------------------- 家庭
@app.post("/api/household/create")
def household_create(req: CreateReq):
    hh_id = "hh" + uuid.uuid4().hex[:12]
    me_id = "m" + uuid.uuid4().hex[:12]
    with closing(db()) as conn:
        code = unique_invite_code(conn)
        conn.execute("INSERT INTO households(id, invite_code, name, created_at) VALUES(?,?,?,?)",
                     (hh_id, code, req.householdName or "我们的家", now_ms()))
        conn.execute("INSERT INTO members(id, household_id, nickname, emoji, color, joined_at) VALUES(?,?,?,?,?,?)",
                     (me_id, hh_id, req.nickname or "我", req.emoji or "🙂", req.color or "#FFB627", now_ms()))
        conn.commit()
    return {"ok": True, "householdId": hh_id, "inviteCode": code, "memberId": me_id,
            "nickname": req.nickname, "emoji": req.emoji, "color": req.color}


@app.post("/api/household/join")
def household_join(req: JoinReq):
    code = (req.inviteCode or "").strip().upper().replace(" ", "")
    if len(code) < 4:
        raise HTTPException(400, "邀请码格式不对")
    me_id = "m" + uuid.uuid4().hex[:12]
    with closing(db()) as conn:
        h = conn.execute("SELECT * FROM households WHERE invite_code=?", (code,)).fetchone()
        if not h:
            raise HTTPException(404, "邀请码无效，请核对后重试")
        # 同昵称视为同一人重进（同一台设备丢数据后可用昵称找回？不强制，仅提示）
        conn.execute("INSERT INTO members(id, household_id, nickname, emoji, color, joined_at) VALUES(?,?,?,?,?,?)",
                     (me_id, h["id"], req.nickname or "TA", req.emoji or "🐱", req.color or "#4ECDC4", now_ms()))
        conn.commit()
        mem = conn.execute("SELECT * FROM members WHERE household_id=?", (h["id"],)).fetchall()
    return {"ok": True, "householdId": h["id"], "inviteCode": h["invite_code"],
            "householdName": h["name"], "memberId": me_id,
            "nickname": req.nickname, "emoji": req.emoji, "color": req.color,
            "members": [member_dict(m) for m in mem]}


@app.get("/api/household/state")
def household_state(hh: str, me: str, since: int = 0):
    h, m = require_member(hh, me)
    with closing(db()) as conn:
        mem = conn.execute("SELECT * FROM members WHERE household_id=? ORDER BY joined_at", (hh,)).fetchall()
        rows = conn.execute(
            "SELECT uid, kind, payload, updated_at, deleted, server_at FROM records "
            "WHERE hh=? AND server_at>? ORDER BY server_at", (hh, since)
        ).fetchall()
        cursor = conn.execute("SELECT COALESCE(MAX(server_at),0) AS c FROM records WHERE hh=?", (hh,)).fetchone()["c"]
    return {
        "ok": True,
        "household": {"id": h["id"], "name": h["name"], "inviteCode": h["invite_code"]},
        "me": member_dict(m),
        "members": [member_dict(x) for x in mem],
        "records": [dict(uid=r["uid"], kind=r["kind"], payload=json.loads(r["payload"]),
                         updatedAt=r["updated_at"], deleted=bool(r["deleted"]), serverAt=r["server_at"])
                    for r in rows],
        "cursor": cursor,
    }


@app.post("/api/sync")
def sync(req: SyncReq):
    h, m = require_member(req.hh, req.me)
    applied = 0
    conflicts = 0
    with closing(db()) as conn:
        for ch in req.changes:
            if not ch.uid:
                continue
            up = int(ch.updatedAt or now_ms())
            cur = conn.execute("SELECT updated_at, server_at FROM records WHERE hh=? AND uid=?",
                               (req.hh, ch.uid)).fetchone()
            srv = now_ms()
            if cur is None:
                conn.execute(
                    "INSERT INTO records(hh, uid, kind, payload, updated_at, deleted, server_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (req.hh, ch.uid, ch.kind, json.dumps(ch.payload or {}, ensure_ascii=False),
                     up, 1 if ch.deleted else 0, srv),
                )
                applied += 1
            else:
                # last-write-wins；同刻按 server 顺序，后到覆盖
                if up >= cur["updated_at"]:
                    conn.execute(
                        "UPDATE records SET kind=?, payload=?, updated_at=?, deleted=?, server_at=? "
                        "WHERE hh=? AND uid=?",
                        (ch.kind, json.dumps(ch.payload or {}, ensure_ascii=False), up,
                         1 if ch.deleted else 0, srv, req.hh, ch.uid),
                    )
                    applied += 1
                else:
                    conflicts += 1
        conn.commit()

        # 回传该游标之后的全部记录（含自己刚推的，便于统一以服务端为准）
        rows = conn.execute(
            "SELECT uid, kind, payload, updated_at, deleted, server_at FROM records "
            "WHERE hh=? AND server_at>? ORDER BY server_at", (req.hh, req.since)
        ).fetchall()
        cursor = conn.execute("SELECT COALESCE(MAX(server_at),0) AS c FROM records WHERE hh=?", (req.hh,)).fetchone()["c"]
        mem = conn.execute("SELECT * FROM members WHERE household_id=? ORDER BY joined_at", (req.hh,)).fetchall()

    return {
        "ok": True, "applied": applied, "conflicts": conflicts,
        "records": [dict(uid=r["uid"], kind=r["kind"], payload=json.loads(r["payload"]),
                         updatedAt=r["updated_at"], deleted=bool(r["deleted"]), serverAt=r["server_at"])
                    for r in rows],
        "cursor": cursor,
        "members": [member_dict(x) for x in mem],
    }


@app.get("/api/health")
def health():
    with closing(db()) as conn:
        n = conn.execute("SELECT COUNT(*) AS n FROM households").fetchone()["n"]
        r = conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"]
    return {"ok": True, "households": n, "records": r, "ts": now_ms()}


# ---------------------------------------------------------------- 静态前端
init_db()

if os.path.isdir(STATIC_DIR):
    from fastapi.responses import HTMLResponse

    INDEX_PATH = os.path.join(STATIC_DIR, "index.html")

    @app.get("/")
    def index():
        try:
            with open(INDEX_PATH, "r", encoding="utf-8") as f:
                html = f.read()
        except FileNotFoundError:
            return HTMLResponse("<h1>前端未构建</h1><p>请把 index.html 放到 static/ 目录</p>", status_code=500)
        marker = '<script id="xmq-backend-flag">window.XMQ_BACKED=true;window.XMQ_SERVER=location.origin;</script>'
        if marker not in html:
            html = html.replace("</head>", marker + "</head>", 1)
        return HTMLResponse(html)

    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8080"))
    print(f"👛 小钱袋同步服务启动中… 数据库: {DB_PATH}")
    uvicorn.run(app, host="0.0.0.0", port=port)
