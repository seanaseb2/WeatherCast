#!/usr/bin/env python3
"""WeatherCast message broker.

Thread-per-connection TCP server. Subscriptions, messages and per-subscriber
delivery state are persisted in SQLite so that offline subscribers can
retrieve missed alerts and the broker itself can crash and restart safely.
"""
import argparse
import re
import socket
import sqlite3
import threading
from datetime import datetime

from common import (HEARTBEAT_TIMEOUT, HOST, LOCATIONS, PORT, LineSocket,
                    setup_console)

NAME_RE = re.compile(r"^[A-Za-z0-9_\- ]{1,32}$")
SCHEMA = """
CREATE TABLE IF NOT EXISTS subscribers(
    username TEXT PRIMARY KEY, first_seen TEXT, last_seen TEXT);
CREATE TABLE IF NOT EXISTS subscriptions(
    username TEXT NOT NULL, location TEXT NOT NULL,
    PRIMARY KEY(username, location));
CREATE TABLE IF NOT EXISTS messages(
    id INTEGER PRIMARY KEY AUTOINCREMENT, location TEXT NOT NULL,
    condition TEXT, temperature TEXT, alert_level TEXT, text TEXT,
    publisher TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS deliveries(
    username TEXT NOT NULL, message_id INTEGER NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0, delivered_at TEXT,
    PRIMARY KEY(username, message_id));
"""


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def ok(**kw):
    return {"type": "response", "ok": True, **kw}


def err(text):
    return {"type": "response", "ok": False, "error": text}


def push_from_row(row, missed=False):
    return {"type": "push", "missed": missed, "id": row["id"],
            "location": row["location"], "condition": row["condition"],
            "temperature": row["temperature"], "alert_level": row["alert_level"],
            "message": row["text"], "created_at": row["created_at"]}


class Session:
    def __init__(self, ls: LineSocket, role: str, name: str, addr):
        self.ls, self.role, self.name, self.addr = ls, role, name, addr

    def send(self, obj):
        return self.ls.send(obj)

    def close(self):
        self.ls.close()


class Broker:
    def __init__(self, host, port, db_path):
        self.host, self.port = host, port
        self.lock = threading.RLock()        # guards DB + online table
        self.online = {}                     # username -> Session
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self.db.commit()

    # ------------------------------------------------------------ server --
    def serve_forever(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(50)
        srv.settimeout(1.0)  # lets Ctrl+C work on Windows too
        log(f"Broker listening on {self.host}:{self.port}")
        try:
            while True:
                try:
                    conn, addr = srv.accept()
                except socket.timeout:
                    continue
                threading.Thread(target=self.handle, args=(conn, addr),
                                 daemon=True).start()
        except KeyboardInterrupt:
            log("Broker shutting down")
        finally:
            srv.close()

    def handle(self, conn, addr):
        conn.settimeout(HEARTBEAT_TIMEOUT)   # clients ping more often than this
        ls = LineSocket(conn)
        session = None
        try:
            hello = ls.recv()
            session = self._register(ls, hello, addr)
            if session is None:
                return
            while True:
                msg = ls.recv()
                if msg is None or msg.get("cmd") == "bye":
                    break
                self._dispatch(session, msg)
        except OSError as exc:
            log(f"Connection error {addr}: {exc!r}")
        finally:
            self._unregister(session, ls)

    # ----------------------------------------------------- registration --
    def _register(self, ls, hello, addr):
        if not hello or hello.get("type") != "hello":
            ls.send(err("Expected hello"))
            ls.close()
            return None
        role, name = hello.get("role"), str(hello.get("name", "")).strip()
        if role not in ("subscriber", "publisher") or not NAME_RE.match(name):
            ls.send(err("Invalid role or name (1-32 letters, digits, _ - space)"))
            ls.close()
            return None
        session = Session(ls, role, name, addr)
        if role == "publisher":
            ls.send(ok(info="connected"))
            log(f"Publisher '{name}' connected from {addr}")
            return session
        with self.lock:
            old = self.online.get(name)
            if old:
                old.send({"type": "kicked",
                          "reason": "Logged in from another terminal"})
                old.close()
            self.online[name] = session
            self.db.execute(
                "INSERT INTO subscribers VALUES(?,?,?) ON CONFLICT(username) "
                "DO UPDATE SET last_seen=excluded.last_seen", (name, now(), now()))
            rows = self.db.execute(
                "SELECT m.* FROM deliveries d JOIN messages m ON m.id=d.message_id "
                "WHERE d.username=? AND d.delivered=0 ORDER BY m.id", (name,)
            ).fetchall()
            ls.send(ok(info="connected", missed=len(rows)))
            for row in rows:
                if not session.send(push_from_row(row, missed=True)):
                    break
                self.db.execute(
                    "UPDATE deliveries SET delivered=1, delivered_at=? "
                    "WHERE username=? AND message_id=?", (now(), name, row["id"]))
            self.db.commit()
        log(f"Subscriber '{name}' connected from {addr} "
            f"({len(rows)} missed alert(s) replayed)")
        return session

    def _unregister(self, session, ls):
        ls.close()
        if session is None:
            return
        with self.lock:
            if self.online.get(session.name) is session and session.role == "subscriber":
                del self.online[session.name]
                self.db.execute("UPDATE subscribers SET last_seen=? WHERE username=?",
                                (now(), session.name))
                self.db.commit()
        log(f"{session.role.capitalize()} '{session.name}' disconnected")

    # ----------------------------------------------------------- commands --
    def _dispatch(self, s, d):
        cmd = d.get("cmd")
        if cmd == "ping":
            s.send({"type": "pong"})
            return
        table = {
            "subscriber": {"subscribe": self._subscribe, "unsubscribe": self._unsubscribe,
                           "my_locations": self._my_locations,
                           "view_updates": self._view_updates},
            "publisher": {"publish": self._publish, "published": self._published},
        }[s.role]
        handler = table.get(cmd)
        if handler is None:
            s.send(err(f"Unknown command '{cmd}' for {s.role}"))
            return
        try:
            s.send(handler(s, d))
        except sqlite3.Error as exc:
            log(f"DB error: {exc}")
            s.send(err("Broker storage error"))

    def _subscribe(self, s, d):
        loc = d.get("location")
        if loc not in LOCATIONS:
            return err("Unknown location")
        with self.lock:
            cur = self.db.execute("INSERT OR IGNORE INTO subscriptions VALUES(?,?)",
                                  (s.name, loc))
            self.db.commit()
        log(f"'{s.name}' subscribed to {loc}")
        return ok(location=loc, already=cur.rowcount == 0)

    def _unsubscribe(self, s, d):
        loc = d.get("location")
        with self.lock:
            cur = self.db.execute("DELETE FROM subscriptions WHERE username=? AND location=?",
                                  (s.name, loc))
            self.db.execute(
                "DELETE FROM deliveries WHERE username=? AND delivered=0 AND message_id IN "
                "(SELECT id FROM messages WHERE location=?)", (s.name, loc))
            self.db.commit()
        if cur.rowcount == 0:
            return err("You are not subscribed to that location")
        log(f"'{s.name}' unsubscribed from {loc}")
        return ok(location=loc)

    def _my_locations(self, s, d):
        with self.lock:
            rows = self.db.execute("SELECT location FROM subscriptions WHERE username=? "
                                   "ORDER BY location", (s.name,)).fetchall()
        return ok(locations=[r["location"] for r in rows])

    def _view_updates(self, s, d):
        with self.lock:
            rows = self.db.execute(
                "SELECT * FROM messages WHERE location IN "
                "(SELECT location FROM subscriptions WHERE username=?) "
                "ORDER BY id DESC LIMIT 10", (s.name,)).fetchall()
        return ok(updates=[push_from_row(r) for r in reversed(rows)])

    def _publish(self, s, d):
        loc = d.get("location")
        if loc not in LOCATIONS:
            return err("Unknown location")
        fields = {k: str(d.get(k, "")).strip()[:200]
                  for k in ("condition", "temperature", "alert_level", "message")}
        if not all(fields.values()):
            return err("All fields are required")
        with self.lock:
            cur = self.db.execute(
                "INSERT INTO messages(location,condition,temperature,alert_level,text,"
                "publisher,created_at) VALUES(?,?,?,?,?,?,?)",
                (loc, fields["condition"], fields["temperature"], fields["alert_level"],
                 fields["message"], s.name, now()))
            mid = cur.lastrowid
            row = self.db.execute("SELECT * FROM messages WHERE id=?", (mid,)).fetchone()
            users = [r["username"] for r in self.db.execute(
                "SELECT username FROM subscriptions WHERE location=?", (loc,))]
            live = 0
            for u in users:
                self.db.execute("INSERT OR IGNORE INTO deliveries(username,message_id) "
                                "VALUES(?,?)", (u, mid))
                target = self.online.get(u)
                if target and target.send(push_from_row(row)):
                    self.db.execute("UPDATE deliveries SET delivered=1, delivered_at=? "
                                    "WHERE username=? AND message_id=?", (now(), u, mid))
                    live += 1
            self.db.commit()
        log(f"Publisher '{s.name}' -> {loc}: {live}/{len(users)} delivered live, "
            f"{len(users) - live} queued")
        return ok(message_id=mid, total=len(users), delivered=live,
                  queued=len(users) - live)

    def _published(self, s, d):
        with self.lock:
            rows = self.db.execute("SELECT * FROM messages WHERE publisher=? "
                                   "ORDER BY id DESC LIMIT 20", (s.name,)).fetchall()
        return ok(updates=[push_from_row(r) for r in reversed(rows)])


def main():
    setup_console()
    p = argparse.ArgumentParser(description="WeatherCast broker")
    p.add_argument("--host", default=HOST)
    p.add_argument("--port", type=int, default=PORT)
    p.add_argument("--db", default="weathercast.db")
    a = p.parse_args()
    Broker(a.host, a.port, a.db).serve_forever()


if __name__ == "__main__":
    main()
