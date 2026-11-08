#!/usr/bin/env python3
"""Automated fault tests: run `python tests/fault_test.py` from the project root."""
import os
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from client_base import BrokerClient  # noqa: E402

PORT = 5999
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(tempfile.mkdtemp(), "test.db")
results = []


def check(name, cond):
    results.append(cond)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")


def start_broker():
    p = subprocess.Popen([sys.executable, os.path.join(ROOT, "broker.py"), "--port",
                          str(PORT), "--db", DB], stdout=subprocess.DEVNULL)
    time.sleep(1)
    return p


def wait_for(fn, timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        if fn():
            return True
        time.sleep(0.2)
    return False


def publish(pub, loc, text="test"):
    return pub.request({"cmd": "publish", "location": loc, "condition": "Rain",
                        "temperature": "27", "alert_level": "Moderate", "message": text})


def make(role, name, inbox=None):
    quiet = lambda *_: None
    return BrokerClient(role, name, "127.0.0.1", PORT,
                        on_push=(inbox.append if inbox is not None else None),
                        on_status=quiet)


broker = start_broker()
try:
    alice_in, bob_in = [], []
    alice, bob, pub = make("subscriber", "alice", alice_in), make("subscriber", "bob", bob_in), make("publisher", "PAGASA")
    for c in (alice, bob, pub):
        c.connect()
    alice.request({"cmd": "subscribe", "location": "Pavia"})
    bob.request({"cmd": "subscribe", "location": "Kalibo"})

    print("Test 1: location-based filtering")
    r = publish(pub, "Pavia", "live-1")
    time.sleep(0.5)
    check("publisher sees 1 live delivery", r["delivered"] == 1 and r["total"] == 1)
    check("alice (Pavia) received the alert", len(alice_in) == 1)
    check("bob (Kalibo) received nothing", len(bob_in) == 0)

    print("Test 2: offline subscriber retrieves missed alerts")
    alice.disconnect()
    time.sleep(0.5)
    r = publish(pub, "Pavia", "while-offline")
    check("alert queued for offline subscriber", r["queued"] == 1 and r["delivered"] == 0)
    alice_in.clear()
    ack = alice.connect()
    time.sleep(0.5)
    check("missed alert replayed on reconnect",
          ack["missed"] == 1 and len(alice_in) == 1 and alice_in[0]["missed"])

    print("Test 3: subscriber crash (socket dropped without goodbye)")
    alice.ls.close()
    alice.connected = False
    alice.closed = True          # simulate process death: no auto-reconnect
    time.sleep(1)
    r = publish(pub, "Pavia", "after-crash")
    check("broker detected dead client and queued alert", r["queued"] == 1)
    alice_in.clear()
    alice.connect()
    time.sleep(0.5)
    check("crashed subscriber recovers missed alert", len(alice_in) == 1)

    print("Test 4: broker crash and restart")
    broker.kill()
    broker.wait()
    time.sleep(1)
    r = publish(pub, "Pavia")
    check("publisher gets clear error while broker is down", not r["ok"])
    broker = start_broker()
    check("subscriber auto-reconnects", wait_for(lambda: alice.connected))
    check("publisher auto-reconnects", wait_for(lambda: pub.connected))
    alice_in.clear()
    r = publish(pub, "Pavia", "after-restart")
    time.sleep(0.5)
    check("subscriptions persisted across broker restart", r["total"] == 1)
    check("alert delivered after broker restart", len(alice_in) == 1)

    print("Test 5: duplicate login replaces old session")
    alice2 = make("subscriber", "alice")
    alice2.connect()
    check("old session kicked", wait_for(lambda: alice.closed, 3))
    alice2.disconnect()
finally:
    broker.kill()

print(f"\n{sum(results)}/{len(results)} checks passed")
sys.exit(0 if all(results) else 1)
