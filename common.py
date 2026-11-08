"""Shared constants and helpers for WeatherCast (low-level sockets only).

Wire protocol: newline-delimited JSON over TCP. One JSON object per line.
"""
import json
import socket
import sys
import threading

HOST = "127.0.0.1"
PORT = 5050
LOCATIONS = ["Iloilo City", "Pavia", "Roxas City", "Kalibo"]
ALERT_LEVELS = ["Low", "Moderate", "High", "Severe"]

HEARTBEAT_INTERVAL = 10   # seconds between client pings
HEARTBEAT_TIMEOUT = 30    # seconds of silence before a peer is considered dead
MAX_LINE = 65536


def setup_console():
    """Make sure symbols such as the degree sign print on any terminal."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass


class LineSocket:
    """Thread-safe send / line-buffered receive wrapper around a TCP socket."""

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self._rfile = sock.makefile("r", encoding="utf-8", newline="\n")
        self._wlock = threading.Lock()

    def send(self, obj: dict) -> bool:
        data = (json.dumps(obj) + "\n").encode("utf-8")
        try:
            with self._wlock:
                self.sock.sendall(data)
            return True
        except OSError:
            return False

    def recv(self):
        """Return the next message dict, or None if the peer closed.

        Raises OSError (including socket.timeout) on network failure.
        """
        line = self._rfile.readline(MAX_LINE)
        if not line:
            return None
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return {"type": "invalid"}
        return obj if isinstance(obj, dict) else {"type": "invalid"}

    def close(self):
        for closer in (lambda: self.sock.shutdown(socket.SHUT_RDWR), self.sock.close):
            try:
                closer()
            except OSError:
                pass


def format_temperature(value) -> str:
    text = str(value).strip()
    try:
        float(text)
        return f"{text}\u00b0C"
    except ValueError:
        return text


def format_alert(m: dict, missed: bool = False) -> str:
    tag = "[MISSED WEATHER ALERT]" if missed else "[WEATHER ALERT]"
    return "\n".join([
        tag,
        f"Location: {m['location']}",
        f"Condition: {m['condition']}",
        f"Temperature: {format_temperature(m['temperature'])}",
        f"Alert Level: {m['alert_level']}",
        f"Issued: {m['created_at']}",
        "",
        f"Message: {m['message']}",
    ])
