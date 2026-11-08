# WeatherCast: Terminal-Based Location-Specific Weather Alert System

A publish-subscribe system built on **low-level TCP sockets only** (no RPC or
messaging libraries). A central broker routes weather alerts from publishers to
subscribers by location, stores everything in SQLite, and replays missed alerts
to subscribers who were offline.

## Requirements
- Python 3.8+ (standard library only; see `requirements.txt`)

## Quick start (use 4+ terminal windows, from the project folder)
```bash
python broker.py                      # Terminal 1: start the broker (port 5050)
python subscriber.py --name Juan      # Terminal 2: subscriber
python subscriber.py --name Maria     # Terminal 3: another subscriber
python publisher.py --name PAGASA     # Terminal 4: publisher (manual entry)
python publisher.py --simulate 5      # optional: auto-publish a random alert every 5s
```
Options: `--host`, `--port` on every script; `--db` on the broker (default `weathercast.db`).
To demo across machines, run the broker with `--host 0.0.0.0` and point clients at its IP.

## Architecture
```
 Publisher(s) --publish--> [ Broker + SQLite ] --push--> Subscriber(s)
                              |  subscriptions
                              |  messages
                              |  deliveries (pending / delivered)
```
| File | Role |
|---|---|
| `broker.py` | Thread-per-connection TCP server; topic (location) filtering; persistence; replay |
| `subscriber.py` | Menu UI; receives live pushes asynchronously |
| `publisher.py` | Menu UI or `--simulate` mode |
| `client_base.py` | Connection, heartbeat, auto-reconnect with backoff, request/response |
| `common.py` | Constants, newline-delimited JSON framing (`LineSocket`), formatting |
| `tests/fault_test.py` | Automated crash/reconnect tests |

### Protocol
Newline-delimited JSON over TCP. First message is `{"type":"hello","role":"subscriber|publisher","name":"..."}`.
Client commands use `{"cmd": ...}`: `subscribe`, `unsubscribe`, `my_locations`, `view_updates`,
`publish`, `published`, `ping`, `bye`. Broker sends `response`, `push`, `pong`, `kicked`.

### Fault-tolerance design
| Failure | Handling |
|---|---|
| Subscriber offline / crashes | Delivery rows stay `delivered=0`; replayed (marked "MISSED") on reconnect |
| Half-open / dead connection | Clients ping every 10 s; broker drops peers silent for 30 s; failed sends are queued |
| Broker crash | State in SQLite survives; clients retry with backoff (1,2,4,5 s) and resume automatically |
| Duplicate username login | Old session receives `kicked` and is closed (prevents reconnect fights) |
| Malformed input | Invalid JSON / role / location / empty fields rejected with an error response |
| Concurrent publishers | One `RLock` serialises DB writes + delivery so alerts are never lost or duplicated |

## Testing
```bash
python tests/fault_test.py     # runs 13 automated checks (needs port 5999 free)
```
Manual live-demo scenarios are in `docs/fault_test_plan.md`.

## References / citations
All socket code was written by the team from the Python standard library
documentation. **Add any external socket reference you actually used here**
(required by the course), e.g.:
- Python Software Foundation. `socket`: Low-level networking interface. https://docs.python.org/3/library/socket.html
- Python Software Foundation. `sqlite3`: DB-API 2.0 interface for SQLite. https://docs.python.org/3/library/sqlite3.html

## Team
[Member names here]
