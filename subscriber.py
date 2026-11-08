#!/usr/bin/env python3
"""WeatherCast subscriber terminal."""
import argparse
import sys

from client_base import BrokerClient
from common import HOST, LOCATIONS, PORT, format_alert, setup_console

LINE = "-" * 40
BANNER = "=" * 40


def choose(prompt, count):
    raw = input(prompt).strip()
    return int(raw) if raw.isdigit() and 1 <= int(raw) <= count else None


def pick_location(options):
    for i, loc in enumerate(options, 1):
        print(f"[{i}] {loc}")
    idx = choose("\nSelect location: ", len(options))
    return options[idx - 1] if idx else None


def show_updates(updates):
    if not updates:
        print("No weather updates yet for your locations.")
    for u in updates:
        print("\n" + format_alert(u))
        print(LINE)


def main():
    setup_console()
    p = argparse.ArgumentParser(description="WeatherCast subscriber")
    p.add_argument("--name")
    p.add_argument("--host", default=HOST)
    p.add_argument("--port", type=int, default=PORT)
    a = p.parse_args()
    name = (a.name or input("Enter your username: ")).strip()

    def on_push(m):
        print("\n\n" + format_alert(m, missed=m.get("missed", False)) + "\n" + LINE)
        print("Enter choice: ", end="", flush=True)

    def on_status(text):
        print(f"\n*** {text}")

    client = BrokerClient("subscriber", name, a.host, a.port, on_push, on_status)
    try:
        ack = client.connect()
    except (OSError, ConnectionError) as exc:
        print(f"Cannot connect to broker at {a.host}:{a.port}: {exc}")
        sys.exit(1)
    if ack.get("missed"):
        print(f"\n*** You missed {ack['missed']} alert(s) while offline (shown below).")

    while True:
        print(f"\n{BANNER}\n            WEATHERCAST\n   Location-Based Weather Alert System\n{BANNER}\n")
        print(f"Welcome, {name}!\n")
        if client.closed:
            print("You are OFFLINE.\n[1] Reconnect\n[2] Exit")
            choice = input("\nEnter choice: ").strip()
            if choice == "1":
                try:
                    ack = client.connect()
                    print(f"Reconnected. Missed alerts retrieved: {ack.get('missed', 0)}")
                except (OSError, ConnectionError) as exc:
                    print(f"Reconnect failed: {exc}")
            elif choice == "2":
                break
            continue

        print("[1] Subscribe to Location\n[2] Unsubscribe from Location\n"
              "[3] View My Locations\n[4] View Weather Updates\n[5] Disconnect\n[6] Exit")
        choice = input("\nEnter choice: ").strip()
        if choice == "1":
            print("\nAvailable Locations:")
            loc = pick_location(LOCATIONS)
            if not loc:
                print("Invalid selection.")
                continue
            r = client.request({"cmd": "subscribe", "location": loc})
            if not r["ok"]:
                print(f"Error: {r['error']}")
            elif r.get("already"):
                print(f"\nYou are already subscribed to {loc}.")
            else:
                print(f"\nSuccessfully subscribed to {loc}!")
        elif choice == "2":
            r = client.request({"cmd": "my_locations"})
            if not r["ok"]:
                print(f"Error: {r['error']}")
            elif not r["locations"]:
                print("\nYou have no subscriptions.")
            else:
                print("\nYour Locations:")
                loc = pick_location(r["locations"])
                if loc:
                    r = client.request({"cmd": "unsubscribe", "location": loc})
                    print(f"\nUnsubscribed from {loc}." if r["ok"] else f"Error: {r['error']}")
                else:
                    print("Invalid selection.")
        elif choice == "3":
            r = client.request({"cmd": "my_locations"})
            if not r["ok"]:
                print(f"Error: {r['error']}")
            else:
                print("\nMy Locations:")
                print("\n".join(f"  - {x}" for x in r["locations"]) or "  (none)")
        elif choice == "4":
            r = client.request({"cmd": "view_updates"})
            if r["ok"]:
                print("\nLatest Weather Updates (up to 10):")
                show_updates(r["updates"])
            else:
                print(f"Error: {r['error']}")
        elif choice == "5":
            client.disconnect()
            print("\nDisconnected. Alerts published now will be queued for you.")
        elif choice == "6":
            client.disconnect()
            break
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nGoodbye.")
