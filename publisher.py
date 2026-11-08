#!/usr/bin/env python3
"""WeatherCast publisher terminal (manual entry, or --simulate)."""
import argparse
import random
import sys
import time

from client_base import BrokerClient
from common import ALERT_LEVELS, HOST, LOCATIONS, PORT, format_alert, setup_console

BANNER = "=" * 40
SAMPLES = [
    ("Heavy Rain", "Moderate", "Possible flooding in low-lying areas."),
    ("Thunderstorm", "High", "Avoid open areas and stay indoors."),
    ("Sunny", "Low", "Good weather. Stay hydrated."),
    ("Typhoon Signal #2", "Severe", "Strong winds expected. Secure loose objects."),
    ("Cloudy", "Low", "Light overcast, no precipitation expected."),
]


def choose(prompt, count):
    raw = input(prompt).strip()
    return int(raw) if raw.isdigit() and 1 <= int(raw) <= count else None


def pick(title, options):
    print(f"\n{title}")
    for i, o in enumerate(options, 1):
        print(f"[{i}] {o}")
    idx = choose("\nChoice: ", len(options))
    return options[idx - 1] if idx else None


def report(r):
    if not r["ok"]:
        print(f"\nPublish failed: {r['error']}")
        return
    print("\nWeather update published successfully!")
    print(f"\nSubscribers notified now: {r['delivered']} of {r['total']}"
          f" ({r['queued']} offline, queued for reconnect)")


def simulate(client, interval):
    print(f"Simulation mode: publishing a random update every {interval}s (Ctrl+C to stop)")
    while True:
        cond, level, msg = random.choice(SAMPLES)
        r = client.request({"cmd": "publish", "location": random.choice(LOCATIONS),
                            "condition": cond, "temperature": random.randint(24, 35),
                            "alert_level": level, "message": msg})
        report(r)
        time.sleep(interval)


def main():
    setup_console()
    p = argparse.ArgumentParser(description="WeatherCast publisher")
    p.add_argument("--name", default="Publisher")
    p.add_argument("--host", default=HOST)
    p.add_argument("--port", type=int, default=PORT)
    p.add_argument("--simulate", type=int, metavar="SECONDS")
    a = p.parse_args()
    client = BrokerClient("publisher", a.name, a.host, a.port,
                          on_status=lambda t: print(f"\n*** {t}"))
    try:
        client.connect()
    except (OSError, ConnectionError) as exc:
        print(f"Cannot connect to broker at {a.host}:{a.port}: {exc}")
        sys.exit(1)
    if a.simulate:
        simulate(client, a.simulate)
        return

    while True:
        print(f"\n{BANNER}\n       WEATHERCAST - PUBLISHER\n{BANNER}\n")
        print("[1] Publish Weather Update\n[2] View Published Updates\n[3] Exit")
        choice = input("\nEnter choice: ").strip()
        if choice == "1":
            loc = pick("Select Location:", LOCATIONS)
            if not loc:
                print("Invalid selection.")
                continue
            cond = input("\nWeather Condition: ").strip()
            temp = input("Temperature (C): ").strip()
            level = pick("Alert Level:", ALERT_LEVELS)
            if not level:
                print("Invalid selection.")
                continue
            msg = input("Weather Message: ").strip()
            if not (cond and temp and msg):
                print("All fields are required.")
                continue
            if input("\nPublish update? (Y/N): ").strip().lower() == "y":
                report(client.request({"cmd": "publish", "location": loc, "condition": cond,
                                       "temperature": temp, "alert_level": level,
                                       "message": msg}))
        elif choice == "2":
            r = client.request({"cmd": "published"})
            if not r["ok"]:
                print(f"Error: {r['error']}")
            for u in r.get("updates", []) or []:
                print("\n" + format_alert(u) + "\n" + "-" * 40)
            if r["ok"] and not r["updates"]:
                print("Nothing published yet.")
        elif choice == "3":
            client.disconnect()
            break
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nGoodbye.")
