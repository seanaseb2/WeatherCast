#!/usr/bin/env python3
"""WeatherCast publisher terminal (manual entry, or --simulate)."""
import argparse
import random
import sys
import time

from client_base import BrokerClient
from common import (ALERT_LEVELS, BOLD, GREEN, HOST, PORT, RED, RESET,
                    clear_screen, format_alert, print_header, setup_console)

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
    print(f"\n{BOLD}{title}{RESET}")
    for i, o in enumerate(options, 1):
        print(f" [{i}] {o}")
    idx = choose("\nChoice: ", len(options))
    return options[idx - 1] if idx else None


def report(r):
    if not r["ok"]:
        print(f"\n{RED}Publish failed: {r['error']}{RESET}")
        return
    print(f"\n{GREEN}Weather update published successfully!{RESET}")
    print(f"Subscribers notified now: {r['delivered']} of {r['total']}"
          f" ({r['queued']} offline, queued for reconnect)")


def simulate(client, interval):
    print(f"Simulation mode: publishing a random update every {interval}s (Ctrl+C to stop)")
    while True:
        cond, level, msg = random.choice(SAMPLES)
        r = client.request({"cmd": "publish", "location": random.choice(client.locations),
                            "condition": cond, "temperature": random.randint(24, 35),
                            "alert_level": level, "message": msg})
        report(r)
        time.sleep(interval)


def main():
    setup_console()
    p = argparse.ArgumentParser(description="WeatherCast publisher")
    p.add_argument("--name", default="Publisher")
    p.add_argument("--host", default=HOST, help="broker IP/hostname")
    p.add_argument("--port", type=int, default=PORT)
    p.add_argument("--simulate", type=int, metavar="SECONDS")
    a = p.parse_args()
    client = BrokerClient("publisher", a.name, a.host, a.port,
                          on_status=lambda t: print(f"\n*** {t}"))
    try:
        client.connect()
    except (OSError, ConnectionError) as exc:
        print(f"{RED}Cannot connect to broker at {a.host}:{a.port}: {exc}{RESET}")
        sys.exit(1)
    if a.simulate:
        simulate(client, a.simulate)
        return

    clear_screen()
    while True:
        print_header("WEATHERCAST - PUBLISHER", f"Logged in as {a.name}")
        print("\n [1] Publish Weather Update\n"
              " [2] View Published Updates\n"
              " [3] Exit")
        choice = input("\nEnter choice: ").strip()

        if choice == "1":
            clear_screen()
            print_header("PUBLISH WEATHER UPDATE")
            loc = pick("Select Location:", client.locations)
            if not loc:
                print(f"{RED}Invalid selection.{RESET}\n")
                continue
            cond = input("\nWeather Condition: ").strip()
            temp = input("Temperature (C): ").strip()
            try:
                float(temp)
            except ValueError:
                print(f"{RED}Temperature must be a number.{RESET}\n")
                continue
            level = pick("Alert Level:", ALERT_LEVELS)
            if not level:
                print(f"{RED}Invalid selection.{RESET}\n")
                continue
            msg = input("\nWeather Message: ").strip()
            if not (cond and temp and msg):
                print(f"{RED}All fields are required.{RESET}\n")
                continue
            if input("\nPublish update? (Y/N): ").strip().lower() == "y":
                report(client.request({"cmd": "publish", "location": loc, "condition": cond,
                                       "temperature": temp, "alert_level": level,
                                       "message": msg}))
                print()
            else:
                clear_screen()

        elif choice == "2":
            clear_screen()
            print_header("PUBLISHED HISTORY")
            r = client.request({"cmd": "published"})
            if not r["ok"]:
                print(f"\n{RED}Error: {r['error']}{RESET}")
            for u in r.get("updates", []) or []:
                print("\n" + format_alert(u))
            if r["ok"] and not r["updates"]:
                print("\nNothing published yet.")
            print()

        elif choice == "3":
            client.disconnect()
            break

        else:
            clear_screen()
            print(f"{RED}Invalid choice.{RESET}\n")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nGoodbye.")