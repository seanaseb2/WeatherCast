#!/usr/bin/env python3
"""WeatherCast subscriber terminal."""
import argparse
import sys

from client_base import BrokerClient
from common import (BOLD, CYAN, DIM, GREEN, HOST, PORT, RED, RESET, YELLOW,
                    clear_screen, format_alert, print_header, setup_console)


def choose(prompt, count):
    raw = input(prompt).strip()
    return int(raw) if raw.isdigit() and 1 <= int(raw) <= count else None


def pick_location(options):
    for i, loc in enumerate(options, 1):
        print(f" [{i}] {loc}")
    idx = choose("\nSelect location: ", len(options))
    return options[idx - 1] if idx else None


def show_updates(updates):
    if not updates:
        print("\nNo weather updates yet for your locations.")
    for u in updates:
        print("\n" + format_alert(u))


def main():
    setup_console()
    p = argparse.ArgumentParser(description="WeatherCast subscriber")
    p.add_argument("--name")
    p.add_argument("--host", default=HOST, help="broker IP/hostname")
    p.add_argument("--port", type=int, default=PORT)
    a = p.parse_args()
    name = (a.name or input("Enter your username: ")).strip()

    def on_push(m):
        print("\n\n" + format_alert(m, missed=m.get("missed", False)))
        print("\nEnter choice: ", end="", flush=True)

    def on_status(text):
        print(f"\n*** {text}")

    client = BrokerClient("subscriber", name, a.host, a.port, on_push, on_status)
    try:
        ack = client.connect()
    except (OSError, ConnectionError) as exc:
        print(f"{RED}Cannot connect to broker at {a.host}:{a.port}: {exc}{RESET}")
        sys.exit(1)

    clear_screen()
    if ack.get("missed"):
        print(f"{YELLOW}*** You missed {ack['missed']} alert(s) while offline.{RESET}\n")

    while True:
        status = f"{GREEN}● ONLINE{RESET}" if not client.closed else f"{RED}● OFFLINE{RESET}"
        print_header("WEATHERCAST", "Location-Based Weather Alert System")
        print(f" User: {BOLD}{name}{RESET}   Status: {status}\n")

        if client.closed:
            print(" [1] Reconnect\n"
                  " [2] Exit")
            choice = input("\nEnter choice: ").strip()
            if choice == "1":
                clear_screen()
                try:
                    ack = client.connect()
                    print(f"{GREEN}Reconnected. Missed alerts: {ack.get('missed', 0)}{RESET}\n")
                except (OSError, ConnectionError) as exc:
                    print(f"{RED}Reconnect failed: {exc}{RESET}\n")
            elif choice == "2":
                break
            else:
                clear_screen()
                print(f"{RED}Invalid choice.{RESET}\n")
            continue

        print(" [1] Subscribe to Location\n"
              " [2] Unsubscribe from Location\n"
              " [3] View My Locations\n"
              " [4] View Weather Updates\n"
              " [5] Disconnect\n"
              " [6] Exit")
        choice = input("\nEnter choice: ").strip()

        if choice == "1":
            clear_screen()
            print_header("SUBSCRIBE TO LOCATION")
            print("\nAvailable Locations:")
            loc = pick_location(client.locations)
            if not loc:
                clear_screen()
                print(f"{RED}Invalid selection.{RESET}\n")
                continue
            r = client.request({"cmd": "subscribe", "location": loc})
            clear_screen()
            if not r["ok"]:
                print(f"{RED}Error: {r['error']}{RESET}\n")
            elif r.get("already"):
                print(f"{YELLOW}You are already subscribed to {loc}.{RESET}\n")
            else:
                print(f"{GREEN}Successfully subscribed to {loc}!{RESET}\n")

        elif choice == "2":
            r = client.request({"cmd": "my_locations"})
            clear_screen()
            if not r["ok"]:
                print(f"{RED}Error: {r['error']}{RESET}\n")
            elif not r["locations"]:
                print(f"{YELLOW}You have no subscriptions.{RESET}\n")
            else:
                print_header("UNSUBSCRIBE FROM LOCATION")
                print("\nYour Locations:")
                loc = pick_location(r["locations"])
                clear_screen()
                if loc:
                    r = client.request({"cmd": "unsubscribe", "location": loc})
                    if r["ok"]:
                        print(f"{GREEN}Unsubscribed from {loc}.{RESET}\n")
                    else:
                        print(f"{RED}Error: {r['error']}{RESET}\n")
                else:
                    print(f"{RED}Invalid selection.{RESET}\n")

        elif choice == "3":
            r = client.request({"cmd": "my_locations"})
            clear_screen()
            if not r["ok"]:
                print(f"{RED}Error: {r['error']}{RESET}\n")
            else:
                print_header("MY SUBSCRIBED LOCATIONS")
                locs = "\n".join(f"  • {x}" for x in r["locations"])
                print(f"\n{locs if locs else '  (none)'}\n")

        elif choice == "4":
            r = client.request({"cmd": "view_updates"})
            clear_screen()
            if r["ok"]:
                print_header("LATEST WEATHER UPDATES")
                show_updates(r["updates"])
                print()
            else:
                print(f"{RED}Error: {r['error']}{RESET}\n")

        elif choice == "5":
            client.disconnect()
            clear_screen()
            print(f"{YELLOW}Disconnected. Alerts published now will be queued for you.{RESET}\n")

        elif choice == "6":
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