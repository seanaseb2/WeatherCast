# Fault Test Plan (use for the report and the live demo)

Start the broker and: Juan + Maria subscribed to Pavia, Pedro subscribed to Kalibo, one publisher.

| # | Scenario | Steps | Expected result |
|---|---|---|---|
| 1 | Location filtering | Publish to Pavia | Juan and Maria get it instantly; Pedro gets nothing; publisher sees counts |
| 2 | Voluntary offline | Juan: `[5] Disconnect`; publish to Pavia; Juan: `[1] Reconnect` | Publisher shows 1 queued; Juan sees a `[MISSED WEATHER ALERT]` |
| 3 | Subscriber crash | Close Maria's terminal window (or Ctrl+C); publish; restart Maria | Broker log shows disconnect; alert is replayed on restart |
| 4 | Broker crash | Ctrl+C the broker; publish (error shown); restart broker | Clients print "Connection lost. Retrying..." then "Reconnected"; subscriptions intact; new alerts delivered |
| 5 | Publisher crash | Kill publisher mid-menu; start a new one | Broker unaffected; previous alerts still stored |
| 6 | Duplicate login | Start a second `subscriber.py --name Juan` | First terminal is disconnected with a notice |
| 7 | Network failure | Unplug network / block port (cross-machine demo) | Dead peer dropped within ~30 s; alerts queued |
| 8 | Unsubscribe | Pedro unsubscribes from Kalibo; publish to Kalibo | Pedro not counted; pending Kalibo alerts discarded |

Record for each: time, observed output, pass/fail, screenshot. `python tests/fault_test.py` covers scenarios 1-4 and 6 automatically.
