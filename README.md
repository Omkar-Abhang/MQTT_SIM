# mqtt_device_simulator

Simulates thousands of **ACCON** and **CO2TH** devices publishing to an MQTT broker, with message shapes copied
from real captured traffic (244k messages, 217 devices) and from the firmware protocol documents.
Use it to load-test EMQX, `mqtt_rmq_receiver` and `mqtt_consumer` without real hardware.

```
simulator ──MQTT──▶ EMQX ──rule──▶ RMQ #1 ──▶ mqtt_rmq_receiver ──▶ Influx (raw) + RMQ #2 ──▶ mqtt_consumer
```

## Quick start

```bash
pip install -r requirements.txt
cp .env.example .env            # then edit MQTT_HOST / port / credentials
python main.py --dry-run 1      # print what a device of each type would send. Nothing is published
python main.py --accon 2000 --co2th 1000
```

Docker (one socket per simulated device, so give it a high open-files limit):

```bash
docker build -t mqtt-device-simulator .
docker run --rm --env-file .env --ulimit nofile=65536:65536 mqtt-device-simulator
```

Stop with Ctrl-C or SIGTERM: it disconnects cleanly and prints a summary.

## Safety: simulated devices are always recognisable

| | Simulated | Real |
|---|---|---|
| ACCON serial | `IACCON-9999-0001` | `IACCON-2608-0087` |
| CO2TH serial | `CO2TH-9999-0001` | `CO2TH-2607-0058` |
| MAC | `02:AC:00:..` / `02:C2:00:..` (locally administered, never a real vendor MAC) | `84:1F:E8:..`, `5C:01:3B:..` |

`9999` can never be a real month, so these devices are easy to find and purge downstream (the receiver's raw
bucket, device log files). `SERIAL_BATCH` changes it. If `MQTT_HOST` is not local, the simulator prints a warning and
waits 5 seconds before publishing (`--yes` skips the pause).

## Settings (`.env`, flags override them)

| Setting | Default | Meaning |
|---|---|---|
| `MQTT_HOST`, `MQTT_PORT` | `127.0.0.1`, `1883` | the broker |
| `MQTT_USERNAME`, `MQTT_PASSWORD` | empty | sent by every connection |
| `MQTT_PROTOCOL` | `3.1.1` | what the ESP devices speak. `5` is also supported |
| `MQTT_QOS` | `0` | real devices use QoS 0 |
| `MQTT_KEEPALIVE`, `MQTT_CLEAN_SESSION` | `60`, `true` | |
| `MQTT_CLIENT_ID_PREFIX` | `IES-` | client id = prefix + MAC, like real devices |
| `MQTT_TOPIC_PREFIX` | `IES` | topics are `<prefix>/<TYPE>/<SERIAL>/<channel>` |
| `MQTT_TLS` | `false` | enable TLS |
| `MQTT_TLS_CA_CERT` | empty | CA that signed the broker certificate (empty = system CAs) |
| `MQTT_TLS_CLIENT_CERT`, `MQTT_TLS_CLIENT_KEY` | empty | only for brokers that require client certificates |
| `MQTT_TLS_INSECURE` | `false` | encrypt but do not verify the broker certificate (self-signed, connecting by IP) |
| `MQTT_CONNECTION_MODE` | `per_device` | `per_device`: one connection per device, what EMQX sees from a real fleet. `shared`: `MQTT_SHARED_CONNECTIONS` connections carry all devices |
| `MQTT_CONNECT_RATE` | `100` | new connections per second while ramping up |
| `ACCON_COUNT`, `CO2TH_COUNT` | `1000`, `1000` | fleet size (`--accon`, `--co2th`) |
| `CO2TH_PROTOCOL` | `legacy` | `legacy` = `/metrics`, firmware 1.0.1. `iesmq-v1` = `/data` and the richer heartbeat of firmware 1.0.2 |
| `METRICS_INTERVAL_SECONDS`, `HB_INTERVAL_SECONDS` | `60`, `900` | what the real fleet does |
| `INTERVAL_SCALE` | `1` | `10` = everything runs 10x faster than real time (`--scale`) |
| `DURATION_SECONDS` | `0` | `0` = until stopped (`--duration`) |
| `BAD_TS_PERCENT` | `0` | share of messages with a faulty timestamp, as seen on a real device: garbled, RTC reset to 2000, empty, seconds = 60 |
| `DUPLICATE_PERCENT` | `0` | share of messages delivered twice (real fleet: about 0.8%) |
| `RANDOM_SEED` | `1` | same seed = same fleet and same behaviour every run |

A bad value is rejected at start-up with the setting's name.

## What it sends

| Device | Channel | When | Content |
|---|---|---|---|
| ACCON | `metrics` | every 60 s, **two messages** | `misc` + `temperature` (`RLY1-6`, `R1-6_RH`, `R1-6_ORH`, `TEMP1`), then `override` (`RLY1-6`) |
| ACCON | `hb` | every 15 min | `DBM`, `DIP`, `CPT`, `d_data: devid0 1006` |
| ACCON | `alert` | on each relay change | 40% of units alternate a relay pair every 15/30/60 min: the relay going off reports first, the other 1-2 s later |
| CO2TH | `metrics` | every 60 s | `temperature` (TEMP1/2), `humidity` (HUMI1/2), `co2` (CO2, CO2_RAW) |
| CO2TH | `hb` | every 15 min | `DBM`, `CPT`, `d_data: []` (1.5% carry `[{}]`) |
| CO2TH (`iesmq-v1`) | `data`, `hb` | as above | `pv`, IO states in `data`, heartbeat with `DIP`, `ESPDT`, `WiFiSTA`, `MQTTSTA`, `RST` |

Details copied from the real fleet: compact JSON, field order, whole numbers printed without `.0`, `TEMP1` in 1/16 degree
steps, CO2TH values with 8 decimals, run-hour counters that move in steps of 5, each device starting at its own random
phase of the cycle. A few ACCON units have only 2 relays, and about 1% have an override.

Not simulated: the control plane (`srv` / `clt` / `req`), `txn` batches, and the IESMBG Modbus gateway.

## Measured

On one machine, against a local broker (pure Python `amqtt`, not EMQX):

| Fleet | Result |
|---|---|
| 2,000 ACCON + 1,000 CO2TH, one connection each, 5x speed | 3,000 connections, steady **438 messages/s**, 0 connect failures, 0 publish errors, peak **106 MB**, **14%** of one CPU core |

The simulator is one asyncio loop with no thread per device. If a machine cannot keep up, the status line says
`BEHIND SCHEDULE`. At 1x speed the same 3,000 devices send about 87 messages/s.

## Status line (every `STATS_INTERVAL_SECONDS`)

```
connected 3000/3000 | sent 8,748 (437/s) ACCON/alert=112 ACCON/hb=222 ACCON/metrics=6650 CO2TH/hb=101 CO2TH/metrics=1663
```

`skipped_not_connected` counts messages a device could not send because its connection was down, as a real offline
device sends nothing. Clients reconnect on their own after a broker restart.

## Testing a receiver with faults

```bash
python main.py --accon 200 --co2th 100 --scale 10 --duration 120   # BAD_TS_PERCENT=5 DUPLICATE_PERCENT=1 in .env
```

With `BAD_TS_PERCENT` set, `mqtt_rmq_receiver` logs `!TS_FIXED` lines with the original timestamp and still processes
the messages.

## Tests

```bash
pip install -r requirements-dev.txt
python tests/selftest.py
```

Runs the simulator against an in-process MQTT broker (including a TLS broker with a generated certificate) and checks:
every message shape against real captured messages (keys, order, zones, value types), that the real
`mqtt_rmq_receiver` code accepts everything sent (if it sits next to this folder), both connection modes,
reconnection after a broker restart, fault injection, TLS (verified, insecure, untrusted), credentials and
configuration errors.

Not tested here: a real EMQX broker, and anything beyond 3,000 connections.
