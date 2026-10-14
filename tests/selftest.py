"""End-to-end self-test: the simulator against a real (in-process) MQTT broker.

    pip install -r requirements-dev.txt
    python tests/selftest.py

Checks: message shapes against real captured messages, receiver acceptance (if ../mqtt_rmq_receiver is next to this
folder), per-device and shared connections, reconnect after a broker restart, fault injection, config validation.
"""
import asyncio
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
os.environ.setdefault("TZ", "Asia/Kolkata")
time.tzset() if hasattr(time, "tzset") else None

import logging
logging.disable(logging.CRITICAL)
from loguru import logger
logger.remove()

from amqtt.broker import Broker
from gmqtt import Client
from gmqtt.mqtt.constants import MQTTv311

from src import config
from src.devices import build_fleet
from src.mqtt_pool import ClientPool, build_ssl_context
from src.runner import Runner

PORT = 18840
results = []


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(("PASS " if ok else "FAIL ") + name + (f"   {detail}" if detail else ""))


# ---- real messages captured from the live fleet (the ground truth for "does it look real") -------------------
REAL = {
    ("ACCON", "metrics", "misc"): '{"ts":"2026-10-05 17:49:37","model":"ACCON","ver":"1.3.4-R","srno":"IACCON-2608-0087","mac":"00:4B:12:0E:8F:E0","gwid":"26080087","devid":0,"mes":{"misc":["RLY1","RLY2","RLY3","RLY4","RLY5","RLY6","R1_RH","R2_RH","R3_RH","R4_RH","R5_RH","R6_RH","R1_ORH","R2_ORH","R3_ORH","R4_ORH","R5_ORH","R6_ORH"],"temperature":["TEMP1"]},"data":[{"z":"RLY1","v":0},{"z":"RLY2","v":0},{"z":"RLY3","v":0},{"z":"RLY4","v":0},{"z":"RLY5","v":0},{"z":"RLY6","v":0},{"z":"R1_RH","v":10105},{"z":"R2_RH","v":10110},{"z":"R3_RH","v":0},{"z":"R4_RH","v":0},{"z":"R5_RH","v":0},{"z":"R6_RH","v":0},{"z":"R1_ORH","v":0},{"z":"R2_ORH","v":0},{"z":"R3_ORH","v":0},{"z":"R4_ORH","v":0},{"z":"R5_ORH","v":0},{"z":"R6_ORH","v":0},{"z":"TEMP1","v":30.0625}]}',
    ("ACCON", "metrics", "override"): '{"ts":"2026-10-05 18:14:59","model":"ACCON","ver":"1.3.4-R","srno":"IACCON-2608-0088","mac":"00:4B:12:0E:84:84","gwid":"26080088","devid":0,"mes":{"override":["RLY1","RLY2","RLY3","RLY4","RLY5","RLY6"]},"data":[{"z":"RLY1","v":0},{"z":"RLY2","v":0},{"z":"RLY3","v":0},{"z":"RLY4","v":0},{"z":"RLY5","v":0},{"z":"RLY6","v":0}]}',
    ("ACCON", "alert", "misc"): '{"ts":"2026-10-05 18:16:01","model":"ACCON","ver":"1.3.4-R","srno":"IACCON-2608-0088","mac":"00:4B:12:0E:84:84","gwid":"26080088","devid":0,"mes":{"misc":["RLY1"]},"data":[{"z":"RLY1","v":0}]}',
    ("ACCON", "hb", ""): '{"ts":"2026-10-05 18:28:59","model":"ACCON","ver":"1.3.4-R","srno":"IACCON-2608-0088","mac":"00:4B:12:0E:84:84","gwid":"26080088","devid":0,"s_data":[{"z":"DBM","v":-56},{"z":"DIP","v":"10.1.252.190"},{"z":"CPT","v":28}],"d_data":[{"devid0":1006}]}',
    ("CO2TH", "metrics", "co2"): '{"ts":"2026-10-05 18:15:41","model":"CO2TH","ver":"1.0.1","srno":"CO2TH-2607-0058","mac":"5C:01:3B:20:47:6C","gwid":"26070058","devid":0,"mes":{"temperature":["TEMP1","TEMP2"],"humidity":["HUMI1","HUMI2"],"co2":["CO2","CO2_RAW"]},"data":[{"z":"CO2","v":1790},{"z":"CO2_RAW","v":1848},{"z":"TEMP1","v":25.68627357},{"z":"HUMI1","v":59.38658905},{"z":"TEMP2","v":24.48043823},{"z":"HUMI2","v":65.88039398}]}',
    ("CO2TH", "hb", ""): '{"ts":"2026-10-05 18:26:40","model":"CO2TH","ver":"1.0.1","srno":"CO2TH-2607-0058","mac":"5C:01:3B:20:47:6C","gwid":"26070058","devid":0,"s_data":[{"z":"DBM","v":-52},{"z":"CPT","v":48}],"d_data":[]}',
    ("CO2TH", "data", "co2"): '{"pv":"iesmq-v1","ts":"2026-10-05 17:50:17","model":"CO2TH","ver":"1.0.2","srno":"8C8C29B76DD8","mac":"8C:8C:29:B7:6D:D8","gwid":"111111","devid":0,"data":[{"z":"CO2","v":3866},{"z":"CO2_RAW","v":3900},{"z":"TEMP1","v":29.97215271},{"z":"HUMI1","v":38.90135193},{"z":"TEMP2","v":28.31516266},{"z":"HUMI2","v":42.45662689},{"z":"IN1STA","v":0},{"z":"IN2STA","v":0},{"z":"OUT1STA","v":1},{"z":"OUT2STA","v":1}],"mes":{"co2":["CO2","CO2_RAW"],"temperature":["TEMP1","TEMP2"],"humidity":["HUMI1","HUMI2"],"misc":["IN1STA","IN2STA","OUT1STA","OUT2STA"]}}',
    ("CO2TH", "hb", "v1"): '{"pv":"iesmq-v1","ts":"2026-10-05 18:02:17","model":"CO2TH","ver":"1.0.2","srno":"8C8C29B76DD8","mac":"8C:8C:29:B7:6D:D8","gwid":"111111","devid":0,"s_data":[{"z":"DBM","v":-62},{"z":"DIP","v":"10.160.187.61"},{"z":"CPT","v":46},{"z":"ESPDT","v":"2026-10-05 18:02:17"},{"z":"WiFiSTA","v":3},{"z":"MQTTSTA","v":0},{"z":"RST","v":"SW"}],"d_data":[{"devid0":1006}]}',
}


def signature(raw):
    """Everything about a message except its values: key order, group/zone order, and each value's JSON type."""
    p = json.loads(raw)
    def ty(v):
        return type(v).__name__
    sig = [("keys", tuple(p))]
    for k in ("mes",):
        if k in p:
            sig.append((k, tuple((g, tuple(z)) for g, z in p[k].items())))
    for k in ("data", "s_data"):
        if k in p:
            sig.append((k, tuple((r["z"], ty(r["v"])) for r in p[k])))
    if "d_data" in p:
        sig.append(("d_data", tuple(tuple((kk, ty(vv)) for kk, vv in r.items()) for r in p["d_data"])))
    sig.append(("types", tuple((k, ty(p[k])) for k in ("ts", "devid", "gwid", "srno", "mac") if k in p)))
    return sig


def make_cfg(**env):
    base = dict(MQTT_HOST="127.0.0.1", MQTT_PORT=str(PORT), ACCON_COUNT="10", CO2TH_COUNT="10", INTERVAL_SCALE="60",
                MQTT_CONNECT_RATE="500", STATS_INTERVAL_SECONDS="3600", DURATION_SECONDS="0", RANDOM_SEED="7")
    base.update({k: str(v) for k, v in env.items()})
    saved = {k: os.environ.get(k) for k in base}
    os.environ.update(base)
    try:
        return config.load()
    finally:
        for k, v in saved.items():
            os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)


async def start_broker():
    b = Broker({"listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{PORT}"}},
                "plugins": {"amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True}}})
    await b.start()
    return b


async def collector(box):
    c = Client("collector")
    c.on_message = lambda cl, topic, payload, qos, props: box.append((time.time(), topic, payload.decode())) or 0
    await c.connect("127.0.0.1", PORT, version=MQTTv311)
    c.subscribe("IES/#")
    return c


async def run_sim(cfg, seconds):
    devices = build_fleet(cfg)
    pool = ClientPool(cfg, devices)
    runner = Runner(cfg, devices, pool)
    stop = asyncio.Event()
    pool.start()
    await pool.wait_connected(timeout=30)
    task = asyncio.ensure_future(runner.run(stop))
    await asyncio.sleep(seconds)
    stop.set()
    await task
    await asyncio.sleep(0.5)
    return devices, pool, runner


def run_receiver_check(messages):
    """Feed every message through the REAL receiver code (parser, ts guard, validators, dispatcher)."""
    recv = os.path.join(os.path.dirname(ROOT), "mqtt_rmq_receiver")
    if not os.path.isdir(os.path.join(recv, "src")):
        return None
    script = r'''
import json, sys, types, os, tempfile
os.environ.update(LOG_DIR=tempfile.mkdtemp(), PRODUCER_QUEUE_NAME="Q", PRODUCER_EXCHANGE_NAME="X",
                  PRODUCER_EXCHANGE_TYPE="fanout", INFLUX_RAW_BUCKET="b", ENABLE_INFLUX="false", ENABLE_RMQ_PUBLISH="false")
from loguru import logger; logger.remove()
import src.consumer as C
statuses = []
orig = C.log_status; C.log_status = lambda m, s, d="": (statuses.append((s, d)), orig(m, s, d))
class Ch:
    acks = 0
    def basic_ack(s, delivery_tag): s.acks += 1
    def basic_nack(s, delivery_tag, requeue): pass
ch = Ch(); n = 0
for line in sys.stdin:
    topic, clientid, payload = json.loads(line)
    body = json.dumps({"topic": topic, "clientid": clientid, "payload": payload}).encode()
    C.process_message(ch, types.SimpleNamespace(delivery_tag=1, redelivered=False), types.SimpleNamespace(headers=None), body); n += 1
from collections import Counter
print(json.dumps({"n": n, "acks": ch.acks, "statuses": dict(Counter(s for s, _ in statuses))}))
'''
    feed = "\n".join(json.dumps([t, "IES-sim", p]) for t, p in messages)
    out = subprocess.run([sys.executable, "-c", script], input=feed, text=True, capture_output=True, cwd=recv,
                         env=dict(os.environ, PYTHONPATH=recv), timeout=300)
    return json.loads(out.stdout.strip().splitlines()[-1]) if out.returncode == 0 and out.stdout.strip() else {"error": out.stderr[-400:]}


async def main():
    broker = await start_broker()
    box = []
    col = await collector(box)

    # ---- A. shapes match real captured messages -----------------------------------------------------------
    for proto in ("legacy", "iesmq-v1"):
        cfg = make_cfg(CO2TH_PROTOCOL=proto)
        devs = build_fleet(cfg)
        sample = {}
        for d in devs:
            for ch, p in d.metrics(0.0) + d.hb():
                kind = ("misc" if "misc" in json.loads(p).get("mes", {}) and "temperature" in json.loads(p)["mes"] and d.TYPE == "ACCON"
                        else "override" if "override" in json.loads(p).get("mes", {})
                        else "co2" if "co2" in json.loads(p).get("mes", {}) else "")
                if ch == "hb" and proto == "iesmq-v1" and d.TYPE == "CO2TH":
                    kind = "v1"
                sample.setdefault((d.TYPE, ch, kind), p)
            if d.TYPE == "ACCON" and getattr(d, "alternating", False):
                sample.setdefault(("ACCON", "alert", "misc"), d.toggle(15.0)[0][2])
        for key, real in REAL.items():
            if (proto == "legacy" and key in (("CO2TH", "data", "co2"), ("CO2TH", "hb", "v1"))) or \
               (proto == "iesmq-v1" and key in (("CO2TH", "metrics", "co2"), ("CO2TH", "hb", ""))):
                continue
            simulated = sample.get(key)
            check(f"A  [{proto}] {key[0]} {key[1]} {key[2] or ''}: same keys, order, zones and value types as a real message".replace("  ", " "),
                  simulated is not None and signature(simulated) == signature(real),
                  "" if simulated is not None and signature(simulated) == signature(real) else f"\n      real: {signature(real)}\n      sim : {signature(simulated) if simulated else None}")

    # ---- B. a fleet over per-device connections ------------------------------------------------------------
    box.clear()
    cfg = make_cfg(ACCON_COUNT=300, CO2TH_COUNT=200, INTERVAL_SCALE=60)
    t = time.time()
    devices, pool, runner = await run_sim(cfg, 12)
    msgs = list(box)
    topics = Counter()
    bad_topic = [m for m in msgs if not re.fullmatch(r"IES/(ACCON/IACCON-9999-\d{4}|CO2TH/CO2TH-9999-\d{4})/(metrics|hb|alert)", m[1])]
    for _, tp, _ in msgs:
        topics[(tp.split("/")[1], tp.split("/")[3])] += 1
    seen = {tp.split("/")[2] for _, tp, _ in msgs}
    check("B1 500 devices over 500 connections: every device published", len(seen) == 500 and pool.connected_count >= 495,
          f"{len(seen)}/500 devices, {pool.connected_count}/{pool.total} connected, {len(msgs)} messages in 12 s")
    check("B2 all topics are IES/<TYPE>/<fake serial>/<channel>", not bad_topic, f"{dict(topics)}")
    sessions = len(getattr(broker, "_sessions", {})) - 1
    check("B3 one MQTT connection per device (client ids IES-<MAC>)", sessions >= 495, f"broker sessions: {sessions}")
    accon_metrics = defaultdict(list)
    for _, tp, pl in msgs:
        if "/ACCON/" in tp and tp.endswith("/metrics"):
            accon_metrics[tp.split("/")[2]].append(json.loads(pl))
    pairs_ok = all(sum(1 for p in v if "misc" in p["mes"]) == sum(1 for p in v if "override" in p["mes"]) or
                   abs(sum(1 for p in v if "misc" in p["mes"]) - sum(1 for p in v if "override" in p["mes"])) <= 1
                   for v in accon_metrics.values())
    check("B4 ACCON metrics come as misc+override pairs", pairs_ok and accon_metrics)
    counters_ok = True; steps_ok = True
    for v in accon_metrics.values():
        last = {}
        for p in (q for q in v if "misc" in q["mes"]):
            for r in p["data"]:
                if r["z"].endswith("_RH"):
                    if r["z"] in last and r["v"] < last[r["z"]]:
                        counters_ok = False
                    if r["z"] in last and (r["v"] - last[r["z"]]) % 5:
                        steps_ok = False
                    last[r["z"]] = r["v"]
    check("B5 run-hour counters never go backwards and move in steps of 5", counters_ok and steps_ok)
    alerts = defaultdict(list)
    for ts_, tp, pl in msgs:
        if tp.endswith("/alert"):
            alerts[tp.split("/")[2]].append((ts_, json.loads(pl)))
    paired = [v for v in alerts.values() if len(v) >= 2]
    off_first = all(v[0][1]["data"][0]["v"] == 0 and v[1][1]["data"][0]["v"] == 1 and v[0][1]["data"][0]["z"] != v[1][1]["data"][0]["z"]
                    for v in paired)
    check("B6 relay changes arrive as pairs, the relay going OFF first, on two different relays", paired and off_first,
          f"{len(alerts)} devices raised alerts, {sum(len(v) for v in alerts.values())} alerts")
    check("B7 no clock faults by default: every ts parses and is current", all(re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", json.loads(m[2])["ts"]) and
          abs(time.mktime(time.strptime(json.loads(m[2])["ts"], "%Y-%m-%d %H:%M:%S")) - m[0]) < 5 for m in msgs[:2000]))
    rc = run_receiver_check([(m[1], m[2]) for m in msgs])
    if rc is None:
        print("SKIP receiver acceptance (../mqtt_rmq_receiver not found)")
    else:
        bad = {k: v for k, v in rc.get("statuses", {}).items() if k in ("REJECTED", "FAILED", "IGNORED", "RETRY", "TS_FIXED")}
        check("B8 the real receiver accepts every message: no REJECTED / FAILED / IGNORED / TS_FIXED", bool(rc.get("n")) and rc.get("n") == len(msgs) == rc.get("acks") and not bad,
              f"{rc.get('n')} fed, {rc.get('acks')} acked, problems: {bad or 'none'} {rc.get('error', '')}")

    # ---- C. shared connections -----------------------------------------------------------------------------
    box.clear()
    before = len(getattr(broker, "_sessions", {}))
    cfg = make_cfg(ACCON_COUNT=300, CO2TH_COUNT=300, MQTT_CONNECTION_MODE="shared", MQTT_SHARED_CONNECTIONS=5)
    devices, pool, runner = await run_sim(cfg, 8)
    seen = {tp.split("/")[2] for _, tp, _ in box}
    check("C1 shared mode: 600 devices carried by 5 connections", len(seen) == 600 and pool.total == 5, f"{len(seen)}/600 devices, {pool.total} connections")

    # ---- D. protocol variants ------------------------------------------------------------------------------
    box.clear()
    cfg = make_cfg(ACCON_COUNT=0, CO2TH_COUNT=50, CO2TH_PROTOCOL="iesmq-v1")
    await run_sim(cfg, 5)
    chans = Counter(tp.split("/")[3] for _, tp, _ in box)
    check("D1 CO2TH_PROTOCOL=iesmq-v1 publishes /data (not /metrics) with pv", chans["data"] > 0 and chans["metrics"] == 0 and all(json.loads(p)["pv"] == "iesmq-v1" for _, tp, p in box if tp.endswith("/data")), str(dict(chans)))
    rc = run_receiver_check([(tp, p) for _, tp, p in box])
    if rc:
        bad = {k: v for k, v in rc.get("statuses", {}).items() if k in ("REJECTED", "FAILED", "IGNORED", "RETRY", "TS_FIXED")}
        check("D2 the receiver accepts the newer-firmware messages too", bool(rc.get("n")) and rc.get("n") == rc.get("acks") and not bad, str(rc))

    # ---- E. fault injection --------------------------------------------------------------------------------
    box.clear()
    cfg = make_cfg(ACCON_COUNT=100, CO2TH_COUNT=100, BAD_TS_PERCENT=20, DUPLICATE_PERCENT=10)
    await run_sim(cfg, 8)
    stamps = [json.loads(p)["ts"] for _, _, p in box]
    bad_ts = [s for s in stamps if not re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", s) or s.startswith("2000") or s.endswith(":60")]
    frac = len(bad_ts) / max(len(stamps), 1)
    check("E1 BAD_TS_PERCENT=20: about 20% of messages carry a faulty timestamp (garbled / RTC reset / empty / :60)", 0.12 < frac < 0.30, f"{frac:.0%} of {len(stamps)}")
    payloads = Counter((tp, p) for _, tp, p in box)
    dups = sum(1 for v in payloads.values() if v > 1)
    check("E2 DUPLICATE_PERCENT=10: identical messages are delivered twice", dups > 0, f"{dups} duplicated messages")
    rc = run_receiver_check([(tp, p) for _, tp, p in box])
    if rc:
        check("E3 the receiver repairs the faulty timestamps instead of dropping the messages (all acked, none REJECTED)", bool(rc.get("n")) and rc.get("n") == rc.get("acks") and not rc.get("statuses", {}).get("REJECTED") and rc.get("statuses", {}).get("TS_FIXED", 0) > 0, str(rc))

    # ---- F. reconnect after a broker restart ---------------------------------------------------------------
    box.clear()
    cfg = make_cfg(ACCON_COUNT=50, CO2TH_COUNT=50, MQTT_RECONNECT_DELAY=1)
    devices = build_fleet(cfg); pool = ClientPool(cfg, devices); runner = Runner(cfg, devices, pool); stop = asyncio.Event()
    pool.start(); await pool.wait_connected(timeout=20)
    task = asyncio.ensure_future(runner.run(stop)); await asyncio.sleep(3)
    n_before = len(box)
    await col.disconnect(); await broker.shutdown(); await asyncio.sleep(2.5)       # the broker is gone for a while
    down_connected = pool.connected_count
    broker = await start_broker(); box.clear(); col = await collector(box)
    await asyncio.sleep(9)
    stop.set(); await task
    check("F1 broker restart: clients notice the outage, reconnect on their own and publishing resumes",
          n_before > 0 and down_connected < 100 and pool.connected_count >= 95 and len(box) > 0,
          f"before={n_before} msgs, connected while down={down_connected}, after restart connected={pool.connected_count}/100, {len(box)} msgs received")
    await pool.stop()

    await col.disconnect(); await broker.shutdown()

    # ---- H. TLS and credentials ----------------------------------------------------------------------------
    tmp = tempfile.mkdtemp()
    crt, key = os.path.join(tmp, "broker.crt"), os.path.join(tmp, "broker.key")
    gen = subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", crt, "-days", "2",
                          "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"], capture_output=True)
    if gen.returncode != 0:
        print("SKIP TLS tests (openssl could not create a test certificate)")
    else:
        tport = PORT + 1
        tls_broker = Broker({"listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{tport}", "ssl": True, "certfile": crt, "keyfile": key}},
                             "plugins": {"amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True}}})
        await tls_broker.start()
        tbox = []
        tcol = Client("tls-collector"); tcol.on_message = lambda c, t, p, q, pr: tbox.append(t) or 0
        import ssl as _s
        _ctx = _s.create_default_context(cafile=crt)
        await tcol.connect("127.0.0.1", tport, ssl=_ctx, version=MQTTv311); tcol.subscribe("IES/#")
        small = dict(ACCON_COUNT=20, CO2TH_COUNT=20, MQTT_PORT=tport, MQTT_TLS="true", MQTT_CONNECT_RATE=200)
        await run_sim(make_cfg(MQTT_TLS_CA_CERT=crt, **small), 4)
        check("H1 MQTT_TLS=true + MQTT_TLS_CA_CERT: a verified TLS connection publishes", len(tbox) > 0, f"{len(tbox)} messages through TLS")
        tbox.clear()
        d, p, r = await run_sim(make_cfg(MQTT_TLS_INSECURE="true", **small), 4)
        check("H2 MQTT_TLS_INSECURE=true: connects to a self-signed broker without the CA", len(tbox) > 0, f"{len(tbox)} messages")
        tbox.clear()
        cfg_bad = make_cfg(MQTT_RECONNECT_DELAY=1, **small)
        devs = build_fleet(cfg_bad); pl = ClientPool(cfg_bad, devs); pl.start(); await asyncio.sleep(3.5); await pl.stop()
        check("H3 TLS on but the broker certificate is not trusted: refused, nothing published, failures reported", pl.connected_count == 0 and pl.connect_failures > 0 and not tbox, f"connected={pl.connected_count}, failures={pl.connect_failures}")
        await tcol.disconnect(); await tls_broker.shutdown()
    seen_auth = []
    from gmqtt import Client as _GC
    _orig = _GC.set_auth_credentials
    _GC.set_auth_credentials = lambda self, u, pw=None: (seen_auth.append((u, pw)), _orig(self, u, pw))[1]
    broker2 = await start_broker()
    cfgu = make_cfg(ACCON_COUNT=3, CO2TH_COUNT=0, MQTT_USERNAME="simuser", MQTT_PASSWORD="s3cr3t")
    pl = ClientPool(cfgu, build_fleet(cfgu)); pl.start(); await pl.wait_connected(timeout=10); await pl.stop(); await broker2.shutdown()
    _GC.set_auth_credentials = _orig
    check("H4 MQTT_USERNAME / MQTT_PASSWORD are sent by every connection", len(seen_auth) == 3 and set(seen_auth) == {("simuser", "s3cr3t")})

    # ---- G. configuration ----------------------------------------------------------------------------------
    def cli(env):
        r = subprocess.run([sys.executable, os.path.join(ROOT, "main.py"), "--dry-run", "1"], capture_output=True, text=True,
                           env=dict(os.environ, **env), cwd=tempfile.mkdtemp(), timeout=60)
        return r.returncode, (r.stdout + r.stderr)
    # a .env next to main.py would be loaded: run from a scratch copy without one
    for name, env, needle in (("bad number", {"MQTT_PORT": "abc"}, "MQTT_PORT"), ("bad protocol", {"MQTT_PROTOCOL": "9"}, "MQTT_PROTOCOL"),
                              ("both fleets 0", {"ACCON_COUNT": "0", "CO2TH_COUNT": "0"}, "nothing to simulate"),
                              ("cert without key", {"MQTT_TLS_CLIENT_CERT": "/x"}, "MQTT_TLS_CLIENT_KEY"),
                              ("bad serial batch", {"SERIAL_BATCH": "12"}, "SERIAL_BATCH"), ("missing TLS file", {"MQTT_TLS_CA_CERT": "/nope.pem"}, "TLS file not found")):
        code, out = cli(env)
        check(f"G  config error is caught at start-up: {name}", code == 2 and needle in out, f"exit {code}")
    import ssl as _ssl
    insecure = build_ssl_context(make_cfg(MQTT_TLS="true", MQTT_TLS_INSECURE="true"))
    strict = build_ssl_context(make_cfg(MQTT_TLS="true"))
    check("G  TLS contexts: off -> False, insecure -> no verification, default -> verified",
          build_ssl_context(make_cfg()) is False and insecure.verify_mode == _ssl.CERT_NONE and strict.verify_mode == _ssl.CERT_REQUIRED)

    print(f"\n{sum(results)}/{len(results)} checks passed")
    return all(results)


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(main()) else 1)
