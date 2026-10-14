"""Simulated ACCON and CO2TH devices.

Message shapes, field order and value types are copied from real captured traffic (244k messages, 217 devices)
and from the firmware protocol documents, so the receiver sees what a real fleet sends:

  ACCON  metrics every 60 s as TWO messages (misc + temperature, then override), hb every 15 min, and an alert
         for every relay change. Relays alternate in pairs (one goes off, the other on 1-2 s later).
  CO2TH  metrics every 60 s (temperature, humidity, co2), hb every 15 min. CO2TH_PROTOCOL=iesmq-v1 gives the
         newer firmware: /data instead of /metrics and a richer heartbeat.

A device only builds messages. Sending and timing belong to runner.py / mqtt_pool.py.
"""
import json
import random
from datetime import datetime


def dumps(obj):
    """Compact JSON, like the firmware's serializer."""
    return json.dumps(obj, separators=(",", ":"))


def _num(x):
    """The firmware prints 27.0 as 27. Do the same so payloads are byte-for-byte alike."""
    return int(x) if isinstance(x, float) and x == int(x) else x


def wall_ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def faulty_ts(rng):
    """The clock faults seen on real devices: an RTC reset, a garbled value, no clock at all, seconds = 60."""
    kind = rng.randrange(4)
    if kind == 0:
        return "2000-01-01 00:00:%02d" % rng.randint(0, 5)
    if kind == 1:
        return "2040-104-120 163:%02d:00" % rng.choice([51, 55])
    if kind == 2:
        return ""
    return datetime.now().strftime("%Y-%m-%d %H:%M:") + "60"


class Device:
    TYPE = ""
    MAC_PREFIX = ""          # locally administered (02:..), so a simulated MAC can never be a real vendor MAC

    def __init__(self, index, cfg):
        self.cfg = cfg
        self.index = index
        self.rng = random.Random(f"{cfg.seed}:{self.TYPE}:{index}")      # same seed -> same fleet, every run
        self.serial = f"{self.SERIAL_PREFIX}-{cfg.serial_batch}-{index:04d}"
        self.gwid = f"{cfg.serial_batch}{index:04d}"                      # real units: gwid = the serial's digits
        self.mac = "%s:%02X:%02X:%02X" % (self.MAC_PREFIX, (index >> 16) & 255, (index >> 8) & 255, index & 255)
        self.client_id = f"{cfg.client_id_prefix}{self.mac}"
        self.ip = "10.1.%d.%d" % (self.rng.randint(0, 254), self.rng.randint(1, 254))
        self.dbm = self.rng.randint(-85, -55)
        self.cpt = self.rng.randint(20, 60)

    # ---- helpers ------------------------------------------------------------------------------------------
    def topic(self, channel):
        return f"{self.cfg.topic_prefix}/{self.TYPE}/{self.serial}/{channel}"

    def _ts(self):
        if self.cfg.bad_ts_percent and self.rng.random() * 100 < self.cfg.bad_ts_percent:
            return faulty_ts(self.rng)
        return wall_ts()

    def _header(self, ts, ver, pv=False):
        head = {"pv": "iesmq-v1"} if pv else {}
        head.update({"ts": ts, "model": self.TYPE, "ver": ver, "srno": self.serial, "mac": self.mac,
                     "gwid": self.gwid, "devid": 0})
        return head

    def _walk_wifi(self):
        self.dbm = max(-88, min(-50, self.dbm + self.rng.randint(-2, 2)))
        self.cpt = max(15, min(65, self.cpt + self.rng.choice([-1, 0, 0, 1])))


class Accon(Device):
    TYPE = "ACCON"
    SERIAL_PREFIX = "IACCON"
    MAC_PREFIX = "02:AC:00"

    def __init__(self, index, cfg):
        super().__init__(index, cfg)
        r = self.rng
        self.ver = cfg.accon_firmware
        self.n = 2 if r.random() < 0.01 else 6          # a few real units have only 2 relays
        self.state = [0] * self.n
        self.ovr = [0] * self.n
        self.on_minutes = [0.0] * self.n
        self.last_vm = 0.0                              # virtual minute of the last counter update
        self.rh_base = [(r.randint(0, 30000) // 5) * 5 if k < 2 else 0 for k in range(self.n)]
        roll = r.random()
        self.alternating = self.n >= 2 and roll < 0.40  # ~40% of units run a relay schedule, the rest sit idle
        self.period_min = r.choice([15, 30, 60]) if self.alternating else 0
        if roll >= 0.99:                                # ~1% have a relay forced by an override
            self.ovr[0] = r.choice([1, 2])
            self.state[0] = 1 if self.ovr[0] == 1 else 0
        if self.alternating:
            self.state[0] = r.randint(0, 1)
            self.state[1] = 1 - self.state[0]
        self.temp = r.uniform(22, 32)

    # ---- relay model --------------------------------------------------------------------------------------
    def _accumulate(self, vm):
        delta = max(0.0, vm - self.last_vm)
        for k in range(self.n):
            if self.state[k]:
                self.on_minutes[k] += delta
        self.last_vm = vm

    def _rh(self, k):
        return self.rh_base[k] + int(self.on_minutes[k] // 5) * 5      # the counter moves in steps of 5 minutes

    def toggle(self, vm):
        """The alternating pair flips. Returns [(delay_s, channel, payload)]: the relay going OFF reports first,
        the one going ON 1-2 s later, as on real units."""
        self._accumulate(vm)
        new_a = 1 - self.state[0]
        changes = sorted([(0, new_a), (1, 1 - new_a)], key=lambda c: c[1])      # OFF (0) before ON (1)
        out, delay = [], 0.0
        for k, value in changes:
            self.state[k] = value
            payload = self._header(self._ts(), self.ver)
            payload["mes"] = {"misc": [f"RLY{k + 1}"]}
            payload["data"] = [{"z": f"RLY{k + 1}", "v": value}]
            out.append((delay, "alert", dumps(payload)))
            delay += 1.0 + self.rng.random()
        return out

    # ---- messages -----------------------------------------------------------------------------------------
    def metrics(self, vm):
        self._accumulate(vm)
        self.temp = max(15.0, min(42.0, self.temp + self.rng.gauss(0, 0.1)))
        ts = self._ts()
        rl = [f"RLY{k + 1}" for k in range(self.n)]
        rh = [f"R{k + 1}_RH" for k in range(self.n)]
        orh = [f"R{k + 1}_ORH" for k in range(self.n)]
        first = self._header(ts, self.ver)
        first["mes"] = {"misc": rl + rh + orh, "temperature": ["TEMP1"]}
        first["data"] = ([{"z": rl[k], "v": self.state[k]} for k in range(self.n)]
                         + [{"z": rh[k], "v": self._rh(k)} for k in range(self.n)]
                         + [{"z": orh[k], "v": 0} for k in range(self.n)]
                         + [{"z": "TEMP1", "v": _num(round(self.temp * 16) / 16)}])      # DS18B20: 1/16 degree steps
        second = self._header(ts, self.ver)
        second["mes"] = {"override": rl}
        second["data"] = [{"z": rl[k], "v": self.ovr[k]} for k in range(self.n)]
        return [("metrics", dumps(first)), ("metrics", dumps(second))]

    def hb(self):
        self._walk_wifi()
        payload = self._header(self._ts(), self.ver)
        payload["s_data"] = [{"z": "DBM", "v": self.dbm}, {"z": "DIP", "v": self.ip}, {"z": "CPT", "v": self.cpt}]
        payload["d_data"] = [{"devid0": 1006}]
        return [("hb", dumps(payload))]


class Co2th(Device):
    TYPE = "CO2TH"
    SERIAL_PREFIX = "CO2TH"
    MAC_PREFIX = "02:C2:00"

    def __init__(self, index, cfg):
        super().__init__(index, cfg)
        r = self.rng
        self.ver = cfg.co2th_firmware
        self.v1 = cfg.co2th_protocol == "iesmq-v1"
        self.co2 = r.uniform(420, 1500)
        self.temp1 = r.uniform(24, 31)
        self.hum1 = r.uniform(40, 70)

    def metrics(self, vm):
        r = self.rng
        self.co2 = max(400.0, min(3000.0, self.co2 + r.gauss(0, 15)))
        self.temp1 = max(15.0, min(42.0, self.temp1 + r.gauss(0, 0.05)))
        self.hum1 = max(15.0, min(90.0, self.hum1 + r.gauss(0, 0.2)))
        temp2 = self.temp1 - r.uniform(0.8, 1.6)
        hum2 = self.hum1 + r.uniform(1.0, 6.0)
        co2_raw = self.co2 * r.uniform(1.02, 1.08) + r.gauss(0, 10)
        data = [{"z": "CO2", "v": int(round(self.co2))}, {"z": "CO2_RAW", "v": int(round(co2_raw))},
                {"z": "TEMP1", "v": _num(round(self.temp1, 8))}, {"z": "HUMI1", "v": _num(round(self.hum1, 8))},
                {"z": "TEMP2", "v": _num(round(temp2, 8))}, {"z": "HUMI2", "v": _num(round(hum2, 8))}]
        mes = {"temperature": ["TEMP1", "TEMP2"], "humidity": ["HUMI1", "HUMI2"], "co2": ["CO2", "CO2_RAW"]}
        if not self.v1:
            payload = self._header(self._ts(), self.ver)
            payload["mes"] = mes
            payload["data"] = data
            return [("metrics", dumps(payload))]
        # newer firmware: /data, IO states included, its own key order
        payload = self._header(self._ts(), self.ver, pv=True)
        payload["data"] = data + [{"z": "IN1STA", "v": 0}, {"z": "IN2STA", "v": 0},
                                  {"z": "OUT1STA", "v": 0}, {"z": "OUT2STA", "v": 0}]
        payload["mes"] = {"co2": mes["co2"], "temperature": mes["temperature"], "humidity": mes["humidity"],
                          "misc": ["IN1STA", "IN2STA", "OUT1STA", "OUT2STA"]}
        return [("data", dumps(payload))]

    def hb(self):
        self._walk_wifi()
        ts = self._ts()
        if not self.v1:
            payload = self._header(ts, self.ver)
            payload["s_data"] = [{"z": "DBM", "v": self.dbm}, {"z": "CPT", "v": self.cpt}]
            payload["d_data"] = [{}] if self.rng.random() < 0.015 else []   # 1.5% of real heartbeats carry [{}]
            return [("hb", dumps(payload))]
        payload = self._header(ts, self.ver, pv=True)
        payload["s_data"] = [{"z": "DBM", "v": self.dbm}, {"z": "DIP", "v": self.ip}, {"z": "CPT", "v": self.cpt},
                             {"z": "ESPDT", "v": wall_ts()}, {"z": "WiFiSTA", "v": 3}, {"z": "MQTTSTA", "v": 0},
                             {"z": "RST", "v": "SW"}]
        payload["d_data"] = [{"devid0": 1006}]
        return [("hb", dumps(payload))]


def build_fleet(cfg):
    """ACCON devices first, then CO2TH. Same config + seed = the same fleet every time."""
    return ([Accon(i, cfg) for i in range(1, cfg.accon_count + 1)]
            + [Co2th(i, cfg) for i in range(1, cfg.co2th_count + 1)])
