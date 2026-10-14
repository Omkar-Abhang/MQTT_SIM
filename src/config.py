"""All settings, read from the environment (a .env next to main.py is loaded first) and overridable from the
command line. Every setting is validated up front so a typo fails at start-up, not after a minute of publishing."""
import os
from dataclasses import dataclass


def _str(name, default=""):
    return os.getenv(name, default).strip()


def _bool(name, default):
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    if v.strip().lower() in ("1", "true", "yes", "on"):
        return True
    if v.strip().lower() in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"{name}={v!r}: expected true or false")


def _num(name, default, cast=int, minimum=None, maximum=None):
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = cast(raw.strip())
    except ValueError:
        raise ValueError(f"{name}={raw!r}: expected a number")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name}={value}: must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name}={value}: must be <= {maximum}")
    return value


@dataclass
class Config:
    # --- MQTT broker -------------------------------------------------------------------------------------
    host: str
    port: int
    username: str
    password: str
    protocol: str             # "3.1.1" (what the real ESP devices speak) or "5"
    keepalive: int
    qos: int                  # real devices publish with QoS 0
    clean_session: bool
    client_id_prefix: str     # client id = prefix + MAC, like the real devices ("IES-84:1F:E8:...")
    topic_prefix: str         # "IES" -> IES/<TYPE>/<SERIAL>/<channel>
    # --- TLS ---------------------------------------------------------------------------------------------
    tls: bool
    tls_ca_cert: str
    tls_client_cert: str
    tls_client_key: str
    tls_insecure: bool        # encrypt but do not verify the server certificate
    # --- connections -------------------------------------------------------------------------------------
    connection_mode: str      # per_device (one connection per simulated device) | shared
    shared_connections: int
    connect_rate: float       # new connections per second while ramping up
    reconnect_delay: int
    # --- fleet -------------------------------------------------------------------------------------------
    accon_count: int
    co2th_count: int
    serial_batch: str         # the YYMM part of the serial. 9999 can never be a real month: fake devices stand out
    accon_firmware: str
    co2th_firmware: str
    co2th_protocol: str       # legacy (/metrics) | iesmq-v1 (the newer firmware: /data, richer heartbeat)
    # --- timing ------------------------------------------------------------------------------------------
    metrics_interval: float
    hb_interval: float
    interval_scale: float     # 10 = everything happens 10x faster (load testing)
    duration: float           # 0 = run until stopped
    # --- faults, to exercise the receiver ----------------------------------------------------------------
    bad_ts_percent: float
    duplicate_percent: float
    seed: int
    stats_interval: float


def load(args=None):
    """Build the Config from the environment, then apply command-line overrides (args = argparse Namespace)."""
    cfg = Config(
        host=_str("MQTT_HOST", "127.0.0.1"),
        port=_num("MQTT_PORT", 1883, minimum=1, maximum=65535),
        username=_str("MQTT_USERNAME"),
        password=os.getenv("MQTT_PASSWORD", ""),
        protocol=_str("MQTT_PROTOCOL", "3.1.1"),
        keepalive=_num("MQTT_KEEPALIVE", 60, minimum=5),
        qos=_num("MQTT_QOS", 0, minimum=0, maximum=1),
        clean_session=_bool("MQTT_CLEAN_SESSION", True),
        client_id_prefix=_str("MQTT_CLIENT_ID_PREFIX", "IES-"),
        topic_prefix=_str("MQTT_TOPIC_PREFIX", "IES").strip("/"),
        tls=_bool("MQTT_TLS", False),
        tls_ca_cert=_str("MQTT_TLS_CA_CERT"),
        tls_client_cert=_str("MQTT_TLS_CLIENT_CERT"),
        tls_client_key=_str("MQTT_TLS_CLIENT_KEY"),
        tls_insecure=_bool("MQTT_TLS_INSECURE", False),
        connection_mode=_str("MQTT_CONNECTION_MODE", "per_device"),
        shared_connections=_num("MQTT_SHARED_CONNECTIONS", 20, minimum=1),
        connect_rate=_num("MQTT_CONNECT_RATE", 100.0, cast=float, minimum=0.1),
        reconnect_delay=_num("MQTT_RECONNECT_DELAY", 5, minimum=1),
        accon_count=_num("ACCON_COUNT", 1000, minimum=0, maximum=99999),
        co2th_count=_num("CO2TH_COUNT", 1000, minimum=0, maximum=99999),
        serial_batch=_str("SERIAL_BATCH", "9999"),
        accon_firmware=_str("ACCON_FIRMWARE", "1.3.4-R"),
        co2th_firmware=_str("CO2TH_FIRMWARE", ""),
        co2th_protocol=_str("CO2TH_PROTOCOL", "legacy"),
        metrics_interval=_num("METRICS_INTERVAL_SECONDS", 60.0, cast=float, minimum=1),
        hb_interval=_num("HB_INTERVAL_SECONDS", 900.0, cast=float, minimum=1),
        interval_scale=_num("INTERVAL_SCALE", 1.0, cast=float, minimum=0.01),
        duration=_num("DURATION_SECONDS", 0.0, cast=float, minimum=0),
        bad_ts_percent=_num("BAD_TS_PERCENT", 0.0, cast=float, minimum=0, maximum=100),
        duplicate_percent=_num("DUPLICATE_PERCENT", 0.0, cast=float, minimum=0, maximum=100),
        seed=_num("RANDOM_SEED", 1),
        stats_interval=_num("STATS_INTERVAL_SECONDS", 10.0, cast=float, minimum=1),
    )
    if args is not None:                       # command line wins over the environment
        for field, attr in (("accon", "accon_count"), ("co2th", "co2th_count"), ("duration", "duration"),
                            ("scale", "interval_scale"), ("mode", "connection_mode")):
            value = getattr(args, field, None)
            if value is not None:
                setattr(cfg, attr, value)
    if cfg.protocol not in ("3.1.1", "5"):
        raise ValueError(f"MQTT_PROTOCOL={cfg.protocol!r}: use 3.1.1 or 5")
    if cfg.connection_mode not in ("per_device", "shared"):
        raise ValueError(f"MQTT_CONNECTION_MODE={cfg.connection_mode!r}: use per_device or shared")
    if cfg.co2th_protocol not in ("legacy", "iesmq-v1"):
        raise ValueError(f"CO2TH_PROTOCOL={cfg.co2th_protocol!r}: use legacy or iesmq-v1")
    if not (cfg.serial_batch.isdigit() and len(cfg.serial_batch) == 4):
        raise ValueError(f"SERIAL_BATCH={cfg.serial_batch!r}: expected 4 digits (YYMM)")
    if not cfg.co2th_firmware:                 # the firmware that matches the chosen protocol, as seen in the field
        cfg.co2th_firmware = "1.0.2" if cfg.co2th_protocol == "iesmq-v1" else "1.0.1"
    if cfg.tls_client_cert and not cfg.tls_client_key:
        raise ValueError("MQTT_TLS_CLIENT_CERT is set but MQTT_TLS_CLIENT_KEY is not")
    if cfg.accon_count + cfg.co2th_count == 0:
        raise ValueError("ACCON_COUNT and CO2TH_COUNT are both 0: nothing to simulate")
    return cfg
