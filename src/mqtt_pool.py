"""MQTT connections for the simulated devices (gmqtt: one asyncio event loop, no thread per device).

per_device  one connection per simulated device, client id = prefix + MAC, exactly what EMQX sees from a real
            fleet (connection count, auth, clientid in the rule output)
shared      N connections carry all devices. Same message load on the broker, a fraction of the connections
"""
import asyncio
import random
import ssl

from gmqtt import Client
from gmqtt.mqtt.constants import MQTTv311, MQTTv50
from loguru import logger


def build_ssl_context(cfg):
    """False when TLS is off, otherwise an SSLContext built from the MQTT_TLS_* settings."""
    if not cfg.tls:
        return False
    ctx = ssl.create_default_context(cafile=cfg.tls_ca_cert or None)
    if cfg.tls_insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    if cfg.tls_client_cert:
        ctx.load_cert_chain(cfg.tls_client_cert, cfg.tls_client_key)
    return ctx


class ClientPool:
    def __init__(self, cfg, devices):
        self.cfg = cfg
        self._ssl = build_ssl_context(cfg)
        self._version = MQTTv50 if cfg.protocol == "5" else MQTTv311
        if cfg.connection_mode == "per_device":
            self._ids = [d.client_id for d in devices]
            self._map = list(range(len(devices)))
        else:
            m = min(cfg.shared_connections, len(devices))
            self._ids = [f"{cfg.client_id_prefix}sim-{k:03d}" for k in range(m)]
            self._map = [i % m for i in range(len(devices))]
        self.clients = [None] * len(self._ids)
        self.connected = [False] * len(self._ids)
        self.connected_count = 0
        self.connect_failures = 0
        self.publish_errors = 0
        self._tasks = []
        self._closing = False
        self._seen_errors = set()

    @property
    def total(self):
        return len(self._ids)

    # ---- connecting ---------------------------------------------------------------------------------------
    def start(self):
        """Begin connecting in the background, `connect_rate` connections per second."""
        for k in range(self.total):
            self._tasks.append(asyncio.ensure_future(self._run_client(k, k / self.cfg.connect_rate)))

    async def wait_connected(self, fraction=0.99, timeout=120.0):
        """Wait until most connections are up, so the load starts clean instead of during the ramp-up."""
        waited, last = 0.0, -1
        while waited < timeout and self.connected_count < fraction * self.total:
            await asyncio.sleep(0.25)
            waited += 0.25
            if int(waited) % 5 == 0 and int(waited) != last:
                last = int(waited)
                logger.info(f"connecting... {self.connected_count}/{self.total}")
        return self.connected_count >= fraction * self.total

    def _on_connect(self, k):
        if not self.connected[k]:
            self.connected[k] = True
            self.connected_count += 1

    def _on_disconnect(self, k):
        if self.connected[k]:
            self.connected[k] = False
            self.connected_count -= 1

    async def _run_client(self, k, start_delay):
        await asyncio.sleep(start_delay)
        client = Client(self._ids[k], clean_session=self.cfg.clean_session)
        client.set_config({"reconnect_retries": -1, "reconnect_delay": self.cfg.reconnect_delay})
        if self.cfg.username:
            client.set_auth_credentials(self.cfg.username, self.cfg.password)
        client.on_connect = lambda c, flags, rc, props, k=k: self._on_connect(k)
        client.on_disconnect = lambda c, packet, exc=None, k=k: self._on_disconnect(k)
        self.clients[k] = client
        while not self._closing:
            try:
                await client.connect(self.cfg.host, self.cfg.port, ssl=self._ssl,
                                     keepalive=self.cfg.keepalive, version=self._version)
                self._on_connect(k)
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.connect_failures += 1
                why = f"{type(e).__name__}: {e}"[:140]
                if why not in self._seen_errors and len(self._seen_errors) < 5:   # one line per distinct reason
                    self._seen_errors.add(why)
                    logger.warning(f"connect failed ({self._ids[k]}): {why} - retrying every ~{self.cfg.reconnect_delay}s")
                await asyncio.sleep(self.cfg.reconnect_delay + random.random())

    # ---- publishing ---------------------------------------------------------------------------------------
    def publish(self, device_index, topic, payload):
        """Queue one message. False if that device's connection is down (a real offline device sends nothing)."""
        k = self._map[device_index]
        if not self.connected[k]:
            return False
        try:
            self.clients[k].publish(topic, payload.encode(), qos=self.cfg.qos)
            return True
        except Exception as e:
            self.publish_errors += 1
            why = f"publish: {type(e).__name__}: {e}"[:140]
            if why not in self._seen_errors and len(self._seen_errors) < 5:
                self._seen_errors.add(why)
                logger.warning(why)
            return False

    # ---- shutdown -----------------------------------------------------------------------------------------
    async def stop(self):
        self._closing = True
        for t in self._tasks:
            t.cancel()
        live = [c for c in self.clients if c is not None]

        async def _bye(c):
            try:
                await asyncio.wait_for(c.disconnect(), timeout=2)
            except Exception:
                pass
        await asyncio.gather(*[_bye(c) for c in live], return_exceptions=True)
