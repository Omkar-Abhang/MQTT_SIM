"""The scheduler: one heap of (due time, device, what) drives every device from a single loop.

A heap beats one asyncio task per device at this scale: thousands of timers cost nothing, ordering is exact, and
there is one place to notice that the machine cannot keep up."""
import asyncio
import heapq
import itertools
import random
import time
from collections import Counter

from loguru import logger


class Runner:
    def __init__(self, cfg, devices, pool):
        self.cfg = cfg
        self.devices = devices
        self.pool = pool
        self.rng = random.Random(cfg.seed)
        self.stats = Counter()
        self.max_lag = 0.0
        self._heap = []
        self._seq = itertools.count()
        self._scale = cfg.interval_scale
        self._mi = cfg.metrics_interval / self._scale
        self._hi = cfg.hb_interval / self._scale
        self.t0 = 0.0

    def _push(self, due, kind, idx, extra=None):
        heapq.heappush(self._heap, (due, next(self._seq), kind, idx, extra))

    def _schedule_initial(self, t0):
        """Every device starts at a random phase of its own cycle, as a real fleet does (no thundering herd)."""
        for idx, d in enumerate(self.devices):
            self._push(t0 + self.rng.uniform(0, self._mi), "metrics", idx)
            self._push(t0 + self.rng.uniform(0, self._hi), "hb", idx)
            if getattr(d, "alternating", False):
                self._push(t0 + self.rng.uniform(0, d.period_min * 60 / self._scale), "toggle", idx)

    def _emit(self, idx, channel, payload):
        d = self.devices[idx]
        if self.pool.publish(idx, d.topic(channel), payload):
            self.stats[(d.TYPE, channel)] += 1
            if self.cfg.duplicate_percent and self.rng.random() * 100 < self.cfg.duplicate_percent:
                if self.pool.publish(idx, d.topic(channel), payload):      # a duplicate delivery, as seen in the field
                    self.stats["duplicates"] += 1
        else:
            self.stats["skipped_not_connected"] += 1

    def _handle(self, event, now):
        due, _, kind, idx, extra = event
        d = self.devices[idx]
        vm = (now - self.t0) * self._scale / 60.0               # the device's own clock, in minutes
        jitter = self.rng.uniform(-1, 1) / self._scale
        if kind == "metrics":
            for channel, payload in d.metrics(vm):
                self._emit(idx, channel, payload)
            self._push(due + self._mi + jitter, "metrics", idx)
        elif kind == "hb":
            for channel, payload in d.hb():
                self._emit(idx, channel, payload)
            self._push(due + self._hi + jitter, "hb", idx)
        elif kind == "toggle":
            for delay, channel, payload in d.toggle(vm):
                if delay <= 0:
                    self._emit(idx, channel, payload)
                else:
                    self._push(now + delay / self._scale, "emit", idx, (channel, payload))
            self._push(due + d.period_min * 60 / self._scale, "toggle", idx)
        else:                                                    # "emit": the second half of a relay pair
            self._emit(idx, *extra)

    async def run(self, stop):
        self.t0 = time.monotonic()
        self._schedule_initial(self.t0)
        deadline = self.t0 + self.cfg.duration if self.cfg.duration else None
        while not stop.is_set():
            now = time.monotonic()
            if deadline and now >= deadline:
                break
            due = self._heap[0][0] if self._heap else now + 1
            if due > now:
                wait = min(due - now, 0.5, (deadline - now) if deadline else 0.5)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=max(wait, 0.001))
                except asyncio.TimeoutError:
                    pass
                continue
            self.max_lag = max(self.max_lag, now - due)
            for _ in range(500):                                  # a batch, then let the network tasks run
                if not self._heap or self._heap[0][0] > now:
                    break
                self._handle(heapq.heappop(self._heap), now)
            await asyncio.sleep(0)

    async def report(self, stop):
        """One status line per stats_interval."""
        last_total, last_time = 0, time.monotonic()
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.cfg.stats_interval)
                return
            except asyncio.TimeoutError:
                pass
            sent = sum(v for k, v in self.stats.items() if isinstance(k, tuple))
            now = time.monotonic()
            rate = (sent - last_total) / max(now - last_time, 0.001)
            last_total, last_time = sent, now
            parts = " ".join(f"{t}/{c}={self.stats[(t, c)]}" for t, c in sorted(k for k in self.stats if isinstance(k, tuple)))
            extra = "".join(f" {name}={self.stats[name]}" for name in ("skipped_not_connected", "duplicates") if self.stats[name])
            if self.pool.publish_errors:
                extra += f" publish_errors={self.pool.publish_errors}"
            behind = f" BEHIND SCHEDULE by up to {self.max_lag:.1f}s: too many devices for this machine/scale" if self.max_lag > 5 else ""
            logger.info(f"connected {self.pool.connected_count}/{self.pool.total} | sent {sent:,} ({rate:.0f}/s) {parts}{extra}{behind}")
            self.max_lag = 0.0
