from dotenv import load_dotenv
load_dotenv()   # a .env next to this file; real environment variables (Docker) take precedence

import argparse
import asyncio
import os
import signal
import sys

from loguru import logger

from src import config
from src.devices import build_fleet
from src.mqtt_pool import ClientPool
from src.runner import Runner

LOCAL_HOSTS = ("localhost", "::1")


def parse_args():
    p = argparse.ArgumentParser(description="Simulate thousands of ACCON / CO2TH devices publishing to an MQTT broker. "
                                            "Settings come from .env; these flags override them.")
    p.add_argument("--accon", type=int, help="number of ACCON devices (ACCON_COUNT)")
    p.add_argument("--co2th", type=int, help="number of CO2TH devices (CO2TH_COUNT)")
    p.add_argument("--duration", type=float, help="stop after this many seconds (DURATION_SECONDS, 0 = forever)")
    p.add_argument("--scale", type=float, help="run N times faster than real time (INTERVAL_SCALE)")
    p.add_argument("--mode", choices=("per_device", "shared"), help="MQTT_CONNECTION_MODE")
    p.add_argument("--dry-run", type=int, nargs="?", const=2, metavar="N",
                   help="print what N devices of each type would send, then exit. Nothing is published")
    p.add_argument("--yes", action="store_true", help="do not pause before publishing to a non-local broker")
    return p.parse_args()


def dry_run(cfg, devices, n):
    shown = {}
    for d in devices:
        if shown.get(d.TYPE, 0) >= n:
            continue
        shown[d.TYPE] = shown.get(d.TYPE, 0) + 1
        print(f"\n=== {d.TYPE} #{d.index}  serial={d.serial}  mac={d.mac}  clientid={d.client_id}")
        for channel, payload in d.metrics(0.0) + d.hb():
            print(f"{d.topic(channel)}\n  {payload}")
        if getattr(d, "alternating", False):
            for delay, channel, payload in d.toggle(15.0):
                print(f"{d.topic(channel)}   (+{delay:.1f}s, relay change)\n  {payload}")


def estimate(cfg):
    metrics = (cfg.accon_count * 2 + cfg.co2th_count) * cfg.interval_scale / cfg.metrics_interval
    hb = (cfg.accon_count + cfg.co2th_count) * cfg.interval_scale / cfg.hb_interval
    return metrics + hb


def raise_fd_limit(needed):
    """One socket per simulated device needs a high open-files limit."""
    try:
        import resource
    except ImportError:
        return
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft >= needed:
        return
    target = needed if hard == resource.RLIM_INFINITY else min(hard, needed)
    resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    if target < needed:
        logger.warning(f"open-files limit is {target} but about {needed} are needed: use MQTT_CONNECTION_MODE=shared "
                       "or raise the limit (docker run --ulimit nofile=65536)")


async def amain(cfg, args):
    devices = build_fleet(cfg)
    pool = ClientPool(cfg, devices)
    runner = Runner(cfg, devices, pool)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:      # Windows
            pass

    tls = "TLS" if cfg.tls else "plain"
    logger.info(f"=== mqtt_device_simulator: {cfg.accon_count} ACCON + {cfg.co2th_count} CO2TH ({cfg.co2th_protocol}) "
                f"-> {cfg.host}:{cfg.port} ({tls}, MQTT {cfg.protocol}, QoS {cfg.qos})")
    logger.info(f"{pool.total} connection(s) [{cfg.connection_mode}], ~{estimate(cfg):.0f} messages/s at scale x{cfg.interval_scale:g}, "
                f"serials {devices[0].serial} .. {devices[-1].serial}, topics {cfg.topic_prefix}/<TYPE>/<SERIAL>/<channel>")
    if cfg.bad_ts_percent or cfg.duplicate_percent:
        logger.info(f"fault injection: bad timestamps {cfg.bad_ts_percent:g}%, duplicates {cfg.duplicate_percent:g}%")
    if cfg.host.lower() not in LOCAL_HOSTS and not cfg.host.startswith("127.") and not args.yes:
        logger.warning(f"{cfg.host} is not local: this will publish FAKE devices (serial batch {cfg.serial_batch}) into that "
                       "broker and everything downstream of it. Press Ctrl-C within 5 s to abort (--yes skips this pause)")
        try:
            await asyncio.wait_for(stop.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass
        if stop.is_set():
            logger.info("aborted")
            return

    if cfg.connection_mode == "per_device":
        raise_fd_limit(pool.total + 256)
    pool.start()
    if not await pool.wait_connected(timeout=pool.total / cfg.connect_rate + 60):
        logger.warning(f"only {pool.connected_count}/{pool.total} connected: starting anyway, the rest keep retrying")
    reporter = asyncio.ensure_future(runner.report(stop))
    try:
        await runner.run(stop)
    finally:
        stop.set()
        await reporter
        logger.info("stopping, disconnecting...")
        await pool.stop()
        sent = sum(v for k, v in runner.stats.items() if isinstance(k, tuple))
        logger.info(f"done: {sent:,} messages sent, {runner.stats['skipped_not_connected']:,} skipped (not connected), "
                    f"{pool.connect_failures} connect failures, {pool.publish_errors} publish errors")


def main():
    args = parse_args()
    try:
        cfg = config.load(args)
        for path in (cfg.tls_ca_cert, cfg.tls_client_cert, cfg.tls_client_key):
            if path and not os.path.isfile(path):
                raise ValueError(f"TLS file not found: {path}")
    except ValueError as e:
        logger.error(f"configuration error: {e}")
        sys.exit(2)
    if args.dry_run is not None:
        dry_run(cfg, build_fleet(cfg), args.dry_run)
        return
    asyncio.run(amain(cfg, args))


if __name__ == "__main__":
    main()
