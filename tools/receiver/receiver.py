#!/usr/bin/env python3
"""幂等业务接收端：事务落库后发布业务 ACK，并可注入 ACK 丢失。"""

import argparse
import asyncio
import json
import pathlib
import sqlite3
import struct
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from mqtt_wire import connect, packet, parse_publish, publish, read_packet, subscribe


SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS events(
 gateway_id TEXT NOT NULL, store_epoch TEXT NOT NULL, event_seq INTEGER NOT NULL,
 device_id TEXT NOT NULL, received_at_ms INTEGER NOT NULL,
 PRIMARY KEY(gateway_id,store_epoch,event_seq));
CREATE TABLE IF NOT EXISTS readings(
 gateway_id TEXT NOT NULL, store_epoch TEXT NOT NULL, event_seq INTEGER NOT NULL,
 point_key TEXT NOT NULL, value REAL NOT NULL, unit TEXT NOT NULL, quality TEXT NOT NULL,
 PRIMARY KEY(gateway_id,store_epoch,event_seq,point_key));
CREATE TABLE IF NOT EXISTS deliveries(
 gateway_id TEXT NOT NULL, store_epoch TEXT NOT NULL, event_seq INTEGER NOT NULL,
 attempts INTEGER NOT NULL,
 PRIMARY KEY(gateway_id,store_epoch,event_seq));
"""


def save_event(database, event):
    """在同一事务内记录投递次数，并以业务唯一键幂等保存事件和读数。"""
    required = {"schema_version", "gateway_id", "store_epoch", "event_seq", "device_id", "received_at_ms", "measurements"}
    if not required.issubset(event) or event["schema_version"] != 1 or len(json.dumps(event)) > 8192:
        raise ValueError("invalid or unsupported event")
    # context manager 在成功时提交、异常时回滚；ACK 必须在本事务完成后发送。
    with database:
        database.execute(
            "INSERT INTO deliveries VALUES(?,?,?,1) "
            "ON CONFLICT(gateway_id,store_epoch,event_seq) "
            "DO UPDATE SET attempts=attempts+1",
            (event["gateway_id"], event["store_epoch"], event["event_seq"]))
        database.execute("INSERT OR IGNORE INTO events VALUES(?,?,?,?,?)",
                         (event["gateway_id"], event["store_epoch"], event["event_seq"],
                          event["device_id"], event["received_at_ms"]))
        for item in event["measurements"]:
            database.execute("INSERT OR IGNORE INTO readings VALUES(?,?,?,?,?,?,?)",
                             (event["gateway_id"], event["store_epoch"], event["event_seq"],
                              item["point_key"], item["value"], item["unit"], item["quality"]))


async def run(args):
    database = sqlite3.connect(args.database)
    database.executescript(SCHEMA)
    packet_id = 1
    backoff = 0.5
    dropped_once = set()
    while True:
        writer = None
        try:
            reader, writer = await asyncio.open_connection(args.host, args.port)
            writer.write(connect("edgelink-receiver")); await writer.drain()
            header, body = await read_packet(reader)
            if header >> 4 != 2 or body != b"\x00\x00":
                raise ValueError("broker rejected CONNECT")
            topic = f"edgelink/{args.gateway_id}/events"
            writer.write(subscribe(packet_id, topic)); await writer.drain(); packet_id += 1
            header, _ = await read_packet(reader)
            if header >> 4 != 9:
                raise ValueError("broker rejected SUBSCRIBE")
            print(f"receiver subscribed to {topic}", flush=True)
            backoff = 0.5
            while True:
                header, body = await read_packet(reader)
                if header >> 4 != 3:
                    continue
                _, incoming_id, payload = parse_publish(header, body)
                if incoming_id is not None:
                    writer.write(packet(0x40, struct.pack("!H", incoming_id)))
                event = json.loads(payload)
                save_event(database, event)
                identity = (event["gateway_id"], event["store_epoch"], event["event_seq"])
                drop_once = (args.drop_ack_once_every and
                             event["event_seq"] % args.drop_ack_once_every == 0 and
                             identity not in dropped_once)
                if drop_once:
                    # 首次故意不发业务 ACK，后续重复投递仍会 ACK，验证幂等补传闭环。
                    dropped_once.add(identity)
                if ((not args.drop_ack_every or event["event_seq"] % args.drop_ack_every) and
                        not drop_once):
                    ack = json.dumps({"gateway_id": event["gateway_id"], "store_epoch": event["store_epoch"],
                                      "event_seq": event["event_seq"]}, separators=(",", ":")).encode()
                    packet_id = packet_id % 65535 + 1
                    writer.write(publish(packet_id, f"edgelink/{args.gateway_id}/acks", ack))
                await writer.drain()
        except (OSError, asyncio.IncompleteReadError, ValueError, json.JSONDecodeError) as error:
            print(f"receiver reconnecting after: {error}", file=sys.stderr, flush=True)
            try:
                if writer:
                    writer.close(); await writer.wait_closed()
            except Exception:
                pass
            # 接收端与网关采用相同思路做有上限的指数退避，避免故障时忙循环。
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


def main():
    parser = argparse.ArgumentParser(description="EdgeLink idempotent business receiver")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--gateway-id", default="gw-001")
    parser.add_argument("--database", default="receiver.db")
    parser.add_argument("--drop-ack-every", type=int, default=0)
    parser.add_argument("--drop-ack-once-every", type=int, default=0)
    args = parser.parse_args()
    pathlib.Path(args.database).parent.mkdir(parents=True, exist_ok=True)
    asyncio.run(run(args))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
