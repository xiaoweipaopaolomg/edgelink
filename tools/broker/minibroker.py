#!/usr/bin/env python3
"""用于自动化测试和演示的有界本地 Broker；生产环境应使用 Mosquitto 等成熟实现。"""
import argparse
import asyncio
import pathlib
import struct
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from mqtt_wire import mqtt_string, packet, parse_publish, publish, read_packet


class Broker:
    """只实现本项目需要的 CONNECT、SUBSCRIBE、PUBLISH、PUBACK 和心跳。"""
    def __init__(self):
        self.subscribers = {}
        self.packet_id = 1

    async def send(self, writer, data):
        writer.write(data)
        await writer.drain()

    async def handle(self, reader, writer):
        mine = set()
        try:
            while True:
                header, body = await read_packet(reader)
                kind = header >> 4
                if kind == 1:
                    await self.send(writer, packet(0x20, b"\x00\x00"))
                elif kind == 8:
                    packet_id = struct.unpack("!H", body[:2])[0]
                    topic_length = struct.unpack("!H", body[2:4])[0]
                    topic = body[4:4 + topic_length].decode()
                    self.subscribers.setdefault(topic, set()).add(writer)
                    mine.add(topic)
                    await self.send(writer, packet(0x90, struct.pack("!H", packet_id) + b"\x01"))
                elif kind == 3:
                    topic, incoming_id, payload = parse_publish(header, body)
                    # Broker 的 PUBACK 只确认收到报文，不代表业务接收端已经落库。
                    if incoming_id is not None:
                        await self.send(writer, packet(0x40, struct.pack("!H", incoming_id)))
                    for subscriber in list(self.subscribers.get(topic, ())):
                        try:
                            self.packet_id = self.packet_id % 65535 + 1
                            await self.send(subscriber, publish(self.packet_id, topic, payload))
                        except (ConnectionError, asyncio.CancelledError):
                            self.subscribers[topic].discard(subscriber)
                elif kind == 12:
                    await self.send(writer, packet(0xD0))
                elif kind in (4, 14):
                    if kind == 14:
                        break
                else:
                    raise ValueError(f"unsupported MQTT packet type {kind}")
        except (asyncio.IncompleteReadError, ConnectionError, ValueError):
            pass
        finally:
            # 连接关闭时必须移除其全部订阅，避免保留失效 writer。
            for topic in mine:
                self.subscribers.get(topic, set()).discard(writer)
            writer.close()
            await writer.wait_closed()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1883)
    args = parser.parse_args()
    broker = Broker()
    server = await asyncio.start_server(broker.handle, args.host, args.port)
    print(f"minibroker listening on {args.host}:{args.port}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
