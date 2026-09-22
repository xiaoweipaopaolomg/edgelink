#!/usr/bin/env python3
"""确定性的 Modbus TCP 设备模拟器，支持分段和多种链路/协议故障注入。"""

import argparse
import asyncio
import random
import struct


class Simulator:
    def __init__(self, args):
        self.args = args
        self.requests = 0
        self.random = random.Random(args.seed)

    async def handle(self, reader, writer):
        try:
            while True:
                # 先按 MBAP 声明长度收齐请求，模拟真实 TCP 流而非假设一次读取一帧。
                header = await reader.readexactly(7)
                transaction, protocol, length, unit = struct.unpack("!HHHB", header)
                body = await reader.readexactly(length - 1)
                self.requests += 1
                if protocol or len(body) != 5 or body[0] != 3:
                    break
                if self.args.disconnect_every and self.requests % self.args.disconnect_every == 0:
                    break
                if self.args.drop_every and self.requests % self.args.drop_every == 0:
                    # 故意超过网关超时，用于验证超时检测和退避重连。
                    await asyncio.sleep(max(2, self.args.delay_ms / 1000))
                    continue
                await asyncio.sleep(self.args.delay_ms / 1000)
                address, count = struct.unpack("!HH", body[1:])
                if count < 1 or count > 125:
                    response_body = bytes([0x83, 3])
                elif self.args.exception_every and self.requests % self.args.exception_every == 0:
                    response_body = bytes([0x83, 4])
                else:
                    values = [(address + i + self.requests * 3 + self.random.randrange(2)) & 0xFFFF for i in range(count)]
                    response_body = bytes([3, count * 2]) + struct.pack(f"!{count}H", *values)
                declared_length = len(response_body) + 1
                if self.args.bad_length_every and self.requests % self.args.bad_length_every == 0:
                    # 声明长度与实际载荷不一致，用于触发 MBAP 长度校验。
                    declared_length = 1
                response = struct.pack("!HHHB", transaction, 0, declared_length, unit) + response_body
                if self.args.split and len(response) > 5:
                    # 随机切成两个 TCP 写入，验证网关能够缓存并重组半帧。
                    cut = self.random.randint(1, len(response) - 1)
                    writer.write(response[:cut]); await writer.drain(); await asyncio.sleep(0.01)
                    writer.write(response[cut:])
                else:
                    writer.write(response)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()


async def main():
    parser = argparse.ArgumentParser(description="Deterministic Modbus TCP 0x03 simulator")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1502)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--split", action="store_true")
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--drop-every", type=int, default=0)
    parser.add_argument("--disconnect-every", type=int, default=0)
    parser.add_argument("--exception-every", type=int, default=0)
    parser.add_argument("--bad-length-every", type=int, default=0)
    args = parser.parse_args()
    simulator = Simulator(args)
    server = await asyncio.start_server(simulator.handle, args.host, args.port)
    print(f"Modbus simulator listening on {args.host}:{args.port}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
