"""EdgeLink 演示工具共用的最小 MQTT 3.1.1 编解码，不是通用客户端库。"""
import asyncio
import struct


def encode_remaining(length: int) -> bytes:
    """编码 MQTT 的 128 进制变长 Remaining Length。"""
    out = bytearray()
    while True:
        byte = length % 128
        length //= 128
        if length:
            byte |= 0x80
        out.append(byte)
        if not length:
            return bytes(out)


def mqtt_string(value: str) -> bytes:
    """编码带两字节大端长度前缀的 MQTT UTF-8 字符串。"""
    encoded = value.encode()
    if len(encoded) > 65535:
        raise ValueError("MQTT string too long")
    return struct.pack("!H", len(encoded)) + encoded


def packet(header: int, body: bytes = b"") -> bytes:
    return bytes([header]) + encode_remaining(len(body)) + body


async def read_packet(reader: asyncio.StreamReader, maximum: int = 16384):
    """读取一个完整报文，并用 maximum 限制测试工具的内存占用。"""
    header = (await reader.readexactly(1))[0]
    length = 0
    multiplier = 1
    for _ in range(4):
        byte = (await reader.readexactly(1))[0]
        length += (byte & 127) * multiplier
        if not byte & 128:
            break
        multiplier *= 128
    else:
        raise ValueError("malformed remaining length")
    if length > maximum:
        raise ValueError("MQTT packet exceeds configured bound")
    return header, await reader.readexactly(length)


def connect(client_id: str) -> bytes:
    body = mqtt_string("MQTT") + b"\x04\x02" + struct.pack("!H", 30) + mqtt_string(client_id)
    return packet(0x10, body)


def subscribe(packet_id: int, topic: str) -> bytes:
    return packet(0x82, struct.pack("!H", packet_id) + mqtt_string(topic) + b"\x01")


def publish(packet_id: int, topic: str, payload: bytes) -> bytes:
    return packet(0x32, mqtt_string(topic) + struct.pack("!H", packet_id) + payload)


def parse_publish(header: int, body: bytes):
    """解析 PUBLISH 的主题、可选报文标识和载荷。"""
    if len(body) < 2:
        raise ValueError("short PUBLISH")
    topic_len = struct.unpack("!H", body[:2])[0]
    offset = 2 + topic_len
    if offset > len(body):
        raise ValueError("invalid topic length")
    topic = body[2:offset].decode()
    packet_id = None
    if (header >> 1) & 3:
        if offset + 2 > len(body):
            raise ValueError("missing packet id")
        packet_id = struct.unpack("!H", body[offset:offset + 2])[0]
        offset += 2
    return topic, packet_id, body[offset:]
