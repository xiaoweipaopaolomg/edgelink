#include "edgelink/modbus.h"

/* Modbus TCP 线上字段均为大端序，显式读写可避免未对齐访问和主机字节序差异。 */
static uint16_t read_be16(const uint8_t* p) {
  return (uint16_t)(((uint16_t)p[0] << 8) | p[1]);
}

static void write_be16(uint8_t* p, uint16_t value) {
  p[0] = (uint8_t)(value >> 8);
  p[1] = (uint8_t)value;
}

int edgelink_modbus_build_read03(uint16_t transaction_id, uint8_t unit_id,
                                 uint16_t address, uint16_t count,
                                 uint8_t out[12]) {
  if (!out || count == 0 || count > 125) return EDGELINK_MODBUS_RANGE;
  /* MBAP：事务号、协议号 0、后续长度 6、单元号；之后是 5 字节 PDU。 */
  write_be16(out, transaction_id);
  write_be16(out + 2, 0);
  write_be16(out + 4, 6);
  out[6] = unit_id;
  out[7] = 0x03;
  write_be16(out + 8, address);
  write_be16(out + 10, count);
  return EDGELINK_MODBUS_OK;
}

int edgelink_modbus_frame_size(const uint8_t* data, size_t size,
                               size_t* frame_size) {
  uint16_t length;
  size_t total;
  if (!data || !frame_size) return EDGELINK_MODBUS_INVALID;
  /* MBAP 固定头为 7 字节，不能把 TCP 的一次 recv 当作一帧。 */
  if (size < 7) return EDGELINK_MODBUS_NEED_MORE;
  if (read_be16(data + 2) != 0) return EDGELINK_MODBUS_INVALID;
  length = read_be16(data + 4);
  /* length 包含单元号和 PDU；上限同时约束恶意或损坏报文的内存占用。 */
  if (length < 2 || length > 254) return EDGELINK_MODBUS_INVALID;
  total = 6u + length;
  *frame_size = total;
  return size < total ? EDGELINK_MODBUS_NEED_MORE : EDGELINK_MODBUS_OK;
}

int edgelink_modbus_parse_read03(const uint8_t* data, size_t size,
                                 uint16_t expected_transaction,
                                 uint8_t expected_unit, uint16_t* registers,
                                 size_t capacity, size_t* register_count,
                                 uint8_t* exception_code) {
  size_t frame_size = 0;
  int status = edgelink_modbus_frame_size(data, size, &frame_size);
  if (status != EDGELINK_MODBUS_OK) return status;
  /* 同一连接也必须匹配事务号，防止迟到响应被错误归入下一次轮询。 */
  if (frame_size != size || read_be16(data) != expected_transaction ||
      data[6] != expected_unit)
    return EDGELINK_MODBUS_MISMATCH;
  if (data[7] == (uint8_t)(0x03 | 0x80)) {
    if (frame_size != 9) return EDGELINK_MODBUS_INVALID;
    if (exception_code) *exception_code = data[8];
    return EDGELINK_MODBUS_EXCEPTION;
  }
  if (data[7] != 0x03 || frame_size < 9) return EDGELINK_MODBUS_INVALID;
  /* 每个寄存器占两个字节，字节数必须为偶数并与 MBAP 声明长度一致。 */
  if ((data[8] & 1u) != 0 || (size_t)data[8] + 9u != frame_size)
    return EDGELINK_MODBUS_INVALID;
  *register_count = (size_t)data[8] / 2u;
  if (*register_count > capacity || (!registers && *register_count))
    return EDGELINK_MODBUS_RANGE;
  for (size_t i = 0; i < *register_count; ++i)
    registers[i] = read_be16(data + 9 + i * 2);
  return EDGELINK_MODBUS_OK;
}
