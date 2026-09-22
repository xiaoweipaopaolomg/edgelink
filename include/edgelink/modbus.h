#ifndef EDGELINK_MODBUS_H
#define EDGELINK_MODBUS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum {
  /* 返回值区分“等待更多 TCP 字节”和“当前帧确定非法”，便于状态机决策。 */
  EDGELINK_MODBUS_OK = 0,
  EDGELINK_MODBUS_NEED_MORE = 1,
  EDGELINK_MODBUS_INVALID = -1,
  EDGELINK_MODBUS_MISMATCH = -2,
  EDGELINK_MODBUS_EXCEPTION = -3,
  EDGELINK_MODBUS_RANGE = -4
};

/* 构造 Modbus TCP 读保持寄存器（功能码 0x03）请求，out 固定为 12 字节。 */
int edgelink_modbus_build_read03(uint16_t transaction_id, uint8_t unit_id,
                                 uint16_t address, uint16_t count,
                                 uint8_t out[12]);

/* 校验 MBAP 头并返回完整帧长度；TCP 分段时返回 NEED_MORE。 */
int edgelink_modbus_frame_size(const uint8_t* data, size_t size,
                               size_t* frame_size);

/* 解析完整 0x03 响应，校验事务号/单元号，并按主机字节序输出寄存器。 */
int edgelink_modbus_parse_read03(const uint8_t* data, size_t size,
                                 uint16_t expected_transaction,
                                 uint8_t expected_unit, uint16_t* registers,
                                 size_t capacity, size_t* register_count,
                                 uint8_t* exception_code);

#ifdef __cplusplus
}
#endif
#endif
