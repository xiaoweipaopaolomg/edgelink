#include "edgelink/modbus.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

int main(void) {
  /* 请求编码：验证正常字节布局和寄存器数量边界。 */
  uint8_t request[12];
  assert(edgelink_modbus_build_read03(0x1234, 7, 10, 2, request) == EDGELINK_MODBUS_OK);
  const uint8_t expected[] = {0x12,0x34,0,0,0,6,7,3,0,10,0,2};
  assert(memcmp(request, expected, sizeof(expected)) == 0);
  assert(edgelink_modbus_build_read03(1, 1, 0, 0, request) == EDGELINK_MODBUS_RANGE);
  assert(edgelink_modbus_build_read03(1, 1, 0, 126, request) == EDGELINK_MODBUS_RANGE);

  /* 分段识别及正常响应解析。 */
  const uint8_t response[] = {0x12,0x34,0,0,0,7,7,3,4,0x12,0x34,0xab,0xcd};
  size_t frame_size = 0;
  assert(edgelink_modbus_frame_size(response, 5, &frame_size) == EDGELINK_MODBUS_NEED_MORE);
  assert(edgelink_modbus_frame_size(response, sizeof(response), &frame_size) == EDGELINK_MODBUS_OK);
  assert(frame_size == sizeof(response));
  uint16_t registers[2]; size_t count = 0; uint8_t exception = 0;
  assert(edgelink_modbus_parse_read03(response, sizeof(response), 0x1234, 7,
                                      registers, 2, &count, &exception) == EDGELINK_MODBUS_OK);
  assert(count == 2 && registers[0] == 0x1234 && registers[1] == 0xabcd);
  /* 事务号不一致的迟到响应不能被当前轮询接收。 */
  assert(edgelink_modbus_parse_read03(response, sizeof(response), 1, 7,
                                      registers, 2, &count, &exception) == EDGELINK_MODBUS_MISMATCH);
  /* 设备异常响应应保留异常码并返回独立状态。 */
  const uint8_t error[] = {0,1,0,0,0,3,1,0x83,2};
  assert(edgelink_modbus_parse_read03(error, sizeof(error), 1, 1,
                                      registers, 2, &count, &exception) == EDGELINK_MODBUS_EXCEPTION);
  assert(exception == 2);
  /* 协议号、MBAP 长度和奇数字节数等畸形报文必须被拒绝。 */
  const uint8_t bad_protocol[] = {0,1,0,1,0,3,1,3,0};
  assert(edgelink_modbus_frame_size(bad_protocol, sizeof(bad_protocol), &frame_size) == EDGELINK_MODBUS_INVALID);
  const uint8_t bad_length[] = {0,1,0,0,0,1,1};
  assert(edgelink_modbus_frame_size(bad_length, sizeof(bad_length), &frame_size) == EDGELINK_MODBUS_INVALID);
  const uint8_t odd_bytes[] = {0,1,0,0,0,4,1,3,1,0};
  assert(edgelink_modbus_parse_read03(odd_bytes, sizeof(odd_bytes), 1, 1,
                                      registers, 2, &count, &exception) == EDGELINK_MODBUS_INVALID);
  puts("modbus tests passed");
  return 0;
}
