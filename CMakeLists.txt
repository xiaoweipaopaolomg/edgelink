cmake_minimum_required(VERSION 3.16)
project(edgelink VERSION 1.0.0 LANGUAGES C CXX)

# 协议层保持 C17，网关与存储层使用 C++17；禁用编译器私有语言扩展。
set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(EDGELINK_ENABLE_SANITIZERS "Enable ASan and UBSan" OFF)
if(EDGELINK_ENABLE_SANITIZERS)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

# 优先使用当前 Conda 环境中的 SQLite，未激活环境时仍兼容已知开发路径。
find_package(Threads REQUIRED)
find_path(SQLITE3_INCLUDE_DIR sqlite3.h
  HINTS $ENV{CONDA_PREFIX}/include /home/wang/miniconda3/include)
find_library(SQLITE3_LIBRARY NAMES sqlite3
  HINTS $ENV{CONDA_PREFIX}/lib /home/wang/miniconda3/lib)
if(NOT SQLITE3_INCLUDE_DIR OR NOT SQLITE3_LIBRARY)
  message(FATAL_ERROR "SQLite3 development files not found (install libsqlite3-dev)")
endif()

# C 协议库不依赖 C++ 运行时，便于独立复用和单元测试。
add_library(edgelink_protocol src/protocol/modbus.c)
target_include_directories(edgelink_protocol PUBLIC include)
target_compile_options(edgelink_protocol PRIVATE -Wall -Wextra -Wpedantic)

# 核心库包含配置、单线程 SQLite 所有权和 MQTT 上报线程。
add_library(edgelink_core
  src/common/config.cpp
  src/storage/storage.cpp
  src/uplink/mqtt_client.cpp)
target_include_directories(edgelink_core PUBLIC include PRIVATE ${SQLITE3_INCLUDE_DIR})
target_link_libraries(edgelink_core PUBLIC Threads::Threads ${SQLITE3_LIBRARY})
target_compile_options(edgelink_core PRIVATE -Wall -Wextra -Wpedantic)

add_executable(edgelinkd apps/edgelinkd/main.cpp)
target_link_libraries(edgelinkd PRIVATE edgelink_core edgelink_protocol)
target_compile_options(edgelinkd PRIVATE -Wall -Wextra -Wpedantic)

add_executable(edgectl apps/edgectl/main.cpp)
target_link_libraries(edgectl PRIVATE edgelink_core)
target_compile_options(edgectl PRIVATE -Wall -Wextra -Wpedantic)

include(CTest)
if(BUILD_TESTING)
  # C 单元测试覆盖线协议边界；Python 集成测试启动真实本地进程与 Socket。
  add_executable(test_modbus tests/unit/test_modbus.c)
  target_link_libraries(test_modbus PRIVATE edgelink_protocol)
  add_test(NAME modbus_unit COMMAND test_modbus)
  add_test(NAME config_check
    COMMAND edgectl config-check ${CMAKE_SOURCE_DIR}/config/edgelink.conf)
  find_package(Python3 COMPONENTS Interpreter)
  if(Python3_Interpreter_FOUND)
    add_test(NAME integration_smoke
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/integration/smoke.py
              --build-dir ${CMAKE_BINARY_DIR})
    set_tests_properties(integration_smoke PROPERTIES TIMEOUT 30)
    add_test(NAME integration_capacity
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/integration/capacity.py
              --build-dir ${CMAKE_BINARY_DIR})
    set_tests_properties(integration_capacity PROPERTIES TIMEOUT 40)
  endif()
endif()

install(TARGETS edgelinkd edgectl RUNTIME DESTINATION bin)
install(FILES config/edgelink.conf DESTINATION etc/edgelink)
install(FILES deploy/edgelink.service DESTINATION lib/systemd/system)
