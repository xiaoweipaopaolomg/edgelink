#!/usr/bin/env python3
"""验证容量耗尽时暂停采集、保护未确认事件，并在 ACK 后自动恢复。"""

import argparse
import json
import pathlib
import signal
import socket
import subprocess
import sys
import tempfile
import time


def free_port():
    """由内核分配一个本机临时端口，降低并行测试端口冲突概率。"""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_for(predicate, timeout=25):
    """轮询最终一致状态；超时时保留最后一次返回值辅助定位。"""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (OSError, subprocess.CalledProcessError, json.JSONDecodeError):
            pass
        time.sleep(0.1)
    raise AssertionError(f"condition timed out; last value={last!r}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir", required=True)
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parents[2]
    build = pathlib.Path(args.build_dir).resolve()
    processes = []
    with tempfile.TemporaryDirectory(prefix="edgelink-capacity-") as temp_text:
        temp = pathlib.Path(temp_text)
        broker_port, device_port = free_port(), free_port()
        config = temp / "edgelink.conf"
        config.write_text(f"""gateway_id=gw-capacity
database={temp / 'gateway.db'}
socket={temp / 'edgelink.sock'}
mqtt_host=127.0.0.1
mqtt_port={broker_port}
mqtt_ack_timeout_ms=200
mqtt_window=8
event_queue_capacity=32
event_queue_high_watermark=24
event_queue_low_watermark=8
control_queue_capacity=16
storage_budget_bytes=65536
storage_reserve_bytes=0
history_retention_ms=0
device=meter-01,127.0.0.1,{device_port},1,50,300
point=meter-01,meter-01.voltage,0,uint16,0.1,V
point=meter-01,meter-01.current,1,uint16,0.01,A
point=meter-01,meter-01.temperature,2,int16,0.1,C
point=meter-01,meter-01.p3,3,uint16,1,count
point=meter-01,meter-01.p4,4,uint16,1,count
point=meter-01,meter-01.p5,5,uint16,1,count
point=meter-01,meter-01.p6,6,uint16,1,count
point=meter-01,meter-01.p7,7,uint16,1,count
point=meter-01,meter-01.p8,8,uint16,1,count
point=meter-01,meter-01.p9,9,uint16,1,count
""")

        def start(command):
            process = subprocess.Popen(command, cwd=root, stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL)
            processes.append(process)
            return process

        def status():
            output = subprocess.check_output(
                [build / "edgectl", "--socket", temp / "edgelink.sock", "status"],
                text=True, stderr=subprocess.DEVNULL)
            return json.loads(output)

        try:
            # 先不启动业务接收端，使 outbox 持续增长直至触发小容量预算。
            start([sys.executable, root / "tools/broker/minibroker.py", "--port", str(broker_port)])
            start([sys.executable, root / "tools/device_sim/modbus_sim.py", "--port", str(device_port)])
            start([build / "edgelinkd", "--config", config])
            wait_for(lambda: (temp / "edgelink.sock").exists())
            full = wait_for(lambda: status() if status().get("storage_full") else None)
            assert full["collection_paused"]
            assert full["pending_events"] > 0, full
            assert full["storage_bytes"] <= full["storage_budget_bytes"]
            pending = full["pending_events"]
            time.sleep(0.5)
            # 即使已经满载，未收到业务 ACK 的事件也不能被容量清理淘汰。
            assert status()["pending_events"] == pending

            # 启动接收端消化 outbox，随后应解除满载状态并自动恢复采集。
            start([sys.executable, root / "tools/receiver/receiver.py", "--port", str(broker_port),
                   "--gateway-id", "gw-capacity", "--database", temp / "receiver.db"])
            recovered = wait_for(lambda: status() if (status().get("pending_events") == 0 and
                                                       not status().get("storage_full") and
                                                       not status().get("collection_paused")) else None)
            committed = recovered["committed_events"]
            wait_for(lambda: status().get("committed_events", 0) > committed)
            print("capacity integration test passed")
        finally:
            # 逆序退出依赖进程；超时后强制结束，避免污染后续测试。
            for process in reversed(processes):
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
            for process in reversed(processes):
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()


if __name__ == "__main__":
    main()
