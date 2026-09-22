#!/usr/bin/env python3
"""端到端验证多设备采集、故障隔离、幂等补传和崩溃恢复。"""

import argparse
import json
import pathlib
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time


def free_port():
    """由内核分配一个本机临时端口，避免使用固定测试端口。"""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def wait_for(predicate, timeout=12):
    """等待异步组件达到目标状态，并容忍启动阶段的短暂连接失败。"""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (OSError, subprocess.CalledProcessError, json.JSONDecodeError, sqlite3.Error):
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
    with tempfile.TemporaryDirectory(prefix="edgelink-smoke-") as temp_text:
        temp = pathlib.Path(temp_text)
        broker_port, device_port, device2_port = free_port(), free_port(), free_port()
        config = temp / "edgelink.conf"
        config.write_text(f"""gateway_id=gw-test
database={temp / 'gateway.db'}
socket={temp / 'edgelink.sock'}
mqtt_host=127.0.0.1
mqtt_port={broker_port}
mqtt_ack_timeout_ms=500
mqtt_window=8
event_queue_capacity=128
event_queue_high_watermark=96
event_queue_low_watermark=48
control_queue_capacity=32
device=meter-01,127.0.0.1,{device_port},1,100,300
point=meter-01,meter-01.voltage,0,uint16,0.1,V
point=meter-01,meter-01.current,1,uint16,0.01,A
device=meter-02,127.0.0.1,{device2_port},2,100,300
point=meter-02,meter-02.voltage,0,uint16,0.1,V
point=meter-02,meter-02.current,1,uint16,0.01,A
""")

        def start(command):
            process = subprocess.Popen(command, cwd=root, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            processes.append(process)
            return process

        def status():
            output = subprocess.check_output([build / "edgectl", "--socket", temp / "edgelink.sock", "status"],
                                             text=True, stderr=subprocess.DEVNULL)
            return json.loads(output)

        try:
            # 两台设备分别注入 TCP 分段和异常长度；接收端注入一次性 ACK 丢失。
            broker = start([sys.executable, root / "tools/broker/minibroker.py", "--port", str(broker_port)])
            simulator1 = start([sys.executable, root / "tools/device_sim/modbus_sim.py", "--port", str(device_port), "--split"])
            simulator2 = start([sys.executable, root / "tools/device_sim/modbus_sim.py", "--port", str(device2_port),
                                "--bad-length-every", "5"])
            receiver = start([sys.executable, root / "tools/receiver/receiver.py", "--port", str(broker_port),
                              "--gateway-id", "gw-test", "--database", temp / "receiver.db",
                              "--drop-ack-once-every", "2"])
            daemon = start([build / "edgelinkd", "--config", config])
            wait_for(lambda: (temp / "edgelink.sock").exists())
            try:
                wait_for(lambda: status().get("committed_events", 0) >= 6 and
                          status().get("business_acks", 0) >= 4 and
                          status().get("pending_events", 99) <= 4)
            except AssertionError as error:
                raise AssertionError(f"{error}; gateway status={status()}") from error
            latest = json.loads(subprocess.check_output([build / "edgectl", "--socket", temp / "edgelink.sock", "latest"], text=True))
            assert len(latest["items"]) == 4
            history = json.loads(subprocess.check_output([build / "edgectl", "--socket", temp / "edgelink.sock",
                                                          "history", "meter-01.voltage"], text=True))
            assert history["items"]
            export_path = temp / "voltage.csv"
            subprocess.check_call([build / "edgectl", "--socket", temp / "edgelink.sock", "export",
                                   "meter-01.voltage", export_path], stdout=subprocess.DEVNULL)
            assert export_path.read_text().count("\n") >= 2

            # 单台设备退出后，另一台设备仍应持续采集，证明状态机相互隔离。
            connection = sqlite3.connect(temp / "receiver.db")
            # attempts >= 2 证明 ACK 丢失确实触发重复投递，而唯一键仍保持幂等。
            wait_for(lambda: connection.execute(
                "SELECT coalesce(max(attempts),0) FROM deliveries").fetchone()[0] >= 2)
            meter2_before = connection.execute("SELECT count(*) FROM events WHERE device_id='meter-02'").fetchone()[0]
            simulator1.terminate(); simulator1.wait(timeout=3)
            wait_for(lambda: any(item["device_id"] == "meter-01" and item["state"] != "ONLINE"
                                 for item in json.loads(subprocess.check_output(
                                     [build / "edgectl", "--socket", temp / "edgelink.sock", "devices"],
                                     text=True))["items"]))
            wait_for(lambda: connection.execute(
                "SELECT count(*) FROM events WHERE device_id='meter-02'").fetchone()[0] > meter2_before)
            wait_for(lambda: any(item["device_id"] == "meter-02" and item["parse_errors"] > 0
                                 for item in json.loads(subprocess.check_output(
                                     [build / "edgectl", "--socket", temp / "edgelink.sock", "devices"],
                                     text=True))["items"]))

            # Broker 中断形成持久化积压；带积压 SIGKILL 后重启，outbox 必须仍然存在。
            broker.terminate(); broker.wait(timeout=3)
            wait_for(lambda: not status()["mqtt_connected"])
            wait_for(lambda: status()["pending_events"] >= 3)
            persisted_pending = status()["pending_events"]
            daemon.kill(); daemon.wait(timeout=3)
            daemon = start([build / "edgelinkd", "--config", config])
            wait_for(lambda: status().get("pending_events", 0) >= persisted_pending)
            simulator2.terminate(); simulator2.wait(timeout=3)
            # 设备停止后恢复 Broker，使待发送量稳定收敛到零，便于断言补传完成。
            broker = start([sys.executable, root / "tools/broker/minibroker.py", "--port", str(broker_port)])
            wait_for(lambda: status().get("mqtt_connected") and status().get("pending_events") == 0)
            assert connection.execute("SELECT count(*) FROM events").fetchone()[0] >= 6
            assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            print("integration smoke test passed")
        finally:
            # 无论断言是否失败都回收子进程，保证测试可重复执行。
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
