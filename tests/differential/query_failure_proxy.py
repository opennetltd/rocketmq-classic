#!/usr/bin/env python3
"""Forward RocketMQ frames and optionally turn query replies into Broker errors."""

import argparse
import json
import socket
import socketserver
import struct
import threading
from pathlib import Path

QUERY_MESSAGE = 12
SYSTEM_ERROR = 1


def recv_exact(connection, count):
    chunks = []
    remaining = count
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            if not chunks:
                return None
            raise ConnectionError("peer closed during a RocketMQ frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def parse_frame(prefix, payload):
    if len(payload) < 4:
        raise ConnectionError("RocketMQ frame omitted its header-length word")
    header_word = int.from_bytes(payload[:4], "big")
    header_length = header_word & 0x00FFFFFF
    if header_length < 2 or header_length > len(payload) - 4:
        raise ConnectionError("RocketMQ frame has an invalid header length")
    header_bytes = payload[4 : 4 + header_length]
    header = json.loads(header_bytes)
    body = payload[4 + header_length :]
    return int.from_bytes(prefix, "big", signed=True), header_word, header, body


def encode_frame(header_word, header, body):
    header_bytes = json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    payload = struct.pack(">I", (header_word & 0xFF000000) | len(header_bytes)) + header_bytes + body
    return struct.pack(">I", len(payload)) + payload


def record(server, event, opaque="-"):
    with server.log_lock:
        with server.log_path.open("a", encoding="utf-8") as output:
            output.write(f"P17_PROXY\t{server.label}\t{event}\t{opaque}\n")


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            upstream = socket.create_connection(self.server.upstream, timeout=5)
        except OSError as error:
            record(self.server, f"CONNECT_ERROR:{type(error).__name__}")
            return
        failing_opaques = set()
        opaque_lock = threading.Lock()

        def forward_requests():
            try:
                while True:
                    prefix = recv_exact(self.request, 4)
                    if prefix is None:
                        return
                    frame_length = int.from_bytes(prefix, "big", signed=True)
                    if frame_length < 4 or frame_length > 64 * 1024 * 1024:
                        raise ConnectionError("invalid RocketMQ request frame length")
                    payload = recv_exact(self.request, frame_length)
                    if payload is None:
                        return
                    _, _, header, _ = parse_frame(prefix, payload)
                    opaque = header.get("opaque")
                    if header.get("code") == QUERY_MESSAGE and self.server.mode_path.read_text().strip() == "error":
                        with opaque_lock:
                            failing_opaques.add(opaque)
                        record(self.server, "QUERY_REQUEST_MARKED", opaque)
                    upstream.sendall(prefix + payload)
            except (ConnectionError, OSError, ValueError, json.JSONDecodeError) as error:
                record(self.server, f"REQUEST_ERROR:{type(error).__name__}")
            finally:
                try:
                    upstream.shutdown(socket.SHUT_WR)
                except OSError:
                    pass

        request_thread = threading.Thread(target=forward_requests, daemon=True)
        request_thread.start()
        try:
            while True:
                prefix = recv_exact(upstream, 4)
                if prefix is None:
                    return
                frame_length = int.from_bytes(prefix, "big", signed=True)
                if frame_length < 4 or frame_length > 64 * 1024 * 1024:
                    record(self.server, "INVALID_RESPONSE_FRAME_LENGTH")
                    return
                payload = recv_exact(upstream, frame_length)
                if payload is None:
                    return
                _, header_word, header, body = parse_frame(prefix, payload)
                opaque = header.get("opaque")
                with opaque_lock:
                    inject_error = opaque in failing_opaques
                    if inject_error:
                        failing_opaques.discard(opaque)
                if inject_error:
                    header["code"] = SYSTEM_ERROR
                    header["remark"] = "P17 synthetic Broker query failure"
                    frame = encode_frame(header_word, header, body)
                    record(self.server, "INJECTED_QUERY_ERROR", opaque)
                    self.request.sendall(frame)
                else:
                    self.request.sendall(prefix + payload)
        except (ConnectionError, OSError, ValueError, json.JSONDecodeError):
            return
        finally:
            try:
                upstream.close()
            except OSError:
                pass


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, label, port, upstream, mode_path, log_path):
        super().__init__(("127.0.0.1", port), Handler)
        self.label = label
        self.upstream = upstream
        self.mode_path = mode_path
        self.log_path = log_path
        self.log_lock = threading.Lock()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--listen-port", required=True, type=int)
    parser.add_argument("--upstream", required=True, help="host:port of one disposable Broker")
    parser.add_argument("--mode-file", required=True, type=Path, help="contains forward or error")
    parser.add_argument("--ready-file", required=True, type=Path)
    parser.add_argument("--log-file", required=True, type=Path)
    args = parser.parse_args()
    host, port_text = args.upstream.rsplit(":", 1)
    server = Server(args.label, args.listen_port, (host, int(port_text)), args.mode_file, args.log_file)
    args.ready_file.write_text(f"127.0.0.1\t{args.listen_port}\n", encoding="utf-8")
    print(f"READY\t{args.label}\t127.0.0.1\t{args.listen_port}", flush=True)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
