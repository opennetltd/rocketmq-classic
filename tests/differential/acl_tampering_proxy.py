#!/usr/bin/env python3
"""Forward one local RocketMQ request after changing its signed JSON body."""

import argparse
import json
import socket
import socketserver
import struct
import threading
import time
from pathlib import Path


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


def record(server, event, connection_id, request_code="-"):
    with server.log_lock:
        with server.log_path.open("a", encoding="utf-8") as output:
            output.write(f"ACL_PROXY\t{event}\t{connection_id}\t{request_code}\n")


def mutate_string(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if isinstance(child, str) and child:
                value[key] = child[:-1] + ("A" if child[-1] != "A" else "B")
                return True
            if mutate_string(child):
                return True
    elif isinstance(value, list):
        for child in value:
            if mutate_string(child):
                return True
    return False


def tamper_frame(frame):
    prefix = frame[:4]
    payload = frame[4:]
    if len(payload) < 4:
        raise ConnectionError("RocketMQ frame omitted the header-length word")
    header_word = int.from_bytes(payload[:4], "big")
    header_length = header_word & 0x00FFFFFF
    if header_length < 2 or header_length > len(payload) - 4:
        raise ConnectionError("RocketMQ frame has an invalid header length")
    header_bytes = payload[4 : 4 + header_length]
    header = json.loads(header_bytes)
    body = payload[4 + header_length :]
    if not body:
        raise ConnectionError("signed heartbeat request has no body to tamper")
    body_value = json.loads(body)
    if not mutate_string(body_value):
        raise ConnectionError("signed heartbeat body contains no mutable string value")
    modified_body = json.dumps(body_value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    modified_payload = payload[: 4 + header_length] + modified_body
    modified_frame = struct.pack(">I", len(modified_payload)) + modified_payload
    return modified_frame, header.get("code")


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        connection_id = f"{self.client_address[1]}-{time.monotonic_ns()}"
        try:
            upstream = socket.create_connection(self.server.upstream, timeout=5)
        except OSError as error:
            record(self.server, f"CONNECT_ERROR:{type(error).__name__}", connection_id)
            return

        def forward_requests():
            tampered = False
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
                    frame = prefix + payload
                    if not tampered:
                        frame, request_code = tamper_frame(frame)
                        tampered = True
                        record(self.server, "TAMPERED_BODY", connection_id, request_code)
                    upstream.sendall(frame)
            except (ConnectionError, OSError, ValueError, json.JSONDecodeError) as error:
                record(self.server, f"REQUEST_ERROR:{type(error).__name__}", connection_id)
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
                    record(self.server, "INVALID_RESPONSE_FRAME_LENGTH", connection_id)
                    return
                payload = recv_exact(upstream, frame_length)
                if payload is None:
                    return
                self.request.sendall(prefix + payload)
                record(self.server, "RESPONSE_FORWARDED", connection_id)
        except (ConnectionError, OSError):
            return
        finally:
            try:
                upstream.close()
            except OSError:
                pass


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, upstream, log_path):
        super().__init__(("127.0.0.1", 0), Handler)
        self.upstream = upstream
        self.log_path = log_path
        self.log_lock = threading.Lock()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", required=True, help="host:port of the disposable ACL Broker")
    parser.add_argument("--ready-file", required=True, type=Path)
    parser.add_argument("--log-file", required=True, type=Path)
    args = parser.parse_args()
    host, port_text = args.upstream.rsplit(":", 1)
    args.log_file.write_text("", encoding="utf-8")
    server = Server((host, int(port_text)), args.log_file)
    args.ready_file.write_text(f"{server.server_address[1]}\n", encoding="utf-8")
    print(f"READY\t127.0.0.1\t{server.server_address[1]}", flush=True)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
