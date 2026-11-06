#!/usr/bin/env python3
"""Forward RocketMQ frames with optional send-response delay and error injection."""

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


def log(server, event, connection_id):
    with server.log_lock:
        with server.log_path.open("a", encoding="utf-8") as output:
            output.write(f"PROXY\t{event}\t{connection_id}\n")


def frame_header(frame):
    payload = frame[4:]
    if len(payload) < 4:
        raise ConnectionError("RocketMQ frame omitted its header-length word")
    header_word = int.from_bytes(payload[:4], "big")
    header_length = header_word & 0x00FFFFFF
    if header_length < 2 or header_length > len(payload) - 4:
        raise ConnectionError("RocketMQ frame has an invalid header length")
    try:
        header = json.loads(payload[4 : 4 + header_length])
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ConnectionError("RocketMQ frame has an invalid JSON header") from error
    return header_word, header, payload[4 + header_length :]


def encode_frame(header_word, header, body=b""):
    encoded_header = json.dumps(header, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    protocol_word = (header_word & 0xFF000000) | len(encoded_header)
    payload = protocol_word.to_bytes(4, "big") + encoded_header + body
    return struct.pack(">I", len(payload)) + payload


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        connection_id = f"{self.client_address[1]}-{time.monotonic_ns()}"
        try:
            upstream = socket.create_connection(self.server.upstream, timeout=5)
        except OSError as error:
            log(self.server, f"CONNECT_ERROR:{type(error).__name__}", connection_id)
            return

        def forward_requests():
            try:
                while True:
                    prefix = recv_exact(self.request, 4)
                    if prefix is None:
                        return
                    frame_length = int.from_bytes(prefix, "big", signed=True)
                    if frame_length < 4 or frame_length > 64 * 1024 * 1024:
                        log(self.server, "INVALID_REQUEST_FRAME_LENGTH", connection_id)
                        return
                    payload = recv_exact(self.request, frame_length)
                    if payload is None:
                        return
                    frame = prefix + payload
                    try:
                        header_word, header, body = frame_header(frame)
                    except ConnectionError:
                        upstream.sendall(frame)
                        continue

                    delay_send = False
                    with self.server.pending_delay_lock:
                        if (
                            self.server.delay_send_flag is not None
                            and self.server.delay_send_flag.exists()
                            and header.get("code") in self.server.send_request_codes
                        ):
                            self.server.delay_send_flag.unlink()
                            self.server.pending_delay_opaque.add(header.get("opaque"))
                            delay_send = True
                    if delay_send:
                        log(self.server, f"DELAY_ARMED:{header.get('opaque', -1)}", connection_id)

                    reject_send = False
                    with self.server.reject_lock:
                        if (
                            self.server.reject_flag is not None
                            and self.server.reject_flag.exists()
                            and header.get("code") in self.server.send_request_codes
                        ):
                            self.server.reject_flag.unlink()
                            reject_send = True
                    if reject_send:
                        response_header = dict(header)
                        response_header["code"] = self.server.reject_response_code
                        response_header["flag"] = int(response_header.get("flag") or 0) | 1
                        response_header["remark"] = "P7-V03 injected retryable NO_PERMISSION response"
                        response_frame = encode_frame(header_word, response_header)
                        with self.server.client_send_lock:
                            self.request.sendall(response_frame)
                        log(
                            self.server,
                            f"INJECTED_RESPONSE:{self.server.reject_response_code}:{header.get('opaque', -1)}",
                            connection_id,
                        )
                        continue
                    upstream.sendall(frame)
            except OSError:
                return

        request_thread = threading.Thread(target=forward_requests, daemon=True)
        request_thread.start()
        try:
            while True:
                prefix = recv_exact(upstream, 4)
                if prefix is None:
                    return
                frame_length = int.from_bytes(prefix, "big", signed=True)
                if frame_length < 4 or frame_length > 64 * 1024 * 1024:
                    log(self.server, "INVALID_FRAME_LENGTH", connection_id)
                    return
                payload = recv_exact(upstream, frame_length)
                if payload is None:
                    log(self.server, "TRUNCATED_FRAME", connection_id)
                    return
                frame = prefix + payload

                should_delay = False
                if self.server.delay_send_flag is not None:
                    try:
                        _, response_header, _ = frame_header(frame)
                    except ConnectionError:
                        response_header = {}
                    with self.server.pending_delay_lock:
                        opaque = response_header.get("opaque")
                        if opaque in self.server.pending_delay_opaque:
                            self.server.pending_delay_opaque.remove(opaque)
                            should_delay = True
                else:
                    with self.server.flag_lock:
                        if self.server.delay_flag.exists():
                            self.server.delay_flag.unlink()
                            should_delay = True
                if should_delay:
                    log(self.server, "DELAY_START", connection_id)
                    time.sleep(self.server.delay_seconds)

                try:
                    with self.server.client_send_lock:
                        self.request.sendall(frame)
                except OSError:
                    log(self.server, "RESPONSE_DROPPED", connection_id)
                else:
                    if not should_delay:
                        log(self.server, "RESPONSE_FORWARDED", connection_id)
                if should_delay:
                    log(self.server, "DELAY_DONE", connection_id)
                    return
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

    def __init__(
        self,
        address,
        upstream,
        delay_flag,
        delay_seconds,
        log_path,
        delay_send_flag,
        reject_flag,
        reject_response_code,
        send_request_codes,
    ):
        super().__init__(address, Handler)
        self.upstream = upstream
        self.delay_flag = delay_flag
        self.delay_seconds = delay_seconds
        self.log_path = log_path
        self.delay_send_flag = delay_send_flag
        self.reject_flag = reject_flag
        self.reject_response_code = reject_response_code
        self.send_request_codes = send_request_codes
        self.flag_lock = threading.Lock()
        self.reject_lock = threading.Lock()
        self.pending_delay_opaque = set()
        self.pending_delay_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.client_send_lock = threading.Lock()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", required=True, help="host:port of the disposable Broker")
    parser.add_argument("--delay-ms", type=int, required=True)
    parser.add_argument("--listen-port", type=int, default=0)
    parser.add_argument("--delay-flag", type=Path, required=True)
    parser.add_argument("--delay-send-flag", type=Path)
    parser.add_argument("--reject-send-flag", type=Path)
    parser.add_argument("--reject-response-code", type=int, default=16)
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--log-file", type=Path, required=True)
    args = parser.parse_args()
    host, port_text = args.upstream.rsplit(":", 1)
    server = Server(
        ("127.0.0.1", args.listen_port),
        (host, int(port_text)),
        args.delay_flag,
        args.delay_ms / 1000,
        args.log_file,
        args.delay_send_flag,
        args.reject_send_flag,
        args.reject_response_code,
        {10, 310, 320, 324},
    )
    args.log_file.write_text("", encoding="utf-8")
    args.ready_file.write_text(f"{server.server_address[1]}\n", encoding="utf-8")
    print(f"READY\t127.0.0.1\t{server.server_address[1]}", flush=True)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
