#!/usr/bin/env python3
"""Terminate local test TLS and forward RocketMQ frames to a disposable Broker."""

import argparse
import socket
import socketserver
import ssl
import threading
from pathlib import Path


def record(server, event, connection_id):
    with server.log_lock:
        with server.log_path.open("a", encoding="utf-8") as output:
            output.write(f"TLS_PROXY\t{event}\t{connection_id}\n")


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        connection_id = f"{self.client_address[1]}-{threading.get_ident()}"
        try:
            client = self.server.context.wrap_socket(self.request, server_side=True)
        except (ssl.SSLError, OSError) as error:
            record(self.server, f"HANDSHAKE_REJECTED:{type(error).__name__}", connection_id)
            return

        record(self.server, "TLS_ACCEPTED", connection_id)
        try:
            upstream = socket.create_connection(self.server.upstream, timeout=5)
        except OSError as error:
            record(self.server, f"UPSTREAM_ERROR:{type(error).__name__}", connection_id)
            client.close()
            return

        def copy(source, destination, label):
            try:
                while True:
                    block = source.recv(65536)
                    if not block:
                        break
                    destination.sendall(block)
            except OSError:
                pass
            finally:
                try:
                    destination.close()
                except OSError:
                    pass
                record(self.server, f"CLOSED_{label}", connection_id)

        client_to_broker = threading.Thread(target=copy, args=(client, upstream, "CLIENT"), daemon=True)
        broker_to_client = threading.Thread(target=copy, args=(upstream, client, "BROKER"), daemon=True)
        client_to_broker.start()
        broker_to_client.start()
        client_to_broker.join()
        broker_to_client.join()
        try:
            client.close()
        finally:
            upstream.close()
        record(self.server, "CONNECTION_CLOSED", connection_id)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, upstream, cert_path, key_path, client_ca_path, log_path):
        super().__init__(("localhost", 0), Handler)
        self.upstream = upstream
        self.log_path = log_path
        self.log_lock = threading.Lock()
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.context.load_cert_chain(str(cert_path), str(key_path))
        if client_ca_path is not None:
            self.context.load_verify_locations(cafile=str(client_ca_path))
            self.context.verify_mode = ssl.CERT_REQUIRED


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--cert", required=True, type=Path)
    parser.add_argument("--key", required=True, type=Path)
    parser.add_argument("--client-ca", type=Path,
                        help="require a client certificate signed by this CA")
    parser.add_argument("--ready-file", required=True, type=Path)
    parser.add_argument("--log-file", required=True, type=Path)
    args = parser.parse_args()
    host, port_text = args.upstream.rsplit(":", 1)
    args.log_file.write_text("", encoding="utf-8")
    server = Server((host, int(port_text)), args.cert, args.key, args.client_ca, args.log_file)
    args.ready_file.write_text(f"{server.server_address[1]}\n", encoding="utf-8")
    print(f"READY\t127.0.0.1\t{server.server_address[1]}", flush=True)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
