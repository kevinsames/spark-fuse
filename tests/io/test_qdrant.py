from __future__ import annotations

import json
import socketserver
import threading
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict, Iterable, List

import pytest

from spark_fuse.io import (
    QDRANT_CONFIG_OPTION,
    QDRANT_FORMAT,
    build_qdrant_config,
    build_qdrant_write_config,
    register_qdrant_data_source,
    write_qdrant_points,
)


class _ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True


def _start_scroll_server(
    points: List[Dict[str, Any]],
    headers_log: list[str] | None = None,
    request_log: list[dict[str, Any]] | None = None,
) -> tuple[_ThreadedTCPServer, str]:
    class _Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b"{}"
            try:
                payload = json.loads(body.decode("utf-8"))
            except Exception:
                payload = {}

            if headers_log is not None:
                headers_log.append(self.headers.get("api-key"))
            if request_log is not None:
                request_log.append(payload)

            offset = int(payload.get("offset") or 0)
            limit = int(payload.get("limit") or 10)
            batch = points[offset : offset + limit]
            next_offset = offset + len(batch)
            next_page = next_offset if next_offset < len(points) else None

            response = {
                "result": {"points": batch, "next_page_offset": next_page},
                "status": "ok",
                "time": 0.001,
            }
            encoded = json.dumps(response).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format, *args):
            return

    server: _ThreadedTCPServer = _ThreadedTCPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    return server, f"http://{host}:{port}"


def _start_points_server(
    headers_log: list[str] | None = None,
    body_log: list[dict[str, Any]] | None = None,
) -> tuple[_ThreadedTCPServer, str]:
    class _Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b"{}"
            if headers_log is not None:
                headers_log.append(self.headers.get("api-key"))
            decoded: dict[str, Any]
            try:
                decoded = json.loads(body.decode("utf-8"))
            except Exception:
                decoded = {}
            if body_log is not None:
                body_log.append(decoded)
            response = {"status": "ok", "result": {"operation_id": "op-1"}, "time": 0.001}
            encoded = json.dumps(response).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format, *args):
            return

    server: _ThreadedTCPServer = _ThreadedTCPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    return server, f"http://{host}:{port}"


def _collect_rows(df) -> Iterable[dict[str, Any]]:
    return [row.asDict(recursive=True) for row in df.collect()]


@pytest.mark.usefixtures("spark")
def test_qdrant_scrolls_pages_and_passes_auth(spark):
    points = [
        {"id": 1, "vector": [0.1, 0.2], "payload": {"name": "alpha"}},
        {"id": 2, "vector": [0.2, 0.3], "payload": {"name": "beta"}},
        {"id": 3, "vector": [0.3, 0.4], "payload": {"name": "gamma"}},
    ]
    headers_log: list[str] = []
    request_log: list[dict[str, Any]] = []
    server, base_url = _start_scroll_server(points, headers_log, request_log)
    try:
        config = build_qdrant_config(
            spark,
            base_url,
            collection="pokemon",
            source_config={
                "page_size": 2,
                "limit": 3,
                "with_vectors": True,
                "api_key": "secret-key",
            },
        )
        register_qdrant_data_source(spark)
        df = (
            spark.read.format(QDRANT_FORMAT).option(QDRANT_CONFIG_OPTION, json.dumps(config)).load()
        )

        collected = _collect_rows(df.orderBy("id"))
        assert collected == points
    finally:
        server.shutdown()
        server.server_close()

    assert headers_log
    assert all(header == "secret-key" for header in headers_log if header is not None)
    assert request_log
    assert all(entry.get("with_vectors") is True for entry in request_log)


@pytest.mark.usefixtures("spark")
def test_qdrant_respects_filter_and_payload_flag(spark):
    points = [
        {"id": 1, "payload": {"type": "starter"}},
        {"id": 2, "payload": {"type": "starter"}},
        {"id": 3, "payload": {"type": "starter"}},
    ]
    request_log: list[dict[str, Any]] = []
    server, base_url = _start_scroll_server(points, request_log=request_log)
    try:
        config = build_qdrant_config(
            spark,
            base_url,
            collection="pokemon",
            source_config={
                "page_size": 2,
                "max_pages": 1,
                "with_payload": False,
                "filter": {"must": [{"key": "payload.type", "match": {"value": "starter"}}]},
            },
        )
        register_qdrant_data_source(spark)
        df = (
            spark.read.format(QDRANT_FORMAT).option(QDRANT_CONFIG_OPTION, json.dumps(config)).load()
        )

        collected = _collect_rows(df.orderBy("id"))
        assert collected == [{"id": 1}, {"id": 2}]
    finally:
        server.shutdown()
        server.server_close()

    assert request_log
    assert request_log[0].get("filter") is not None
    assert request_log[0].get("with_payload") is False


def test_write_qdrant_points_helper():
    headers_log: list[str] = []
    body_log: list[dict[str, Any]] = []
    server, base_url = _start_points_server(headers_log, body_log)
    try:
        points = [
            {"id": 1, "vector": [0.1, 0.2], "name": "alpha"},
            {"id": 2, "vector": [0.2, 0.3], "name": "beta"},
        ]
        written = write_qdrant_points(
            points,
            base_url,
            collection="pokemon",
            payload_fields=["name"],
            api_key="secret",
            batch_size=1,
        )
        assert written == 2
    finally:
        server.shutdown()
        server.server_close()

    assert headers_log
    assert all(h == "secret" for h in headers_log if h is not None)
    assert body_log
    assert all("points" in body for body in body_log)
    first_points = body_log[0]["points"]
    assert first_points[0]["payload"] == {"name": "alpha"}


def test_qdrant_datasource_writer_batches():
    from pyspark.sql import Row

    body_log: list[dict[str, Any]] = []
    server, base_url = _start_points_server(body_log=body_log)
    try:
        config = build_qdrant_write_config(
            base_url,
            collection="pokemon",
            batch_size=2,
            wait=False,
        )

        from spark_fuse.io.qdrant import _QdrantWriteConfig, _QdrantDataSourceWriter

        resolved = _QdrantWriteConfig.from_dict(config)
        datasource_writer = _QdrantDataSourceWriter(resolved)

        rows = [Row(id=1, vector=[1.0], name="alpha"), Row(id=2, vector=[2.0], name="beta")]
        datasource_writer.write(iter(rows))
    finally:
        server.shutdown()
        server.server_close()

    assert body_log
    assert body_log[0].get("wait") is False
    sent_points = body_log[0]["points"]
    assert {p["id"] for p in sent_points} == {1, 2}
