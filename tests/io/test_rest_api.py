from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict
import socketserver

import pytest

from spark_fuse.io.rest_api import RestAPIReader


class _ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True


def _start_mock_server(
    responses: Dict[str, Any], headers_log: list[str] | None = None
) -> tuple[_ThreadedTCPServer, str]:
    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            payload = responses.get(self.path)
            if payload is None:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"{}")
                return
            body = json.dumps(payload).encode("utf-8")
            if headers_log is not None:
                headers_log.append(self.headers.get("User-Agent"))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            return

    server: _ThreadedTCPServer = _ThreadedTCPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    return server, f"http://{host}:{port}"


@pytest.mark.usefixtures("spark")
def test_rest_api_reader_query_pagination(spark):
    data_pages = [
        [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}],
        [{"id": 3, "name": "gamma"}],
    ]
    server_responses: Dict[str, Any] = {}
    server, server_base = _start_mock_server(server_responses)
    try:
        for idx, records in enumerate(data_pages, start=1):
            path = f"/items?limit=2&page={idx}"
            server_responses[path] = {"data": records, "meta": {"page": idx}}

        reader = RestAPIReader()
        df = reader.read(
            spark,
            f"{server_base}/items",
            source_config={
                "params": {"limit": 2},
                "records_field": "data",
                "pagination": {"mode": "query", "param": "page", "start": 1, "stop": 2},
                "parallelism": 1,
            },
        )

        rows = {(row.id, row.name) for row in df.select("id", "name").collect()}
        assert rows == {(1, "alpha"), (2, "beta"), (3, "gamma")}
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.usefixtures("spark")
def test_rest_api_reader_response_pagination_next_links(spark):
    server_responses: Dict[str, Any] = {}
    headers_log: list[str] = []
    server, server_base = _start_mock_server(server_responses, headers_log)
    try:
        first_path = "/pokemon?limit=2"
        second_path = "/pokemon?limit=2&offset=2"
        second_url = f"{server_base}{second_path}"

        server_responses[first_path] = {
            "results": [
                {"name": "bulbasaur", "url": "https://pokeapi.co/api/v2/pokemon/1/"},
                {"name": "ivysaur", "url": "https://pokeapi.co/api/v2/pokemon/2/"},
            ],
            "next": second_url,
        }
        server_responses[second_path] = {
            "results": [
                {"name": "venusaur", "url": "https://pokeapi.co/api/v2/pokemon/3/"},
                {"name": "charmander", "url": "https://pokeapi.co/api/v2/pokemon/4/"},
            ],
            "next": None,
        }

        reader = RestAPIReader()
        df = reader.read(
            spark,
            f"{server_base}/pokemon",
            source_config={
                "params": {"limit": 2},
                "records_field": "results",
                "pagination": {"mode": "response", "field": "next", "max_pages": 3},
                "parallelism": 1,
                "headers": {"User-Agent": "spark-fuse-tests"},
            },
        )

        ordered = [row.name for row in df.orderBy("name").collect()]
        assert ordered == ["bulbasaur", "charmander", "ivysaur", "venusaur"]
        assert headers_log
        assert all(header == "spark-fuse-tests" for header in headers_log if header is not None)
    finally:
        server.shutdown()
        server.server_close()
