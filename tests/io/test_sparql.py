from __future__ import annotations

import json
import socketserver
import threading
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict, Iterable, Tuple
from urllib.parse import parse_qs

import pytest

from spark_fuse.io.sparql import SPARQLReader


class _ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True


ResponseKey = Tuple[str, str]


def _start_mock_server(
    responses: Dict[str | ResponseKey, Any],
    headers_log: list[str] | None = None,
    request_log: list[dict[str, Any]] | None = None,
) -> tuple[_ThreadedTCPServer, str]:
    class _Handler(BaseHTTPRequestHandler):
        def _respond(self, method: str, body: bytes | None = None):
            key: ResponseKey = (method, self.path)
            payload = responses.get(key)
            if payload is None:
                payload = responses.get(self.path)
            if payload is None:
                base_path = self.path.split("?", 1)[0]
                payload = responses.get((method, base_path)) or responses.get(base_path)
            if payload is None:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"{}")
                return

            encoded = json.dumps(payload).encode("utf-8")
            if headers_log is not None:
                headers_log.append(self.headers.get("Accept"))
            if request_log is not None:
                entry: dict[str, Any] = {"method": method, "path": self.path}
                if body:
                    entry["body"] = body
                request_log.append(entry)

            self.send_response(200)
            self.send_header("Content-Type", "application/sparql-results+json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self):
            self._respond("GET")

        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            self._respond("POST", body if body else None)

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
def test_sparql_reader_select_with_metadata(spark):
    responses: Dict[str | ResponseKey, Any] = {}
    headers_log: list[str] = []
    request_log: list[dict[str, Any]] = []
    server, server_base = _start_mock_server(responses, headers_log, request_log)
    try:
        path = "/sparql"
        responses[("POST", path)] = {
            "head": {"vars": ["name", "population", "species"]},
            "results": {
                "bindings": [
                    {
                        "name": {"type": "literal", "value": "Pikachu", "xml:lang": "en"},
                        "population": {
                            "type": "literal",
                            "value": "42",
                            "datatype": "http://www.w3.org/2001/XMLSchema#integer",
                        },
                        "species": {
                            "type": "uri",
                            "value": "https://example.org/pokemon/25",
                        },
                    }
                ]
            },
        }

        reader = SPARQLReader()
        df = reader.read(
            spark,
            f"{server_base}{path}",
            source_config={
                "query": "SELECT ?name ?population ?species WHERE { ?s ?p ?o }",
                "include_metadata": True,
            },
        )

        collected = _collect_rows(df)
        assert collected == [
            {
                "name": "Pikachu",
                "population": 42,
                "species": "https://example.org/pokemon/25",
                "name__type": "literal",
                "name__datatype": None,
                "name__xml:lang": "en",
                "population__type": "literal",
                "population__datatype": "http://www.w3.org/2001/XMLSchema#integer",
                "population__xml:lang": None,
                "species__type": "uri",
                "species__datatype": None,
                "species__xml:lang": None,
            }
        ]
    finally:
        server.shutdown()
        server.server_close()

    assert headers_log
    assert all(header and "application/sparql-results+json" in header for header in headers_log)
    assert request_log
    post_entries = [entry for entry in request_log if entry["method"] == "POST"]
    assert post_entries
    body_params = [
        parse_qs(entry["body"].decode("utf-8"))
        for entry in post_entries
        if "body" in entry and entry["body"]
    ]
    assert any("query" in params for params in body_params)


@pytest.mark.usefixtures("spark")
def test_sparql_reader_boolean_response(spark):
    responses: Dict[str | ResponseKey, Any] = {}
    server, server_base = _start_mock_server(responses)
    try:
        path = "/sparql"
        responses[("POST", path)] = {"boolean": True}

        reader = SPARQLReader()
        df = reader.read(
            spark,
            f"{server_base}{path}",
            source_config={"query": "ASK WHERE { ?s ?p ?o }"},
        )

        collected = _collect_rows(df)
        assert collected == [{"boolean": True}]
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.usefixtures("spark")
def test_sparql_reader_get_request(spark):
    responses: Dict[str | ResponseKey, Any] = {}
    request_log: list[dict[str, Any]] = []
    server, server_base = _start_mock_server(responses, request_log=request_log)
    try:
        path = "/sparql"
        responses[("GET", path)] = {
            "head": {"vars": ["name"]},
            "results": {"bindings": [{"name": {"type": "literal", "value": "Eevee"}}]},
        }

        reader = SPARQLReader()
        df = reader.read(
            spark,
            f"{server_base}{path}",
            source_config={
                "query": "SELECT ?name WHERE { ?s ?p ?o }",
                "request_type": "GET",
            },
        )

        names = {row.name for row in df.collect()}
        assert names == {"Eevee"}
    finally:
        server.shutdown()
        server.server_close()

    get_entries = [entry for entry in request_log if entry["method"] == "GET"]
    assert get_entries
    assert any("query=" in entry["path"] for entry in get_entries)
