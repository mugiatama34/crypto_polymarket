"""scripts/probe.py'nin saf mantik parcalari icin testler.

Gercek ag cagrisi yok: httpx.MockTransport ve sahte bir WS baglantisi
kullanilir. Bu script'in kendisi (main/_run) gercek uclara baglanmak
icin tasarlandi ve buradan calistirilmaz -- yalnizca yardimci
fonksiyonlar test edilir.
"""

import json

import httpx
import pytest

from scripts.probe import _extract_first_token_id, _probe_http, _probe_rtds, _write_json


def _sample_event():
    return {
        "id": "evt-1",
        "conditionId": "0xabc",
        "outcomes": json.dumps(["Up", "Down"]),
        "clobTokenIds": json.dumps(["111", "222"]),
    }


def test_extract_first_token_id_valid_event():
    assert _extract_first_token_id(_sample_event()) == "111"


def test_extract_first_token_id_malformed_event_returns_none_not_exception():
    assert _extract_first_token_id({"id": "broken"}) is None


def test_write_json_creates_parent_dirs_and_readable_content(tmp_path):
    path = tmp_path / "nested" / "out.json"
    _write_json(path, {"a": 1})
    assert json.loads(path.read_text(encoding="utf-8")) == {"a": 1}


@pytest.mark.asyncio
async def test_probe_http_success_captures_status_and_body():
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"ok": True}))
    async with httpx.AsyncClient(transport=transport) as client:
        result = await _probe_http(client, "test_probe", "GET", "https://example.test/x")

    assert result["status_code"] == 200
    assert result["body_json"] == {"ok": True}
    assert result["error"] is None


@pytest.mark.asyncio
async def test_probe_http_connection_error_is_captured_not_raised():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await _probe_http(client, "test_probe", "GET", "https://example.test/x")

    assert result["status_code"] is None
    assert "refused" in result["error"]


class _FakeWS:
    def __init__(self, messages):
        self._messages = list(messages)
        self.sent = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def send(self, message):
        self.sent.append(message)

    async def recv(self):
        if not self._messages:
            import asyncio

            await asyncio.sleep(0.05)
            raise TimeoutError  # test suresi cok uzamasin
        return self._messages.pop(0)


@pytest.mark.asyncio
async def test_probe_rtds_captures_messages_and_subscribes_both_topics():
    ws = _FakeWS(['{"topic": "crypto_prices", "payload": {"value": 1}}'])
    result = await _probe_rtds(connect_fn=lambda url: ws)

    assert result["connected"] is True
    assert result["message_count"] == 1
    sent = json.loads(ws.sent[0])
    topics = {s["topic"] for s in sent["subscriptions"]}
    assert topics == {"crypto_prices", "crypto_prices_chainlink"}


@pytest.mark.asyncio
async def test_probe_rtds_connection_failure_is_captured():
    def connect_fn(url):
        raise ConnectionRefusedError("nope")

    result = await _probe_rtds(connect_fn=connect_fn)
    assert result["connected"] is False
    assert "nope" in result["error"]
