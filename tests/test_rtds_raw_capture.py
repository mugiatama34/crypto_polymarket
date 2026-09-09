"""scripts/rtds_raw_capture.py'nin saf mantik parcalari icin testler.

Gercek ag cagrisi yok: sahte bir WS baglantisi kullanilir. Bu script'in
kendisi gercek RTDS ucuna baglanmak icin tasarlandi ve buradan
calistirilmaz -- yalnizca ham yakalamanin filtrelemedigini, el sikisma
bilgisinin ve kapanma bilgisinin dogru cikarildigini dogrular.
"""

import json

import pytest
import websockets.exceptions
from websockets.datastructures import Headers

from scripts.rtds_raw_capture import (
    PHASE3_CANDIDATE_TOPICS,
    RTDS_TOPIC_BINANCE,
    _close_info_from_exc,
    _run,
    _subscription_message,
)


def test_subscription_message_omits_filters_key_for_single_topic():
    message = json.loads(_subscription_message((RTDS_TOPIC_BINANCE,), "update"))
    subscriptions = message["subscriptions"]
    assert len(subscriptions) == 1
    assert subscriptions[0]["topic"] == RTDS_TOPIC_BINANCE
    assert subscriptions[0]["type"] == "update"
    assert "filters" not in subscriptions[0]


def test_subscription_message_supports_multiple_candidate_topics():
    message = json.loads(_subscription_message(PHASE3_CANDIDATE_TOPICS, "*"))
    subscriptions = message["subscriptions"]
    assert {s["topic"] for s in subscriptions} == set(PHASE3_CANDIDATE_TOPICS)
    assert all(s["type"] == "*" and "filters" not in s for s in subscriptions)


class _FakeWS:
    """`_run`'in uc dinleme asamasini (abonelik yok / crypto_prices /
    aday crypto-disi topic'ler) tek bir mesaj kuyrugu uzerinden simule
    eder. Kuyruk bosaldiginda `recv()` kisa bir gercek uyku sonrasi
    TimeoutError firlatir -- `_listen_raw` bunu deadline gibi yakalar
    (bkz. scripts/probe.py _FakeWS ile ayni desen)."""

    def __init__(self, messages, *, close_code=1000, close_reason="", response_headers=None, subprotocol=None):
        self._messages = list(messages)
        self.sent = []
        self.close_code = None
        self.close_reason = None
        self._final_close_code = close_code
        self._final_close_reason = close_reason
        self.closed = False
        self.response_headers = response_headers if response_headers is not None else Headers([("Upgrade", "websocket")])
        self.subprotocol = subprotocol

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def send(self, message):
        self.sent.append(message)

    async def recv(self):
        if not self._messages:
            import asyncio

            await asyncio.sleep(0.02)
            raise TimeoutError
        return self._messages.pop(0)

    async def close(self):
        self.closed = True
        self.close_code = self._final_close_code
        self.close_reason = self._final_close_reason


@pytest.mark.asyncio
async def test_run_records_every_frame_regardless_of_shape_or_topic():
    """Onceki problar (scripts/probe.py) taninmayan sekildeki mesajlari
    (topic eslesmeyen, payload.value olmayan, JSON olmayan) sessizce
    atliyordu. Bu prob hicbirini atmamali -- hepsi ham olarak
    `events`e girmeli."""
    raw_frames = [
        "not json at all",
        '{"type": "welcome"}',
        '{"topic": "unknown_topic", "payload": {"no_value_field": 1}}',
        '{"topic": "crypto_prices", "payload": {"symbol": "btcusdt", "value": 123.4}}',
    ]
    ws = _FakeWS(raw_frames)

    result = await _run(connect_fn=lambda url: ws)

    assert result["connected"] is True
    assert result["frame_received_count"] == len(raw_frames)
    captured_raw = [e["raw"] for e in result["events"] if e["kind"] == "frame_received"]
    assert captured_raw == raw_frames


@pytest.mark.asyncio
async def test_run_sends_all_three_phase_subscriptions_in_order():
    ws = _FakeWS([])

    result = await _run(connect_fn=lambda url: ws)

    subscription_events = [e for e in result["events"] if e["kind"] == "subscription_sent"]
    assert [e["phase"] for e in subscription_events] == [2, 3]

    phase2_sent = json.loads(ws.sent[0])
    assert phase2_sent["subscriptions"][0]["topic"] == RTDS_TOPIC_BINANCE
    assert "filters" not in phase2_sent["subscriptions"][0]

    phase3_sent = json.loads(ws.sent[1])
    phase3_topics = {s["topic"] for s in phase3_sent["subscriptions"]}
    assert phase3_topics == set(PHASE3_CANDIDATE_TOPICS)
    assert all("filters" not in s for s in phase3_sent["subscriptions"])


@pytest.mark.asyncio
async def test_run_records_handshake_headers_and_subprotocol_on_success():
    headers = Headers([("Upgrade", "websocket"), ("X-Custom", "a"), ("X-Custom", "b")])
    ws = _FakeWS([], response_headers=headers, subprotocol="polymarket-rtds-v1")

    result = await _run(connect_fn=lambda url: ws)

    handshake = result["handshake"]
    assert handshake["status_code"] == 101
    assert handshake["status_code_source"] == "inferred_from_successful_connect"
    assert handshake["subprotocol"] == "polymarket-rtds-v1"
    assert ("X-Custom", "a") in handshake["response_headers"]
    assert ("X-Custom", "b") in handshake["response_headers"]


@pytest.mark.asyncio
async def test_run_records_handshake_failure_status_code_and_headers():
    headers = Headers([("Content-Type", "text/plain")])

    def connect_fn(url):
        raise websockets.exceptions.InvalidStatusCode(403, headers)

    result = await _run(connect_fn=connect_fn)

    assert result["connected"] is False
    handshake = result["handshake"]
    assert handshake["status_code"] == 403
    assert handshake["status_code_source"] == "observed_handshake_failure"
    assert ("Content-Type", "text/plain") in handshake["response_headers"]
    assert "403" in result["error"]


@pytest.mark.asyncio
async def test_run_records_close_code_and_reason_on_clean_close():
    ws = _FakeWS([], close_code=1000, close_reason="bye")

    result = await _run(connect_fn=lambda url: ws)

    assert result["connected"] is True
    assert ws.closed is True
    assert result["close_code"] == 1000
    assert result["close_reason"] == "bye"
    assert result["error"] is None


@pytest.mark.asyncio
async def test_run_connection_failure_is_captured_not_raised():
    def connect_fn(url):
        raise ConnectionRefusedError("nope")

    result = await _run(connect_fn=connect_fn)

    assert result["connected"] is False
    assert "nope" in result["error"]
    assert result["handshake"] is None


def _make_connection_closed(code: int, reason: str) -> websockets.exceptions.ConnectionClosed:
    from websockets.frames import Close, CloseCode

    close_code = CloseCode(code) if code in iter(CloseCode) else code
    frame = Close(close_code, reason)
    return websockets.exceptions.ConnectionClosed(rcvd=frame, sent=None)


def test_close_info_from_exc_prefers_rcvd_over_deprecated_properties():
    exc = _make_connection_closed(1011, "keepalive ping timeout")
    code, reason = _close_info_from_exc(exc)
    assert code == 1011
    assert reason == "keepalive ping timeout"
