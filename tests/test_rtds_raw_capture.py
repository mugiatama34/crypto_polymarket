"""scripts/rtds_raw_capture.py'nin saf mantik parcalari icin testler.

Gercek ag cagrisi yok: sahte bir WS baglantisi kullanilir. Bu script'in
kendisi gercek RTDS ucuna baglanmak icin tasarlandi ve buradan
calistirilmaz -- yalnizca ham yakalamanin filtrelemedigini ve kapanma
bilgisinin dogru cikarildigini dogrular.
"""

import json

import pytest
import websockets.exceptions

from scripts.rtds_raw_capture import (
    RTDS_TOPIC_BINANCE,
    _close_info_from_exc,
    _run,
    _subscription_message_no_filter,
)


def test_subscription_message_no_filter_omits_filters_key():
    message = json.loads(_subscription_message_no_filter(RTDS_TOPIC_BINANCE))
    subscriptions = message["subscriptions"]
    assert len(subscriptions) == 1
    assert subscriptions[0]["topic"] == RTDS_TOPIC_BINANCE
    assert "filters" not in subscriptions[0]


class _FakeWS:
    """`_run`'in iki dinleme asamasini (abonelik yok / abonelik var)
    tek bir mesaj kuyrugu uzerinden simule eder. Kuyruk bosaldiginda
    `recv()` kisa bir gercek uyku sonrasi TimeoutError firlatir --
    `_listen_raw` bunu deadline gibi yakalar (bkz. scripts/probe.py
    _FakeWS ile ayni desen)."""

    def __init__(self, messages, *, close_code=1000, close_reason=""):
        self._messages = list(messages)
        self.sent = []
        self.close_code = None
        self.close_reason = None
        self._final_close_code = close_code
        self._final_close_reason = close_reason
        self.closed = False

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

    assert any(e["kind"] == "subscription_sent" for e in result["events"])
    sent_subscription = json.loads(ws.sent[0])
    assert sent_subscription["subscriptions"][0]["topic"] == RTDS_TOPIC_BINANCE
    assert "filters" not in sent_subscription["subscriptions"][0]


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
