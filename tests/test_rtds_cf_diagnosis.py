"""scripts/rtds_cf_diagnosis.py'nin saf mantik parcalari icin testler.

Gercek ag cagrisi yok: sahte bir WS baglantisi kullanilir (bkz.
tests/test_rtds_raw_capture.py ile ayni desen). `_run()` (tum run'i
calistirip probe_output/ altina gercek dosya yazan ust seviye
fonksiyon) kasitli olarak test edilmiyor -- tests/test_probe_script.py
de ayni sebeple scripts/probe.py._run'i cagirmiyor, yalnizca saf alt
parcalari."""

import json

import pytest
import websockets.exceptions
from websockets.datastructures import Headers

from scripts.rtds_cf_diagnosis import (
    ORIGIN_HEADER_VALUE,
    REPRESENTATIVE_BROWSER_USER_AGENT,
    RTDS_TOPIC_BINANCE,
    SHAPE_A1_ACTION_FIELD,
    SHAPE_A2_CURRENT,
    SHAPE_A3_SINGLE_OBJECT,
    SHAPE_A4_SINGULAR_KEY,
    _extract_cf_signals,
    _run_connection,
)


def test_shape_a1_adds_action_field_on_top_of_current_shape():
    a1 = json.loads(SHAPE_A1_ACTION_FIELD)
    a2 = json.loads(SHAPE_A2_CURRENT)
    assert a1["action"] == "subscribe"
    assert a1["subscriptions"] == a2["subscriptions"]


def test_shape_a2_is_production_envelope_with_filters():
    a2 = json.loads(SHAPE_A2_CURRENT)
    subscriptions = a2["subscriptions"]
    assert len(subscriptions) == 1
    sub = subscriptions[0]
    assert sub["topic"] == RTDS_TOPIC_BINANCE
    assert sub["type"] == "update"
    assert "filters" in sub
    assert json.loads(sub["filters"]) == {"symbol": "btcusdt"}


def test_shape_a3_drops_subscriptions_wrapper_but_keeps_fields():
    a2_sub = json.loads(SHAPE_A2_CURRENT)["subscriptions"][0]
    a3 = json.loads(SHAPE_A3_SINGLE_OBJECT)
    assert "subscriptions" not in a3
    assert a3 == a2_sub


def test_shape_a4_wraps_single_object_in_singular_subscription_key():
    a2_sub = json.loads(SHAPE_A2_CURRENT)["subscriptions"][0]
    a4 = json.loads(SHAPE_A4_SINGULAR_KEY)
    assert set(a4.keys()) == {"subscription"}
    assert a4["subscription"] == a2_sub


def test_extract_cf_signals_detects_cookie_and_ray():
    handshake = {
        "response_headers": [
            ("Date", "Wed, 09 Sep 2026 06:31:56 GMT"),
            ("set-cookie", "__cf_bm=abc123; HttpOnly; Secure"),
            ("CF-RAY", "a384271f3f7f8663-IAD"),
        ]
    }
    signals = _extract_cf_signals(handshake)
    assert signals == {"cf_bm_cookie_present": True, "cf_ray": "a384271f3f7f8663-IAD"}


def test_extract_cf_signals_absent_when_no_cf_headers():
    handshake = {"response_headers": [("Upgrade", "websocket")]}
    signals = _extract_cf_signals(handshake)
    assert signals == {"cf_bm_cookie_present": False, "cf_ray": None}


def test_extract_cf_signals_handles_missing_handshake():
    assert _extract_cf_signals(None) == {"cf_bm_cookie_present": False, "cf_ray": None}


class _FakeWS:
    """`_run_connection`'in tek bir senaryosunu simule eder -- bkz.
    tests/test_rtds_raw_capture.py._FakeWS ile ayni desen."""

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
async def test_run_connection_sends_message_and_records_frames():
    ws = _FakeWS(["frame one", "frame two"])

    result = await _run_connection(
        "probe_name",
        subscribe_message=SHAPE_A2_CURRENT,
        listen_seconds=0.05,
        send_app_pings=False,
        connect_fn=lambda url, **kwargs: ws,
    )

    assert result["connected"] is True
    assert ws.sent == [SHAPE_A2_CURRENT]
    assert result["frame_received_count"] == 2
    captured_raw = [e["raw"] for e in result["events"] if e["kind"] == "frame_received"]
    assert captured_raw == ["frame one", "frame two"]
    assert result["close_code"] == 1000
    assert result["error"] is None


@pytest.mark.asyncio
async def test_run_connection_without_subscribe_message_sends_nothing():
    ws = _FakeWS([])

    result = await _run_connection(
        "passive",
        subscribe_message=None,
        listen_seconds=0.02,
        send_app_pings=False,
        connect_fn=lambda url, **kwargs: ws,
    )

    assert ws.sent == []
    assert result["frame_received_count"] == 0
    assert result["subscribe_message_sent"] is None


@pytest.mark.asyncio
async def test_run_connection_passes_origin_and_user_agent_to_connect_fn():
    ws = _FakeWS([])
    received_kwargs = {}

    def connect_fn(url, **kwargs):
        received_kwargs.update(kwargs)
        return ws

    result = await _run_connection(
        "with_headers",
        subscribe_message=None,
        listen_seconds=0.02,
        send_app_pings=False,
        origin=ORIGIN_HEADER_VALUE,
        user_agent_header=REPRESENTATIVE_BROWSER_USER_AGENT,
        connect_fn=connect_fn,
    )

    assert received_kwargs == {
        "origin": ORIGIN_HEADER_VALUE,
        "user_agent_header": REPRESENTATIVE_BROWSER_USER_AGENT,
    }
    assert result["origin_header_sent"] == ORIGIN_HEADER_VALUE
    assert result["user_agent_header_sent"] == REPRESENTATIVE_BROWSER_USER_AGENT


@pytest.mark.asyncio
async def test_run_connection_omits_origin_and_user_agent_kwargs_by_default():
    ws = _FakeWS([])
    received_kwargs = {}

    def connect_fn(url, **kwargs):
        received_kwargs.update(kwargs)
        return ws

    await _run_connection(
        "no_headers",
        subscribe_message=None,
        listen_seconds=0.02,
        send_app_pings=False,
        connect_fn=connect_fn,
    )

    assert received_kwargs == {}


@pytest.mark.asyncio
async def test_run_connection_records_cf_signals_from_handshake():
    headers = Headers(
        [
            ("set-cookie", "__cf_bm=xyz; HttpOnly; Secure"),
            ("CF-RAY", "deadbeef-IAD"),
        ]
    )
    ws = _FakeWS([], response_headers=headers)

    result = await _run_connection(
        "cf_check",
        subscribe_message=None,
        listen_seconds=0.02,
        send_app_pings=False,
        connect_fn=lambda url, **kwargs: ws,
    )

    assert result["cf_signals"] == {"cf_bm_cookie_present": True, "cf_ray": "deadbeef-IAD"}


@pytest.mark.asyncio
async def test_run_connection_handshake_failure_is_captured_not_raised():
    headers = Headers([("Content-Type", "text/plain")])

    def connect_fn(url, **kwargs):
        raise websockets.exceptions.InvalidStatusCode(403, headers)

    result = await _run_connection(
        "failing",
        subscribe_message=None,
        listen_seconds=0.02,
        send_app_pings=False,
        connect_fn=connect_fn,
    )

    assert result["connected"] is False
    assert result["handshake"]["status_code"] == 403
    assert "403" in result["error"]
    assert result["cf_signals"] == {"cf_bm_cookie_present": False, "cf_ray": None}


@pytest.mark.asyncio
async def test_run_connection_protocol_frame_capture_is_empty_with_fake_ws():
    """FakeWS gercek `websockets` protokol makinesinden gecmiyor, o
    yuzden `websockets.protocol` logger'i hic tetiklenmez -- bu, gercek
    baglantida protokol cercevelerinin YAKALANAMAYACAGI anlamina gelmez,
    sadece sahte testte bu yolun cagrilmadigini dogrular (crash etmeden)."""
    ws = _FakeWS(["hello"])

    result = await _run_connection(
        "protocol_capture_smoke",
        subscribe_message=None,
        listen_seconds=0.02,
        send_app_pings=False,
        connect_fn=lambda url, **kwargs: ws,
    )

    assert result["protocol_frame_received_count"] == 0
    assert result["protocol_frame_sent_count"] == 0
