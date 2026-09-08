"""RTDS WebSocket: crypto_prices (Binance relay) + crypto_prices_chainlink.

Kaynak: @polymarket/real-time-data-client (resmi npm paketi) README.md ve
src/model.ts -- bkz. collector/endpoints.py. Abonelik tek mesajla, iki
topic birden; her mesaj zarfi `{topic, type, timestamp, payload,
connection_id}`, payload (CryptoPrice) `{symbol, timestamp(ms), value}`.

Baglanti kurulunca sunucu "initial data dump" da gonderebiliyor (README:
`{symbol, data: [...]}` sekli) -- bu mesajlarda `payload.value` yok, bu
yuzden atlaniyor; yalnizca tekil guncellemeler ("value" alani olan)
cache'e yazilir.
"""

import json
from typing import Optional

from .endpoints import (
    RTDS_BTC_SYMBOL,
    RTDS_PING_INTERVAL_SEC,
    RTDS_PING_MESSAGE,
    RTDS_SUBSCRIPTION_TYPE,
    RTDS_TOPIC_BINANCE,
    RTDS_TOPIC_CHAINLINK,
    RTDS_WS_URL,
)
from .ws_client import PersistentWSClient

TOPICS = (RTDS_TOPIC_BINANCE, RTDS_TOPIC_CHAINLINK)


class RTDSClient:
    def __init__(
        self,
        *,
        connect_fn=None,
        now_ms_fn=None,
        sleep_fn=None,
        on_disconnect=None,
    ):
        self.cache: dict = {topic: None for topic in TOPICS}
        self._ws_client = PersistentWSClient(
            RTDS_WS_URL,
            on_message=self._handle_message,
            on_open=self._handle_open,
            on_disconnect=on_disconnect,
            connect_fn=connect_fn,
            ping_interval_sec=RTDS_PING_INTERVAL_SEC,
            ping_message=RTDS_PING_MESSAGE,
            now_ms_fn=now_ms_fn,
            sleep_fn=sleep_fn,
        )

    async def run(self) -> None:
        await self._ws_client.run()

    def stop(self) -> None:
        self._ws_client.stop()

    async def _handle_open(self, ws) -> None:
        subscription = {
            "subscriptions": [
                {
                    "topic": topic,
                    "type": RTDS_SUBSCRIPTION_TYPE,
                    "filters": json.dumps({"symbol": RTDS_BTC_SYMBOL}),
                }
                for topic in TOPICS
            ]
        }
        await ws.send(json.dumps(subscription))

    async def _handle_message(self, raw_message: str) -> None:
        try:
            envelope = json.loads(raw_message)
        except (json.JSONDecodeError, TypeError):
            return

        topic = envelope.get("topic")
        if topic not in self.cache:
            return

        payload = envelope.get("payload")
        if not isinstance(payload, dict) or "value" not in payload:
            return  # initial data dump veya taninmayan sekil

        value = payload.get("value")
        feed_ts = payload.get("timestamp")
        self.cache[topic] = {
            "value": float(value) if value is not None else None,
            "feed_ts_ms": int(feed_ts) if feed_ts is not None else None,
        }

    def snapshot(self, topic: str) -> Optional[dict]:
        return self.cache.get(topic)
