"""CLOB market WebSocket: `book` (tam anlik goruntu) + `price_change` (delta).

Kaynak: Polymarket/agent-skills websocket.md (resmi repo) -- bkz.
collector/endpoints.py.

`book` event tam defter goruntusudur, cache'i sifirdan kurar. `price_change`
event'leri seviye bazli farktir (`side: BUY` -> bids, `SELL` -> asks,
`size: "0"` -> seviye kaldirilir); uygulanmazsa cache yalnizca abonelik
anindaki ilk `book` mesaji kadar guncel kalir ve WS bacagi anlamsizlasirdi
-- bu yuzden delta'lar da seviye haritasina uygulanip her degisiklikte
best5 yeniden hesaplanir.
"""

import json
from typing import Optional

from .book_transform import build_book_side
from .endpoints import CLOB_WS_PING_INTERVAL_SEC, CLOB_WS_PING_MESSAGE, CLOB_WS_URL
from .ws_client import PersistentWSClient


class ClobMarketWSClient:
    def __init__(
        self,
        *,
        connect_fn=None,
        now_ms_fn=None,
        sleep_fn=None,
        on_disconnect=None,
    ):
        self.cache: dict = {}
        self._levels: dict = {}  # asset_id -> {"bids": {price: size}, "asks": {price: size}}
        self._current_assets: list = []
        self._ws_client = PersistentWSClient(
            CLOB_WS_URL,
            on_message=self._handle_message,
            on_open=self._handle_open,
            on_disconnect=on_disconnect,
            connect_fn=connect_fn,
            ping_interval_sec=CLOB_WS_PING_INTERVAL_SEC,
            ping_message=CLOB_WS_PING_MESSAGE,
            now_ms_fn=now_ms_fn,
            sleep_fn=sleep_fn,
        )

    async def run(self) -> None:
        await self._ws_client.run()

    def stop(self) -> None:
        self._ws_client.stop()

    def _subscribe_message(self, asset_ids: list) -> str:
        return json.dumps(
            {"assets_ids": list(asset_ids), "type": "market", "custom_feature_enabled": True}
        )

    async def subscribe(self, asset_ids: list) -> None:
        """Yeni round'un token'larina abone olur. Onceki abonelikler ayrica
        iptal edilmiyor (resmi dokumanda unsubscribe formati yok); eski
        asset_id'lerden gelecek gecikmeli mesajlar zararsizca cache'te
        kalmaya devam eder, kullanilmayan round'lar icin sorgulanmaz."""
        self._current_assets = list(asset_ids)
        await self._ws_client.send(self._subscribe_message(self._current_assets))

    async def _handle_open(self, ws) -> None:
        if self._current_assets:
            await ws.send(self._subscribe_message(self._current_assets))

    async def _handle_message(self, raw_message: str) -> None:
        try:
            envelope = json.loads(raw_message)
        except (json.JSONDecodeError, TypeError):
            return

        event_type = envelope.get("event_type")
        if event_type == "book":
            self._apply_book_snapshot(envelope)
        elif event_type == "price_change":
            self._apply_price_changes(envelope)

    def _apply_book_snapshot(self, envelope: dict) -> None:
        asset_id = envelope.get("asset_id")
        if not asset_id:
            return
        bids = {float(level["price"]): float(level["size"]) for level in envelope.get("bids", [])}
        asks = {float(level["price"]): float(level["size"]) for level in envelope.get("asks", [])}
        self._levels[asset_id] = {"bids": bids, "asks": asks}
        self._recompute_cache(asset_id, envelope.get("timestamp"))

    def _apply_price_changes(self, envelope: dict) -> None:
        venue_ts = envelope.get("timestamp")
        for change in envelope.get("price_changes", []):
            asset_id = change.get("asset_id")
            if not asset_id:
                continue
            side_key = "bids" if change.get("side") == "BUY" else "asks"
            levels = self._levels.setdefault(asset_id, {"bids": {}, "asks": {}})
            price = float(change["price"])
            size = float(change["size"])
            if size <= 0:
                levels[side_key].pop(price, None)
            else:
                levels[side_key][price] = size
            self._recompute_cache(asset_id, venue_ts)

    def _recompute_cache(self, asset_id: str, venue_ts) -> None:
        levels = self._levels[asset_id]
        raw_bids = [{"price": p, "size": s} for p, s in levels["bids"].items()]
        raw_asks = [{"price": p, "size": s} for p, s in levels["asks"].items()]
        self.cache[asset_id] = {
            "book_side": build_book_side(raw_bids, raw_asks),
            "venue_ts_ms": int(venue_ts) if venue_ts is not None else None,
        }

    def snapshot(self, asset_id: str) -> Optional[dict]:
        return self.cache.get(asset_id)
