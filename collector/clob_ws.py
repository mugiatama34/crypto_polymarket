"""CLOB market WebSocket: `book` (tam anlik goruntu) + `price_change` (delta).

Kaynak: Polymarket/agent-skills websocket.md (resmi repo) -- bkz.
collector/endpoints.py.

`book` event tam defter goruntusudur, cache'i sifirdan kurar. `price_change`
event'leri seviye bazli farktir (`side: BUY` -> bids, `SELL` -> asks,
`size: "0"` -> seviye kaldirilir); uygulanmazsa cache yalnizca abonelik
anindaki ilk `book` mesaji kadar guncel kalir ve WS bacagi anlamsizlasirdi
-- bu yuzden delta'lar da seviye haritasina uygulanip her degisiklikte
best5 yeniden hesaplanir.

Bu ws bacagi su an `COLLECTOR_WS_LEG_ENABLED` ile kapali (bkz.
docs/decisions.md K-32) -- shakedown #3'te (6/6 tur, 72/72 ws-gozlem)
`cache` hic dolmadi, sebebi olcumle dogrulanmadi (K-33, ayri is).

K-25'in CLOB WS karsiligi: `_handle_message` daha once JSON-degil ve
asset_id-eksik durumlarda sessizce dusuyordu, hicbir sayac yoktu (RTDS'in
K-29 oncesi hali gibi). Simdi RTDS'teki `dropped_not_json`/
`dropped_unknown_symbol`/`dropped_unknown_shape` desenine paralel uc
sayac tutuluyor -- `dropped_unknown_event_type` (RTDS'teki sembol
eslesmemesinin analogu, burada ayirt edici alan `event_type`) ve
`dropped_unknown_shape` (asset_id eksik, ya da `price`/`size` sayiya
cevrilemiyor). `_apply_book_snapshot`/`_apply_price_changes` artik
FIRLATMAK yerine (RTDS'teki `_extract_price` gibi) basari/basarisizlik
donduruyor -- bu hem sayilabilirlik hem de guvenlik icin: eskiden
bozuk bir cerceve `_handle_message`'i patlatip `PersistentWSClient.run()`
icindeki genel `except Exception: pass`'e dusuyor, bu da gercek bir
baglanti hatasi gibi goruntu vererek gereksiz bir yeniden-baglanma
dongusune yol aciyordu -- sessizce, sayilmadan.
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
        # K-25 karsiligi: sessizce dusen cerceveler icin gorunurluk (bkz.
        # modul docstring'i). Yalnizca sayilir, cercevenin kendisi hala
        # kaydedilmiyor -- ayni sinir RTDS'te de var.
        self.dropped_not_json = 0
        self.dropped_unknown_event_type = 0
        self.dropped_unknown_shape = 0
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
            self.dropped_not_json += 1
            return

        event_type = envelope.get("event_type")
        if event_type == "book":
            applied = self._apply_book_snapshot(envelope)
        elif event_type == "price_change":
            applied = self._apply_price_changes(envelope)
        else:
            self.dropped_unknown_event_type += 1
            return

        if not applied:
            self.dropped_unknown_shape += 1

    def _apply_book_snapshot(self, envelope: dict) -> bool:
        asset_id = envelope.get("asset_id")
        if not asset_id:
            return False
        try:
            bids = {float(level["price"]): float(level["size"]) for level in envelope.get("bids", [])}
            asks = {float(level["price"]): float(level["size"]) for level in envelope.get("asks", [])}
        except (KeyError, TypeError, ValueError):
            return False
        self._levels[asset_id] = {"bids": bids, "asks": asks}
        self._recompute_cache(asset_id, envelope.get("timestamp"))
        return True

    def _apply_price_changes(self, envelope: dict) -> bool:
        venue_ts = envelope.get("timestamp")
        changes = envelope.get("price_changes")
        if not isinstance(changes, list) or not changes:
            return False
        applied_any = False
        for change in changes:
            asset_id = change.get("asset_id") if isinstance(change, dict) else None
            if not asset_id:
                continue
            try:
                side_key = "bids" if change.get("side") == "BUY" else "asks"
                levels = self._levels.setdefault(asset_id, {"bids": {}, "asks": {}})
                price = float(change["price"])
                size = float(change["size"])
            except (KeyError, TypeError, ValueError):
                continue
            if size <= 0:
                levels[side_key].pop(price, None)
            else:
                levels[side_key][price] = size
            self._recompute_cache(asset_id, venue_ts)
            applied_any = True
        return applied_any

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
