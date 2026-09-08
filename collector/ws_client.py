"""Kalici baglantili, otomatik yeniden baglanan WS istemci taban sinifi.

RTDS ve CLOB market kanallari ayni desende calisir: ac, abone ol, gelen
mesajlari bir cache'e yaz, koparsa yeniden bagla ve kopukluk suresini
disariya bildir (SCHEMA.md K-06 -- "bosluk da veridir", sessiz toparlanma
yok). Bu dosya o ortak desenin tek kopyasi; rtds_ws.py ve clob_ws.py
mesaj/cache semantigini uzerine kurar.

`connect_fn` disaridan enjekte edilir (varsayilan `websockets.connect`) --
testler gercek ag yerine sahte bir baglanti nesnesi verir.
"""

import asyncio
import contextlib
import time
from typing import Awaitable, Callable, Optional

import websockets


class PersistentWSClient:
    def __init__(
        self,
        url: str,
        *,
        on_message: Callable[[str], Awaitable[None]],
        on_open: Optional[Callable[[object], Awaitable[None]]] = None,
        on_disconnect: Optional[Callable[[int, Optional[str]], Awaitable[None]]] = None,
        connect_fn=None,
        ping_interval_sec: float = 5.0,
        ping_message: str = "ping",
        reconnect_delay_sec: float = 1.0,
        max_reconnect_delay_sec: float = 30.0,
        now_ms_fn: Optional[Callable[[], int]] = None,
        sleep_fn: Optional[Callable[[float], Awaitable[None]]] = None,
    ):
        self._url = url
        self._on_message = on_message
        self._on_open = on_open
        self._on_disconnect = on_disconnect
        self._connect_fn = connect_fn or websockets.connect
        self._ping_interval_sec = ping_interval_sec
        self._ping_message = ping_message
        self._reconnect_delay_sec = reconnect_delay_sec
        self._max_reconnect_delay_sec = max_reconnect_delay_sec
        self._now_ms = now_ms_fn or (lambda: int(time.time() * 1000))
        self._sleep = sleep_fn or asyncio.sleep
        self._stop = False
        self._ws = None
        self._forced_reason: Optional[str] = None

    async def run(self) -> None:
        delay = self._reconnect_delay_sec
        disconnected_at: Optional[int] = None

        while not self._stop:
            try:
                async with self._connect_fn(self._url) as ws:
                    self._ws = ws
                    if disconnected_at is not None:
                        duration_ms = self._now_ms() - disconnected_at
                        if self._on_disconnect:
                            await self._on_disconnect(duration_ms, self._forced_reason)
                        disconnected_at = None
                        self._forced_reason = None
                    delay = self._reconnect_delay_sec

                    if self._on_open:
                        await self._on_open(ws)

                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        async for raw_message in ws:
                            await self._on_message(raw_message)
                    finally:
                        ping_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await ping_task
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            finally:
                self._ws = None

            if self._stop:
                break
            if disconnected_at is None:
                disconnected_at = self._now_ms()
            await self._sleep(delay)
            delay = min(delay * 2, self._max_reconnect_delay_sec)

    async def _ping_loop(self, ws) -> None:
        while True:
            await self._sleep(self._ping_interval_sec)
            await ws.send(self._ping_message)

    async def send(self, message: str) -> None:
        if self._ws is not None:
            await self._ws.send(message)

    async def force_reconnect(self, reason: Optional[str] = None) -> None:
        """Mevcut baglantiyi disaridan kapatir; `run()` dongusu normal
        yeniden baglanma yoluna duser (delay/backoff sifirlanir, on_open
        tekrar cagrilir). Sessizlik gibi hata FIRLATMAYAN durumlar icin --
        gercek baglanti hatalarinda zaten ayni yol otomatik isliyor.

        `reason`, bir sonraki basarili baglantida `on_disconnect`'e
        `error` olarak iletilir; organik kopmalarda bu deger hep `None`
        kalir (bkz. testler) -- yalnizca bilinçli force_reconnect
        cagrisi bir sebep tasir."""
        self._forced_reason = reason
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()

    def set_on_disconnect(self, callback: Optional[Callable[[int, Optional[str]], Awaitable[None]]]) -> None:
        self._on_disconnect = callback

    def stop(self) -> None:
        self._stop = True
