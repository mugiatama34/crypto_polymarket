"""Runner'in zaman soyutlamasi -- testlerin saatler/gunler beklemeden
round donguisunu calistirabilmesi icin (bkz. collector/runner.py).
"""

import asyncio
import time


class RealClock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)

    async def sleep_until_ms(self, target_ms: int) -> None:
        delay_s = max(0.0, (target_ms - self.now_ms()) / 1000.0)
        await asyncio.sleep(delay_s)


class FakeClock:
    """Testler icin: sleep_until_ms gercekte beklemez, saati aninda
    hedefe ileri sarar. Gercek asyncio.sleep(0) ile kontrolu event
    loop'a birakir ki es zamanli sahte WS feed'leri isleyebilsin."""

    def __init__(self, start_ms: int):
        self._now_ms = start_ms

    def now_ms(self) -> int:
        return self._now_ms

    async def sleep_until_ms(self, target_ms: int) -> None:
        if target_ms > self._now_ms:
            self._now_ms = target_ms
        await asyncio.sleep(0)
