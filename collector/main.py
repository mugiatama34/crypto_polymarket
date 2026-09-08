"""Yerel calistirilabilir giris noktasi: `python -m collector.main`.

Actions workflow bir sonraki PR'da (bkz. CLAUDE.md kapsam). Bu dosya
yalnizca gercek baglantilarla (RTDS, CLOB WS, Gamma/CLOB/borsa REST)
longjob'u calistirir. Push varsayilan kapali -- acmak icin
`LONGJOB_GIT_PUSH=1` (bkz. collector/git_commit.py).
"""

import asyncio
import logging
import sys

import httpx

from .clob_ws import ClobMarketWSClient
from .clock import RealClock
from .rtds_ws import RTDSClient
from .runner import LongjobRunner

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("longjob")


async def _amain() -> None:
    clock = RealClock()

    async def on_rtds_disconnect(duration_ms, error):
        logger.warning("RTDS koptu, %sms sonra yeniden baglanildi (error=%s)", duration_ms, error)

    async def on_clob_disconnect(duration_ms, error):
        logger.warning("CLOB WS koptu, %sms sonra yeniden baglanildi (error=%s)", duration_ms, error)

    rtds_client = RTDSClient(now_ms_fn=clock.now_ms, on_disconnect=on_rtds_disconnect)
    clob_ws_client = ClobMarketWSClient(now_ms_fn=clock.now_ms, on_disconnect=on_clob_disconnect)

    async with httpx.AsyncClient(timeout=10.0) as http_client:
        runner = LongjobRunner(
            http_client=http_client,
            rtds_client=rtds_client,
            clob_ws_client=clob_ws_client,
            clock=clock,
        )
        await runner.run()


def main() -> None:
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, cikiliyor")
        sys.exit(0)


if __name__ == "__main__":
    main()
