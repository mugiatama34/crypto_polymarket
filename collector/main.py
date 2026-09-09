"""Yerel calistirilabilir giris noktasi: `python -m collector.main`.

Actions workflow bir sonraki PR'da (bkz. CLAUDE.md kapsam). Bu dosya
yalnizca gercek baglantilarla (RTDS, CLOB WS, Gamma/CLOB/borsa REST)
longjob'u calistirir. Push varsayilan kapali -- acmak icin
`LONGJOB_GIT_PUSH=1` (bkz. collector/git_commit.py).

`LONGJOB_DURATION_SEC` ortam degiskeni ayarlanirsa `LongjobRunner`'a
job_duration_sec olarak gecirilir (kisa gercek kosumlar icin --
bkz. .github/workflows/longjob_shakedown.yml). Ayarlanmazsa
`LongjobRunner`'in kendi varsayilani (6 saat) kullanilir, davranis
degismez.

`COLLECTOR_WS_LEG_ENABLED` ortam degiskeni ws bacagini (RTDS + CLOB WS)
acar/kapatir -- varsayilan kapali (bkz. docs/decisions.md K-32). Kapaliyken
RTDS ve CLOB WS baglantilari hic kurulmaz.
"""

import asyncio
import logging
import os
import sys

import httpx

from .clob_ws import ClobMarketWSClient
from .clock import RealClock
from .rtds_ws import RTDSClient
from .runner import LongjobRunner

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("longjob")

DURATION_ENV_VAR = "LONGJOB_DURATION_SEC"
WS_LEG_ENABLED_ENV_VAR = "COLLECTOR_WS_LEG_ENABLED"


def _job_duration_sec_from_env() -> "int | None":
    value = os.environ.get(DURATION_ENV_VAR)
    if not value:
        return None
    return int(value)


def _ws_leg_enabled_from_env() -> bool:
    return os.environ.get(WS_LEG_ENABLED_ENV_VAR) == "1"


async def _amain() -> None:
    clock = RealClock()

    # on_disconnect burada degil, LongjobRunner.run() icinde baglanir --
    # runner heartbeat'i sahipleniyor, boslugu/hatayi heartbeat'e yazmasi
    # gerekiyor (bkz. docs/decisions.md K-06, K-23). Konsol logu da o
    # handler icinde.
    rtds_client = RTDSClient(now_ms_fn=clock.now_ms)
    clob_ws_client = ClobMarketWSClient(now_ms_fn=clock.now_ms)

    runner_kwargs = {}
    job_duration_sec = _job_duration_sec_from_env()
    if job_duration_sec is not None:
        runner_kwargs["job_duration_sec"] = job_duration_sec

    async with httpx.AsyncClient(timeout=10.0) as http_client:
        runner = LongjobRunner(
            http_client=http_client,
            rtds_client=rtds_client,
            clob_ws_client=clob_ws_client,
            clock=clock,
            ws_leg_enabled=_ws_leg_enabled_from_env(),
            **runner_kwargs,
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
