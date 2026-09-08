"""Coinbase -> Kraken -> Binance referans fiyat problamasi (REST bacagi).

Gerekce (docs/decisions.md K-19): Binance'in genel API'si ABD kaynakli
IP'leri (GitHub Actions runner'lari dahil) HER SEFERINDE 451 ile
reddediyor. Binance'i sirada ilk tutmak, her job baslangicinda garantili
basarisiz bir ag turu demekti; bu yuzden sira Coinbase -> Kraken ->
Binance'e cevrildi (Coinbase ISO zaman damgasi tasiyor, Kraken tasimiyor
-- Kraken'e dusuldugunde feed_ts null olur ve staleness_ms
hesaplanamaz, ama en azindan deger elde edilir). Bu sandbox'ta bu
domainlere hic erisim olmadigi icin (egress proxy) sira degisikliginin
etkisi bu oturumda canli dogrulanamadi -- bu yuzden varsaymak yerine
runner her job basinda BIR KEZ gercekten problar, hangi borsanin
kullanildigi heartbeat ve `raw[]` uzerinden gorunur kalir.

Ucler: bkz. collector/endpoints.py (kaynak notlariyla).
"""

from dataclasses import dataclass, field
from typing import Optional

import httpx

from .endpoints import BINANCE_TICKER_URL, COINBASE_TICKER_URL, KRAKEN_TICKER_URL


def _parse_binance(raw: dict) -> float:
    return float(raw["price"])


def _parse_coinbase(raw: dict) -> float:
    return float(raw["price"])


def _parse_kraken(raw: dict) -> float:
    if raw.get("error"):
        raise ValueError(f"kraken error: {raw['error']}")
    result = raw["result"]
    first_pair = next(iter(result.values()))
    return float(first_pair["c"][0])


_EXCHANGES: tuple = (
    ("coinbase", COINBASE_TICKER_URL, _parse_coinbase),
    ("kraken", KRAKEN_TICKER_URL, _parse_kraken),
    ("binance", BINANCE_TICKER_URL, _parse_binance),
)
_EXCHANGE_BY_NAME = {name: (url, parser) for name, url, parser in _EXCHANGES}


@dataclass
class ExchangeAttempt:
    exchange: str
    ok: bool
    status_code: Optional[int] = None
    error: Optional[str] = None


@dataclass
class ExchangeFetchResult:
    exchange: Optional[str]
    value: Optional[float]
    raw: Optional[dict]
    attempts: list = field(default_factory=list)


async def probe_exchanges(client: httpx.AsyncClient) -> ExchangeFetchResult:
    """Sirayla Binance, Coinbase, Kraken dener; ilk basariliyi kullanir.

    Job baslangicinda bir kez cagrilir (bkz. runner.py); sonuc o run
    boyunca sabit kalir.
    """
    attempts = []
    for name, url, parser in _EXCHANGES:
        try:
            response = await client.get(url)
        except httpx.HTTPError as exc:
            attempts.append(ExchangeAttempt(name, ok=False, error=str(exc)))
            continue

        if response.status_code != 200:
            attempts.append(
                ExchangeAttempt(name, ok=False, status_code=response.status_code)
            )
            continue

        try:
            raw = response.json()
            value = parser(raw)
        except (ValueError, KeyError, TypeError) as exc:
            attempts.append(
                ExchangeAttempt(name, ok=False, status_code=response.status_code, error=str(exc))
            )
            continue

        attempts.append(ExchangeAttempt(name, ok=True, status_code=response.status_code))
        return ExchangeFetchResult(exchange=name, value=value, raw=raw, attempts=attempts)

    return ExchangeFetchResult(exchange=None, value=None, raw=None, attempts=attempts)


async def fetch_price(client: httpx.AsyncClient, exchange: str) -> ExchangeFetchResult:
    """Daha once problanmis, bilinen calisan borsadan tek fiyat cagrisi.

    Round basina yeniden fallback zinciri islemez -- yalnizca job
    basindaki `probe_exchanges` secimini kullanir. Basarisiz olursa
    caginin (sampler) `status: error` yazmasi icin bos sonuc doner.
    """
    url, parser = _EXCHANGE_BY_NAME[exchange]
    try:
        response = await client.get(url)
        response.raise_for_status()
        raw = response.json()
        value = parser(raw)
    except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
        return ExchangeFetchResult(
            exchange=exchange,
            value=None,
            raw=None,
            attempts=[ExchangeAttempt(exchange, ok=False, error=str(exc))],
        )
    return ExchangeFetchResult(
        exchange=exchange,
        value=value,
        raw=raw,
        attempts=[ExchangeAttempt(exchange, ok=True, status_code=response.status_code)],
    )
