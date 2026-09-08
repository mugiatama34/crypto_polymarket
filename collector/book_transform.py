"""Ham bids/asks listesini SCHEMA.md 4.1.1 `book_side` seklinde kurar.

REST (`/book`) ve WS (`book` event) ayni `[{"price": ..., "size": ...}, ...]`
sekliyle geliyor (bkz. collector/endpoints.py kaynak notlari), bu yuzden tek
bir donusturucu ikisi tarafindan da kullanilir.

API'nin dondurdugu sira garantisine guvenilmiyor; en iyi seviye burada
acikca sec ilir (bid icin en yuksek fiyat, ask icin en dusuk fiyat).
Boyutu 0 olan seviyeler (kaldirilmis) elenir.
"""


def _sorted_levels(raw_levels, *, best_first_key, limit=5):
    parsed = []
    for level in raw_levels or []:
        price = float(level["price"])
        size = float(level["size"])
        if size <= 0:
            continue
        parsed.append((price, size))
    parsed.sort(key=best_first_key)
    return parsed[:limit]


def build_book_side(raw_bids, raw_asks) -> dict:
    """Bos taraf gecerlidir (SCHEMA.md 4.1.1); best_bid/best_ask o durumda
    0.0 sentineldir -- gercek bir fiyat okumasi degildir, bids_top5/
    asks_top5 zaten bos dizi tasir ve dogrulayici tutarlilik kontrolunu
    bu durumda atlar (bkz. validator/core.py)."""
    bids_top5 = [[p, s] for p, s in _sorted_levels(raw_bids, best_first_key=lambda ps: -ps[0])]
    asks_top5 = [[p, s] for p, s in _sorted_levels(raw_asks, best_first_key=lambda ps: ps[0])]

    best_bid, bid_size = (bids_top5[0][0], bids_top5[0][1]) if bids_top5 else (0.0, 0.0)
    best_ask, ask_size = (asks_top5[0][0], asks_top5[0][1]) if asks_top5 else (0.0, 0.0)

    if bids_top5 and asks_top5:
        mid = (best_bid + best_ask) / 2
    elif bids_top5:
        mid = best_bid
    elif asks_top5:
        mid = best_ask
    else:
        mid = 0.0

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "spread": best_ask - best_bid,
        "mid": mid,
        "bids_top5": bids_top5,
        "asks_top5": asks_top5,
    }
