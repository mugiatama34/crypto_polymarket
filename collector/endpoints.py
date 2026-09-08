"""Doğrulanmış uç nokta ve kanal sabitleri.

Her sabitin kaynağı yorum olarak yanında durur. Tahmin edilen hiçbir URL
yok — burada olmayan bir uca ihtiyaç doğarsa önce doğrulanır, sonra
eklenir (bkz. CLAUDE.md "Çalışma şekli").
"""

# Gamma API — market/event keşfi. Kaynak: Polymarket/agent-skills market-data.md
GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
GAMMA_EVENTS_PATH = "/events"

# GAMMA_EVENTS_PATH deprecated (bkz. docs/decisions.md K-24) -- yanıtın
# kendi `Warning` header'ı halef olarak bunu işaret ediyor, tahmin değil.
# Yalnızca scripts/probe.py'de ölçüm için kullanılır; hiçbir üretim
# kodu (gamma_client.py) buna geçirilmedi.
GAMMA_EVENTS_KEYSET_PATH = "/events/keyset"

# CLOB REST — sipariş defteri. Kaynak: py-clob-client (py_clob_client/endpoints.py,
# client.py) — GET_ORDER_BOOK = "/book", query param "token_id".
CLOB_REST_BASE_URL = "https://clob.polymarket.com"
CLOB_BOOK_PATH = "/book"

# CLOB market WebSocket. Kaynak: Polymarket/agent-skills websocket.md.
CLOB_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CLOB_WS_PING_INTERVAL_SEC = 10.0
CLOB_WS_PING_MESSAGE = "PING"

# RTDS WebSocket. Kaynak: @polymarket/real-time-data-client (resmi npm paketi,
# src/client.ts DEFAULT_HOST, src/model.ts, README.md).
#
# Sembol formati topic'e gore FARKLI -- prob dogruladi (bkz.
# docs/decisions.md K-19 sonrasi RTDS notlari): crypto_prices_chainlink
# "btc/usd" (kucuk harf, egik cizgi) bekliyor, crypto_prices "btcusdt"
# bekliyor. Ikisine ayni sembolu (ornegin "BTCUSDT") gondermek sunucunun
# sessizce hic mesaj yollamamasina yol aciyordu (Polymarket/rs-clob-client
# issue #136 ile ayni belirti).
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
RTDS_PING_INTERVAL_SEC = 5.0
RTDS_PING_MESSAGE = "PING"
RTDS_TOPIC_BINANCE = "crypto_prices"
RTDS_TOPIC_CHAINLINK = "crypto_prices_chainlink"
RTDS_SUBSCRIPTION_TYPE = "update"
RTDS_SYMBOL_BINANCE = "btcusdt"
RTDS_SYMBOL_CHAINLINK = "btc/usd"

# Borsa REST uçları — exchange_probe.py'de sırayla denenir (bkz.
# docs/decisions.md K-19). Binance ABD kaynaklı IP'leri 451 ile
# reddedebiliyor; bu yüzden sıra ve fallback var, tek bir uca güvenilmiyor.
BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
COINBASE_TICKER_URL = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
KRAKEN_TICKER_URL = "https://api.kraken.com/0/public/Ticker?pair=XBTUSD"
