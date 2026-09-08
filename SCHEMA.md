# Veri Sözleşmesi — v1

Bu dosya toplayıcının ne yazacağını tanımlar. Metrik, strateji veya rapor
tanımı **içermez**. Tek amacı: ham veriyi eksiksiz ve değiştirilemez tutmak.

`schema_version: 1` — her kayıt bu alanı taşır. Şema değişirse sürüm artar,
eski veri dönüştürülmez.

---

## 1. Temel ilkeler

1. **Append-only.** Yazılmış hiçbir satır güncellenmez veya silinmez.
2. **Ham dokunulmaz.** API yanıtları `raw` bloğunda geldiği gibi durur.
   Türetilmiş her şey ayrı isim alanında.
3. **Tek yazar, tek dosya.** Her runner kendi dizinine yazar. İki runner asla
   aynı dosyaya dokunmaz.
4. **JSONL kaynaktır.** SQLite veya başka bir özet varsa, JSONL'den yeniden
   üretilebilir olmalıdır. Silinince veri kaybı olmaz.
5. **Boşluk da veridir.** Görülmeyen tur, düşen job, kaçırılan örnekleme
   kaydedilir. Eksik veri sessizce "sinyal yoktu" gibi görünmemelidir.

---

## 2. Dizin düzeni

```
data/raw/runner=<runner_id>/date=<YYYY-MM-DD>/rounds.jsonl
data/coverage/runner=<runner_id>/date=<YYYY-MM-DD>/heartbeat.jsonl
data/outcomes/date=<YYYY-MM-DD>/outcomes.jsonl
```

`runner_id`: `longjob` | `cron`

Gün devrinde bir önceki günün dosyaları gzip'lenir. Bugünün dosyası
sıkıştırılmaz.

---

## 3. Örnekleme offset'leri

Turun kapanışına kalan saniye cinsinden. Karar anı ~t-120 olduğu için o
pencere sıklaştırılmıştır; uçlar bağlam ve geç fiyat referansı içindir.

```
çapa:  240, 180
yoğun: 135, 130, 125, 120, 115, 110, 105
çapa:  60, 30, 10
```

Tur başına, offset başına taşıma yolu sayısı kadar gözlem. Aynı
`offset_sec` için birden fazla gözlem kaydı olabilir — biri `transport:
"ws"`, biri `transport: "rest"` (bkz. 4.1). Bir offset bir taşıma
yolunda kaçırılırsa o gözlem atlanmaz — `status: "missed"` ile yazılır.

---

## 4. Tur kaydı (`rounds.jsonl`)

Bir satır = bir 5 dakikalık market.

| Alan | Tip | Not |
|---|---|---|
| `schema_version` | int | 1 |
| `runner_id` | string | `longjob` \| `cron` |
| `job_id` | string | Bu turu yazan job çalıştırması |
| `data_lane` | string | `forward_paper` \| `backtest` |
| `round_id` | string | Market slug (`btc-updown-5m-<ts>`) |
| `condition_id` | string | Gamma'dan |
| `token_ids` | object | `{up: "...", down: "..."}` |
| `open_ts` | int | Market'in bildirdiği açılış, epoch ms UTC |
| `close_ts` | int | Market'in bildirdiği kapanış, epoch ms UTC |
| `observations` | array | Bkz. 4.1 |
| `decision` | object \| null | Bkz. 4.2 |
| `status` | string | `complete` \| `partial` \| `missed` |
| `raw` | array | Ham API yanıtları, sırayla |

### 4.1 `observations[]`

| Alan | Tip | Not |
|---|---|---|
| `offset_sec` | int | Hedeflenen offset |
| `offset_actual_sec` | float | Gerçekleşen. Sapma burada görünür. |
| `venue_ts` | int | API'nin bildirdiği zaman |
| `response_ts` | int | Yanıtın alındığı zaman |
| `runner_ts` | int | Yerel saat, çağrı öncesi |
| `latency_ms` | int | `response_ts - runner_ts` |
| `transport` | string | `ws` \| `rest` — bu gözlem hangi taşıma yoluyla alındı |
| `book` | object | `{up: {...}, down: {...}}`, bkz. 4.1.1 |
| `btc_binance` | object | Referans fiyat, bkz. 4.1.2 |
| `btc_oracle` | object | Chainlink — çözüm kaynağı, bkz. 4.1.2 |
| `status` | string | `ok` \| `partial` \| `missed` \| `error` |
| `error` | string \| null | Varsa hata metni |

> Not: `venue_ts`, defterin (CLOB) zaman damgasıdır. Fiyat feed'lerinin
> kendi zaman damgaları ayrı ayrı `btc_binance.feed_ts` /
> `btc_oracle.feed_ts` içindedir — bkz. 4.1.2.

#### 4.1.1 `book` (her token için)

| Alan | Tip | Not |
|---|---|---|
| `best_bid` | float | `bids_top5[0][0]` ile tutarlı olmalı |
| `best_ask` | float | `asks_top5[0][0]` ile tutarlı olmalı |
| `bid_size` | float | `bids_top5[0][1]` ile tutarlı olmalı |
| `ask_size` | float | `asks_top5[0][1]` ile tutarlı olmalı |
| `spread` | float | |
| `mid` | float | |
| `bids_top5` | array | `[[price, size], ...]` — en iyiden başlayarak en fazla 5 seviye |
| `asks_top5` | array | `[[price, size], ...]` — en iyiden başlayarak en fazla 5 seviye |

Sıra garantilidir: `index 0` = en iyi fiyat seviyesi (best). Defterde
5'ten az seviye varsa dizi kısa kalır — **doldurma yapılmaz**. Boş dizi
(`[]`) geçerlidir (o tarafta hiç seviye yoksa).

`best_bid`/`bid_size` alanları, dizi boş değilse `bids_top5[0]` ile;
`best_ask`/`ask_size` alanları `asks_top5[0]` ile tutarlı olmak
zorundadır. Doğrulayıcı bu tutarlılığı kontrol eder; uyuşmazsa satır
reddedilir.

#### 4.1.2 `btc_binance` / `btc_oracle`

| Alan | Tip | Not |
|---|---|---|
| `value` | float \| null | Fiyat. `null` ise feed'ten okunamadı. |
| `source` | string | `rtds_chainlink` \| `rtds_binance` \| `rest_poll` \| `onchain` \| `none` |
| `feed_ts` | int \| null | Feed'in kendi bildirdiği zaman — bizim `response_ts`'imiz değil. Oracle gecikmesini ölçmenin tek yolu bu. |

Tutarlılık kuralları:

- `value: null` ⟺ `source: "none"` ve `feed_ts: null`. Değer yoksa
  kaynak ve zaman damgası da yoktur.
- `value` doluysa `source` `"none"` olamaz.
- `feed_ts: null` ile `value` dolu olması geçerlidir — bazı REST uçları
  zaman damgası döndürmez (`source: "rest_poll"` tipik örnek). Bu
  bilinçli bir gevşeklik; katılaştırmadan önce burayı tartışmaya aç.

### 4.2 `decision`

| Alan | Tip | Not |
|---|---|---|
| `ts` | int | Karar anı |
| `action` | string | `enter` \| `skip` — **skip de yazılır** |
| `side` | string \| null | `up` \| `down` |
| `rule_id` | string | Hangi kural tetikledi/engelledi |
| `rule_inputs` | object | Kuralın gördüğü değerler |
| `entry_price_assumed` | float \| null | |
| `entry_price_basis` | string | `best_ask` — varsayım sayının yanında durur |
| `size_shares` | float \| null | |
| `capital_at_risk` | float \| null | Mutlak tutar, yüzde değil |
| `cost_model` | object | `{fee, spread_cost, slippage_assumed, formula_id}` |

`skip` kararlarında da `rule_inputs` doldurulur. Filtre değişirse geçmiş
turlar yeniden değerlendirilebilsin diye.

---

## 5. Sonuç kaydı (`outcomes.jsonl`)

Ayrı akış. Tur kaydının **üzerine yazılmaz**, analiz anında `round_id` ile
birleştirilir.

| Alan | Tip |
|---|---|
| `schema_version` | int |
| `round_id` | string |
| `resolved_ts` | int |
| `outcome` | string — `up` \| `down` \| `invalid` |
| `resolution_source` | string |
| `open_price` | float \| null |
| `close_price` | float \| null |
| `raw` | object |

PnL burada hesaplanmaz. Türetme katmanının işi.

---

## 6. Kapsama kaydı (`heartbeat.jsonl`)

| Alan | Tip | Not |
|---|---|---|
| `schema_version` | int | |
| `runner_id` | string | |
| `job_id` | string | |
| `event` | string | `job_start` \| `job_end` \| `tick` \| `error` |
| `ts` | int | |
| `rounds_seen` | int \| null | `job_end` için |
| `rounds_missed` | int \| null | `job_end` için |
| `detail` | string \| null | |

`tick` en az 60 saniyede bir yazılır. İki tick arasındaki boşluk = kapsama
kaybı.

---

## 7. Doğrulama

Toplayıcı her satırı yazmadan önce şemaya karşı doğrular. Doğrulama
başarısızsa satır `data/rejected/` altına, hata sebebiyle birlikte yazılır —
sessizce düşürülmez.

Alanlar arası tutarlılık kuralları iki yerde yaşar:

- **JSON Schema içinde** (`schemas/round.schema.json`), `if`/`then` ile ifade
  edilebilenler — ör. `oracle_feed`'de `value` ↔ `source`/`feed_ts`.
- **Python'da** (`validator/core.py`), draft-07'nin ifade edemediği
  karşılaştırmalar — ör. `book_side`'da `best_bid`/`bid_size` ↔
  `bids_top5[0]` (iki alanın *değerini* birbirine göre karşılaştırmak
  gerekir, draft-07 bunu yapamaz).

Yeni bir tutarlılık kuralı eklerken önce ikisine de bakılmalı: ifade
edilebiliyorsa şemaya, edilemiyorsa doğrulayıcıya eklenir.
