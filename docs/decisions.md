# Kararlar ve Gerekçeleri

Bu dosya **neden** sorusunun cevabıdır. `CLAUDE.md` kuralları, `SCHEMA.md`
veri sözleşmesini tanımlar; burada o kuralların arkasındaki muhakeme durur.

Bir kuralı değiştirmeden önce buradaki gerekçesini oku. Gerekçe hâlâ
geçerliyse kural değişmez.

---

## K-01 — Bu bir ölçüm projesidir, işlem projesi değil

Polymarket, Almanya'yı kısıtlı ülke listesinde tutuyor ("close-only": mevcut
pozisyon kapatılabilir, yeni emir açılamaz). CLOB API emir göndermeden önce
coğrafi kontrol yapıyor ve Alman IP'lerini reddediyor. Almanya'nın oyun
otoritesi GGL, Eylül 2025'te bu tip platformlar için tüketici uyarısı
yayımladı.

Okuma uçları (Gamma, CLOB market data, Data API) herkese açık ve kimlik
doğrulaması gerektirmiyor.

**Sonuç:** proje veri toplama ve paper ölçümüyle sınırlı. Kısıtlamayı aşma
yöntemleri kapsam dışıdır ve tartışmaya açılmaz. Bu kural bir aşamada
gevşetilmez.

---

## K-02 — Kazanma oranı tek başına anlamsızdır

Bu piyasada fiyat, kalabalığın kazanma olasılığı tahminidir. 0.90'dan alım
yapıyorsan **başabaş kazanma oranın tam olarak 0.90'dır** (ücretler hariç).

| Giriş | Kazanç | 1 kayıp kaç kazancı siler | Başabaş oran |
|---|---|---|---|
| 0.85 | %17.6 | 5.7 | %85 |
| 0.90 | %11.1 | 9 | %90 |
| 0.95 | %5.3 | 19 | %95 |

%90 kazanma oranı başarı değil, nötr sonuç.

**Sonuç:** hiçbir rapor, dashboard veya çıktı kazanma oranını tek başına
göstermez. Her zaman ortalama giriş fiyatıyla birlikte, edge = (kazanma
oranı − ortalama giriş fiyatı) biçiminde ve güven aralığıyla sunulur.

---

## K-03 — Geriye dönük backtest mümkün değil

CLOB'un `/prices-history` ucu, çözülmüş marketlerde yalnızca 12 saat ve üzeri
granülarite döndürüyor. Bir 5 dakikalık marketin tüm ömrü 300 saniye. Yani
geçmiş bir turun fiyat yolu, spread'i ve defter derinliği geriye dönük
alınamıyor.

Alınabilen: sonuçlar (Gamma), ve Binance'ten geçmiş mum verisi.

Bu, **sinyalin** test edilebileceği ama **execution'ın** test edilemeyeceği
anlamına gelir. Ödenen fiyat bu stratejide sonucun tamamını belirlediği için,
yalnızca kazanma oranı üreten bir backtest yanıltıcıdır — iyi görünür ve
yanlıştır.

**Sonuç:** tek geçerli yol ileriye dönük veri toplamadır. Toplanmayan her gün
kalıcı kayıptır.

---

## K-04 — Skip kararları da kaydedilir

Filtre ilerleyen haftalarda değişecek. Yalnızca işleme girilen turlar
kaydedilirse, yeni bir filtreyi geçmiş veriyle test etmek imkânsız hale gelir
ve sayaç sıfırdan başlar.

**Sonuç:** her tur için `decision` yazılır; `action: "skip"` olanlarda da
`rule_id` ve `rule_inputs` doldurulur.

---

## K-05 — Sonuçlar ayrı akışa yazılır

Sonuç, karardan ~3 dakika sonra geliyor. Tur kaydına sonradan eklemek,
append-only ilkesini bozan tek nokta olurdu.

**Sonuç:** `outcomes.jsonl` ayrı bir akıştır, `round_id` ile analiz anında
birleştirilir. Uzlaştırıcı hata yaparsa ham gözlemler etkilenmez. PnL burada
hesaplanmaz — türetme katmanının işidir.

---

## K-06 — Boşluk da veridir

Toplayıcı 40 dakika düştüyse ve bu kaydedilmediyse, eksik veri sessizce
"sinyal yoktu" gibi görünür ve ölçümü bozar. Bu, geriye dönük düzeltilemeyen
hata türüdür.

**Sonuç:** heartbeat kayıtları (en az 60 saniyede bir `tick`), kaçırılan
gözlemler için `status: "missed"`, doğrulamayı geçemeyen satırlar için
`data/rejected/`. Hiçbir şey sessizce düşürülmez.

---

## K-07 — İki runner karşılaştırılıyor

`longjob` (6 saatlik döngü, kendini tetikler) ve `cron` (tur başına job)
paralel çalışıyor. Karşılaştırılan şey strateji değil, **kapsama ve
gecikme**:

- Kaç turun tamamı görüldü, kaçı kısmi, kaçı hiç görülmedi
- Hedef offset'lere sapma dağılımı (`offset_actual_sec`)
- Job başlangıç gecikmeleri ve boşluk süreleri
- Actions dakika tüketimi

İkisi aynı turu gördüğünde değerler karşılaştırılarak veri kalitesi de
doğrulanabilir: bir tarafta olmayan bir sapma varsa toplayıcı hatasıdır,
piyasa değil.

**Sonuç:** iki runner asla aynı dosyaya yazmaz. Ayrı dizinler, `runner=<id>`
deseniyle.

---

## K-08 — Oracle kaynağı Polymarket RTDS'tir

Marketler Chainlink BTC/USD feed'i ile çözülüyor. Polymarket'in kendi RTDS
WebSocket'inde `crypto_prices_chainlink` kanalı, btc/usd filtresiyle,
resolution'da kullanılan fiyatı doğrudan yayınlıyor. Zincirden okumaya gerek
yok.

Yan etki: RTDS bir WebSocket. `longjob` bunu tutabilir, `cron` tutamaz ve
REST'e düşer. İki runner farklı kalitede oracle verisi üretecek.

**Sonuç:** her fiyat kaydı kaynağını taşır (`source`) ve feed'in kendi zaman
damgasını (`feed_ts`) ayrı tutar. Kaynak yazılmazsa iki runner arasındaki
farkın nereden geldiği bilinemez.

---

## K-09 — Sinyal kaynağı çözüm kaynağı değildir

Karar Binance fiyatına bakılarak veriliyor, market Chainlink oracle ile
çözülüyor. Chainlink spot fiyatın 100-500 ms gerisinde olabiliyor ve turun
son saniyelerinde ikisi ayrışabilir.

**Sonuç:** `btc_reference` ve `btc_oracle` her gözlemde **ayrı ayrı**
kaydedilir. Biri diğerinden türetilmez, biri diğerinin yerine kullanılmaz.
(Alan adı sonradan `btc_binance` → `btc_reference` olarak değişti çünkü
REST bacağında referans fiyat Binance dışı borsalardan da gelebiliyor,
bkz. K-19.)

---

## K-10 — Üç saat ayrı kaydedilir

`venue_ts` (API'nin bildirdiği), `response_ts` (yanıtın alındığı),
`runner_ts` (çağrı öncesi yerel). Latency bu stratejide ana risk; ölçmenin
başka yolu yok.

`feed_ts` null olan gözlemlerde gecikme ölçülemez. Bu bilinen bir
eksikliktir, sürpriz değil — metrik katmanı o gözlemleri gecikme analizinden
dışlar ve sayısını raporlar.

---

## K-11 — Ham veri değiştirilmez

API yanıtları `raw` bloğunda geldiği gibi durur. Türetilmiş her şey ayrı isim
alanında. Reddedilen satırlar yeniden serileştirilmeden, ham string olarak
saklanır — kayıt zaten doğrulamayı geçemedi, yeniden serileştirmek hatanın
kendisini bozabilir.

JSONL kaynaktır. SQLite veya başka bir özet varsa JSONL'den yeniden
üretilebilir olmalıdır; silinince veri kaybı olmaz.

---

## K-12 — Derinlik seviye dizisi olarak tutulur

`bids_top5` / `asks_top5` `[[price, size], ...]` biçimindedir, tekil toplam
değil. Bu strateji taker olmak zorunda ve defter uçlarda ince; "şu boyuttaki
emrim dolar mıydı ve ortalama kaça dolardı" sorusu ancak seviyelerle
cevaplanır.

Seviyelerden toplam her zaman türetilir; tersi asla.

---

## K-13 — Alanlar arası tutarlılık iki yerde yaşıyor

JSON Schema draft-07 ile ifade edilebilenler şemada (`oracle_feed`
value↔source↔feed_ts). İki alanın değerini karşılaştırmayı gerektirenler
Python'da (`best_bid` ↔ `bids_top5[0]`), çünkü draft-07 bunu yapamıyor.

**Sonuç:** yeni bir tutarlılık kuralı eklerken her iki yere de bakılır.

---

## K-14 — Şema sürümü artar, veri dönüştürülmez

`schema_version` her kayıtta bulunur. Şema değişirse sürüm artar; eski veri
yeni şemaya çevrilmez, geriye dönük yazılmaz. İki sürüm yan yana yaşar,
analiz katmanı ikisini de okur.

Henüz veri toplanmadığı için sürüm hâlâ 1.

---

## K-15 — Sıralama: şema → toplayıcı → B stratejisi → metrikler

Önceki bir projede ölçüm altyapısı sonradan eklendi ve beş kez "bu
ölçülemiyor" duvarına çarpıldı. Bu projede sıra tersine çevrildi.

Metrik katmanı bilinçli olarak sona bırakıldı: ham veri doğruysa metrikler
her zaman geriye dönük türetilebilir. Tersi doğru değil.

**Sonuç:** metrik, dashboard, bildirim ve strateji optimizasyonu şu anki
fazın dışındadır. İstenirse "bu şu anki fazın dışında" denir ve devam
edilmez.

---

## K-16 — Karşılaştırma şeritleri karışmaz

İleriye dönük paper ölçümü (out-of-sample, birikerek) ile backtest
(in-sample, geçmişin tamamı elde) aynı tabloya konulamaz. Backtest her zaman
daha iyi görünür çünkü kendi geçmişine göre şekillenmiştir.

**Sonuç:** her kayıtta `data_lane` (`forward_paper` | `backtest`) bulunur.
Rapor katmanı iki şeridi karıştırırsa hata verir.

---

## K-17 — Dış kaynaklı kâr iddiaları kanıt değildir

Bu alanda kâr ekran görüntüsü paylaşan çok sayıda repo ve yazı var. Bunlarda
kayıp serisi, çekilen para, hesap yaşı ve örneklem büyüklüğü yok.

**Sonuç:** hiçbir dış kaynaklı performans iddiası bu projede referans,
hedef veya doğrulama olarak kullanılmaz. Yalnızca kendi topladığımız veri
sayılır.

---

## K-18 — Taşıma yolu runner içinde karşılaştırılır

`observations[]`'a `transport` (`ws` \| `rest`) eklendi. Aynı `offset_sec`
için birden fazla gözlem kaydı olabilir.

Değerlendirilen alternatifler:

- **(a) `longjob` hep `ws`, `cron` hep `rest` tutar.** Reddedildi — iki
  değişken (`runner_id` ve `transport`) aynı anda değişmiş olur. İki
  runner arasında bir fark görüldüğünde bunun kapsama/gecikme farkından mı
  (K-07), yoksa taşıma yolu farkından mı geldiği ayırt edilemez.
- **(b) İkisi de yalnızca `rest` tutar.** Reddedildi — WebSocket'in gerçek
  kalitesi (gecikme, kapsama) hiç ölçülmemiş olurdu; K-08'de zaten RTDS
  WebSocket'in `longjob` için mümkün, `cron` için REST'e düştüğü not
  edilmişti, ama bunu doğrulamak için WebSocket verisi toplanması gerekir.

**Seçilen:** her runner, her offset'te iki taşıma yolunu birden kaydeder.
Böylece:

- **Taşıma karşılaştırması** aynı runner içinde yapılır (kontrollü — tek
  değişken `transport`).
- **Runner karşılaştırması** (K-07) `rest` bacağı üzerinden yapılır
  (kontrollü — tek değişken `runner_id`, çünkü her iki runner da `rest`
  gözlemi tutar).

**Sonuç:** `offset_sec` üzerinde tekillik varsayan hiçbir kısıt yok —
şema veya doğrulayıcıda böyle bir kural varsa kaldırılır. `transport`
zorunlu alandır; eksikse veya `ws`/`rest` dışında bir değer taşıyorsa
satır reddedilir.

---

## K-19 — REST bacağının referans borsası WS bacağıyla aynı olmayabilir

WS bacağında `btc_reference`, Polymarket RTDS'in kendi Binance relay'idir
(`source: rtds_binance`, `venue: polymarket_rtds`) — her zaman bu ikili.
REST bacağında ise Binance'in genel API'si ABD merkezli IP'leri (GitHub
Actions runner'ları dahil) 451 ile reddedebiliyor. Bu durumda toplayıcı
Coinbase'e, o da olmazsa Kraken'e düşer.

Sonuç: K-18'in "taşıma karşılaştırması tek değişkenlidir (transport)"
varsayımı, borsa düşmesi tetiklendiğinde `btc_reference` için bozulabilir —
o run'da hem `transport` hem referans borsa değişmiş olur. Bu gizli
değildir: `venue` alanı hangi borsanın kullanıldığını doğrudan taşır
(`binance`/`coinbase`/`kraken` REST bacağında, `polymarket_rtds` WS
bacağında) — ayrıca her REST gözleminin `raw[]` girdisinde
(`endpoint: binance_ticker|coinbase_ticker|kraken_ticker`) ve `job_start`
heartbeat'inde özetlenir. `source` *nasıl* alındığını (`rest_poll`,
`rtds_binance`, ...) anlatır, `venue` *kimden* alındığını — aynı
`source: rest_poll` üç farklı `venue`'ye karşılık gelebildiği için ikisi
ayrı tutulur (bkz. SCHEMA.md 4.1.2). Metrik katmanı, borsa düşmesi
görülen run'ları `btc_reference` transport karşılaştırmasından `venue`
alanına bakarak ayırt edip ayrı değerlendirmelidir.

**Sonuç:** düşme sırası Binance → Coinbase → Kraken. Toplayıcı her job
başlangıcında (round başına değil) bir kez problar ve o run boyunca aynı
borsayı kullanır. Üçü de erişilemezse `btc_reference` REST gözlemi
`{value: null, source: "none", venue: "none", feed_ts: null}` ile
yazılır, atlanmaz.

---

## K-20 — latency_ms ve staleness_ms iki farklı gecikme kavramı ölçer

`ws` ve `rest` bacakları aynı alanı (`latency_ms`) iki farklı şey için
kullanıyordu: `rest`'te gerçek ağ turu, `ws`'te bellek içi cache
kopyalama süresi (tipik olarak sub-ms, pratikte hep `0`). İkincisi
uydurma bir sayı değil ama bilgi taşımıyor — `rest` satırlarının
yanında duran gerçek, değişken RTT değerleriyle karşılaştırıldığında
doldurulmamış/bozuk veri gibi okunuyor.

Alternatif olarak `latency_ms`'i "feed bayatlığı" (`runner_ts - feed_ts`)
anlamına çekmek de değerlendirildi, reddedildi — bu zaten K-10'da ayrı
tutulan üç zaman damgasının (`venue_ts`, `btc_reference.feed_ts`,
`btc_oracle.feed_ts`) hangisine karşılık geldiği belirsizleşir ve metrik
katmanı `transport`'a bakarak alanın anlamını yeniden yorumlamak zorunda
kalırdı.

**Sonuç:** iki ayrı alan. `latency_ms` (`int | null`) yalnızca ağ turunu
ölçer; `rest` bacağında dolu, `ws` bacağında anlamlı olmadığı için
`null`. `staleness_ms` (`int | null`) veri tazeliğini ölçer:
`response_ts - btc_reference.feed_ts`; `btc_reference.feed_ts` null ise
(REST'in çoğu ucu zaman damgası döndürmüyor, bkz. K-10) `staleness_ms`
da `null`. `btc_reference` seçildi çünkü karar bu fiyata bakılarak
veriliyor (K-09) — operasyonel olarak en ilgili olan bu. İkisi de her
zaman `observations[]` içinde zorunlu alan; eksik değil, `null` yazılır
(K-06 — sessiz atlama yok).

---

## K-21 — Keşif yolu tur bazında raw[]'da, toplu görünürlük heartbeat'te

Market keşfi iki yoldan biriyle olur (slug hızlı yol, listeleme yedek
yol — bkz. K-15 sonrası eklenen `gamma_client.discover_round_market`).
Hangi yolun kullanıldığı zaten her round kaydının `raw[]` girdisinde
duruyor (`endpoint: gamma_event_slug` | `gamma_event_listing`) — bu
kaybolmuyor, değişmiyor.

Ama slug deseni bozulursa (Polymarket format değiştirirse, ör.) bunu
fark etmenin tek yolu düzinelerce round kaydını açıp `raw[]` içindeki
`endpoint` alanını tek tek okumak olurdu. Round bazlı bir alan bunun
için yanlış yer: `discovery_method` her round'da zaten var, ayrıca bir
round alanına taşımak tekrar (aynı bilginin iki yerde durması) ve
şemayı büyütmek anlamına gelirdi.

**Sonuç:** `job_end` heartbeat'ine iki sayaç eklendi:
`discovery_slug_hits`, `discovery_listing_hits` (bkz. SCHEMA.md bölüm
6). Round alanı eklenmedi çünkü tur bazında sorgulanacak bir şey değil
— eğilim olarak izlenecek bir şey. Bu iki alan şemada **opsiyoneldir**:
yalnızca `job_end`de bulunur, `job_start`/`tick`/`error` event'lerinde
alan hiç yok (K-06'daki "null yazılır" deseninden farklı — burada alanın
kendisi bu event'lerde anlamsız olduğu için hiç yer almıyor, `null` bile
değil). Slug deseni bozulursa `discovery_listing_hits` job_end'de
yükselir, tek satırda görünür; hangi round'ların etkilendiği gerekirse
`raw[]`'a bakılır.

---

## K-22 — Event üst seviyesindeki `startDate` turun başlangıcı değildir

Prob (2026-09-08), `gamma_client.fetch_round_market`'in `open_ts` için
kullandığı aday alan listesinde `startDate`'in `startTime`'dan önce
denendiğini ve gerçek uçta `startDate`'in turun başlangıcı DEĞİL,
serinin ilk oluşturulma tarihi olduğunu ortaya çıkardı. Yanlış alan
kullanılırsa tüm offset hesabı (SCHEMA.md bölüm 3, `close_ts - offset_sec`
üzerinden hedef zaman) kayar — offset'ler gerçek turun kapanışına değil,
serinin çok eski bir referans noktasına göre hesaplanmış olur.

Aynı prob koşumunda `endDate` (18:35:00Z) turun gerçek bitişiyle
eşleşti — yani sorun yalnızca `startDate`'te, `endDate`'in eşdeğer bir
hatası yok.

**Sonuç:** `gamma_client._START_DATE_FIELDS`, `startDate`'i tamamen
çıkaracak şekilde değiştirildi: `("startTime", "eventStartTime",
"gameStartTime")`. Hiçbiri yoksa (mevcut davranış korunarak) slug'ın
kendisinin kodladığı başlangıç epoch'una düşülür. `_END_DATE_FIELDS`
(`endDate`, `endTime`, `gameEndTime`) değiştirilmedi — `endDate` için
eşdeğer bir hata gözlenmedi, varsayımla genişletilmedi.

---

## K-23 — Sessizlik eşikleri geçicidir, ölçümle sabitlenecek

RTDS bağlantısı teknik olarak açık kalıp mesaj akışı durabilir (K-08'de
bahsedilen `filters` biçimi hatası gibi bir abonelik sorunu sessizce
tekrar oluşursa, bağlantı kopmadan hiç mesaj gelmeyebilir). Tek bir "N
saniye sessizlik = yeniden bağlan" eşiği iki farklı ihtiyacı karıştırır:
erken uyarı (kapsama/ölçüm için — sessizlik başladığını hemen bilmek
istenir) ile geç toparlanma (gereksiz yeniden bağlanma churn'ünden
kaçınmak istenir) aynı sayı olmak zorunda değil. Chainlink ve Binance
relay'inin RTDS üzerindeki doğal mesajlar-arası yayın aralığı da farklı
olabilir; ölçülmeden tek bir sabit seçmek erken uyarıyı ya çok geç ya
da gereksiz sık tetikler.

**Sonuç:** iki ayrı, topic başına ayarlanabilir eşik.
`silence_warn_sec` aşılınca `RTDSClient` bir alert kuyruğuna yazar
(`drain_alerts()`), bağlantı korunur — runner bunu her tick'te
heartbeat'e `error` olarak yazar (K-06: boşluk da veridir). Yalnızca
`silence_reconnect_sec` aşılınca bağlantı zorla kapatılıp yeniden
kurulur (`PersistentWSClient.force_reconnect`); bu da heartbeat'e
yazılır. Varsayılan: `silence_warn_sec=30`, `silence_reconnect_sec=120`
— **bu sayılar geçicidir.** Chainlink/Binance topic'lerinin RTDS
üzerindeki gerçek mesajlar-arası gecikme dağılımı hiç ölçülmedi.
`scripts/probe.py`, 60 saniyelik bir dinleme penceresinde topic başına
min/medyan/maks/sayı raporlayacak şekilde genişletildi; bu ölçüm
alındığında eşikler gerçek dağılıma göre güncellenir, tahminle
sabitlenmiş haliyle bırakılmaz.

---

## K-24 — `/events` deprecated işaretli; risk bilinir ve izlenir, geçiş yok

Prob koşumu `probe_output/20260908T182954Z`'de hem `gamma_slug` hem
`gamma_listing` yanıtının HTTP header'larında şu üçlü aynen bulundu
(ikisinde de birebir aynı, `cf-cache-status: HIT` olmasına rağmen —
yani önbellek gürültüsü değil):

```
deprecation: true
sunset: Fri, 01 May 2026 00:00:00 GMT
warning: 299 - "use /events/keyset"
```

`sunset` tarihi (2026-05-01) prob koşum tarihinden (2026-09-08) önce —
yani uç, ilan edilmiş kapanış tarihini çoktan geçmiş olmasına rağmen
hâlâ 200 ile yanıt veriyor. Bu iki üretim yolunu da ilgilendiriyor:
`gamma_client.fetch_round_market` (slug, birincil keşif) ve
`fetch_round_market_via_listing` (yedek yol) ikisi de `GAMMA_EVENTS_PATH
= "/events"` üzerinden çalışıyor. Uç bir gün gerçekten kapatılırsa
keşif tamamen durur ve toplayıcı sessizce boş toplar (round'lar
`status: "missed"` ile yazılır ama neden hepsi kaçırılmaya başladığını
anlamak için önce burası akla gelmeli).

Halef olarak `warning` header'ının kendisi `/events/keyset`'i işaret
ediyor — bu bir tahmin değil, Polymarket'in kendi yanıtından alınan
birebir metin. Ama yanıt şekli (hangi query parametreleri geçerli,
alan adları `/events` ile aynı mı) doğrulanmadı.

**Sonuç:** Şimdilik `/events`'te kalınır, hiçbir kod `/events/keyset`'e
geçirilmez. `scripts/probe.py`'ye `/events/keyset`'i aday parametrelerle
prob eden ayrı, üretime bağlanmayan problar eklendi (aynı `_run`
içinde, `/events` problarıyla yan yana) — amaç yanıt şeklini ölçmek.
Geçiş (varsa) ayrı bir PR'da, bu ölçüm sonucu okunduktan sonra ele
alınır. Bu bilinen ve izlenen bir risktir, keşfedilmemiş bir boşluk
değil — CLAUDE.md'nin "tahmin üretme, sor" kuralı burada "ölç, sonra
karar ver" olarak uygulanıyor.

---

## K-25 — `rtds_ws.py`'nin sessiz `return`'leri K-06'ya aykırı ama şu anki sıfırın sebebi değil

`RTDSClient._handle_message` (collector/rtds_ws.py) üç noktada sessizce
`return` ediyor: JSON parse hatası, `topic` `self.cache`'te yoksa,
`payload` dict değilse veya `value` alanı yoksa (üçüncüsü "initial data
dump veya tanınmayan şekil" yorumuyla bilinçli eklenmişti). Hiçbirinde
sayaç/log yok — K-06'nın ("boşluk da veridir") ihlali: gerçek bir
çerçeve gelip tanınmayan şekilde olsa bile, sessizlik izleyicisi (K-23)
bunu "hiç mesaj yok" sayıp yanıltıcı bir "sessizlik" alarmı üretir;
alarm "sunucu sustu" der ama aslında "biz tanımadığımız bir çerçeveyi
attık" olabilir.

Ama bu, `probe_output/20260908T214511Z`'deki `message_count: 0` /
`count: 0` sonucunun açıklaması DEĞİL. O çıktıyı üreten
`scripts/probe.py._probe_rtds` (`rtds_messages.json`) hiçbir filtre
uygulamıyor — JSON olsun olmasın her çerçeveyi ham olarak listeye
ekliyor — ve o da sıfır gösterdi. Filtrelemeyen bir prob da sıfır
gösterdiği için, sorunun `_handle_message`'ın seçiciliğinden değil,
bağlantının kendisinden (ya da RTDS'in bu bağlantıya hiç çerçeve
yollamamasından) geldiği düşünülüyor — ama `_probe_rtds`'in pencere
süresi kısa (13s) ve tek denemeydi; `rtds_gap_distribution.json` (60s,
her iki topic) ise `_handle_message` ile AYNI filtreyi taşıyor
(`topic in arrivals and payload.value var`), o yüzden onun `count: 0`'ı
tek başına kesin kanıt sayılmaz.

**Sonuç:** `_handle_message`'ın üç sessiz `return`'ü ayrı bir işte
düzeltilecek (en azından bir sayaç/log eklenecek) — bu bilinen bir
bug'dır ama bu PR'a karıştırılmadı. `scripts/rtds_raw_capture.py`
probu bu ayrımı netleştirmek için eklendi: hiçbir filtre uygulamadan,
önce abonelik göndermeden dinler (sunucu kendiliğinden bir şey yolluyor
mu), sonra `crypto_prices`'a filtresiz abone olur, sonra tamamen farklı
(crypto-dışı, aday) topic'lere filtresiz abone olur — ayrıca HTTP el
sıkışma ayrıntısını (durum kodu, response header'ları, seçilen
subprotocol) ve WS kapanma kodu/sebebini kaydeder. Bu ölçüm
sonuçlanmadan `_handle_message`'ın düzeltmesi de, sıfırın kök nedeni
hakkında bir karar da verilmez.
