Kararların gerekçesi için `docs/decisions.md` dosyasını oku ve bağlayıcı
kabul et. Bir kuralı değiştirmeden önce oradaki gerekçesine bak.

# Proje Kuralları

Bu bir **ölçüm projesidir.** Amacı, Polymarket 5 dakikalık BTC up/down
marketlerinde bir stratejinin edge'ini güven aralığıyla birlikte ölçmektir.
İşlem yapmak amacı değildir.

---

## Kapsam kilidi — bu repo asla içermez

- Emir imzalama veya gönderme kodu
- Cüzdan private key'i, seed phrase, `.env`'de kimlik bilgisi
- `py-clob-client`'ın order fonksiyonları (`create_order`, `post_order`, vb.)
- Coğrafi kısıtlama aşma girişimi (proxy, VPN yönlendirme)
- Getiri tahmini, "bu strateji iyidir" değerlendirmesi, hedef kâr sayısı

Sadece herkese açık, kimlik doğrulaması gerektirmeyen okuma uçları kullanılır:
Gamma API, CLOB market data, Data API.

**Gerekçe:** Almanya Polymarket'in kısıtlı ülke listesinde. Canlı işlem bu
projenin çıktısı değil. Bu kural bir aşamada gevşetilmez — gerekirse ayrı bir
proje açılır.

---

## Mevcut faz: son analiz (K-38)

"C-minimal + A toplayıcı" fazı kapandı. Bu projenin son işi, K-38'de
sonuç görülmeden kayda geçirilmiş **tek kural ve tek kriterin** ölçümüdür.

Yapılacak (sırayla, her biri ayrı commit):
1. `scripts/fetch_outcomes.py` + `workflow_dispatch` workflow'u —
   tek seferlik sonuç çekme (sürekli uzlaştırıcı değil)
2. `scripts/final_analysis.py` — K-38'in tanımladığı rapor

Kural, veri bölünmesi, başarı kriteri ve karar akışı **K-38'dedir ve
değiştirilmez.** `longjob` çalışmaya devam eder (ikinci test penceresi
için, bkz. K-38).

**Yapılmayacak:**
- Kural/parametre ayarlama, optimizasyon, keşif yarısına veya kovalara
  göre kural değiştirme
- Sürekli uzlaştırıcı, `cron` runner, dashboard, Telegram, rapor sitesi
- B stratejisi (OKX/Binance momentum)

Bu maddeler istenirse: "bu şu anki fazın dışında" de ve devam etme.

---

## Değişmez kurallar

1. **Ham veri değiştirilmez.** Yazılan satır güncellenmez, silinmez.
   Sonuçlar ayrı akışa yazılır, tur kaydına eklenmez.
2. **İki runner ayrı dizinlere yazar.** Ortak dosya yok.
3. **Şema değişikliği = `schema_version` artışı.** Eski veri dönüştürülmez,
   geriye dönük yazılmaz.
4. **Skip kararları ve kaçırılan gözlemler kaydedilir.** Sessiz atlama yok.
5. **Doğrulamayı geçemeyen satır `data/rejected/`'a yazılır**, düşürülmez.
6. **Üç saat ayrı kaydedilir:** `venue_ts`, `response_ts`, `runner_ts`.

---

## Git davranışı

- `longjob`: ~15 dakikada bir commit. Çökerse kayıp en fazla 15 dakika.
- `cron`: iş sonunda commit.
- Her push öncesi `pull --rebase`, çakışmada tekrar dene.
- Gün devrinde dünkü dosyalar gzip'lenir.
- Dosya boyutu beklenenin üstüne çıkarsa uyarı — sessizce şişmesin.

---

## Çalışma şekli

- Kod yazmadan önce plan sun, onay bekle.
- Bir seferde tek bileşen. Toplayıcı ve uzlaştırıcı aynı PR'da olmaz.
- Şemaya aykırı bir ihtiyaç doğarsa: kodu şemaya uydurma, **şemayı tartışmaya
  aç.**
- Belirsizlik varsa varsayım üretme, sor.
