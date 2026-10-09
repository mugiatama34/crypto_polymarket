# crypto_polymarket

> **Durum: kapandı (K-39, 9 Ekim 2026).** Veri ve kod arşiv olarak kalır;
> zamanlanmış workflow'lar kapatıldı.

Bir ölçüm projesi — işlem yapmaz (bkz. `CLAUDE.md`, `docs/decisions.md` K-01).

## Kapanış özeti

**Soru.** Polymarket 5 dakikalık BTC up/down marketlerinde, turun 120 sn offset'li
gözleminde `best_ask`'i 0.80–0.99 aralığında olan (favori) tarafı almak, ücret
sonrası pozitif edge veriyor mu?

**Yöntem.** Sonuç görülmeden kayda geçirilmiş tek kural ve tek kriter
(K-38): 10 hisse, top-5 ask derinliğinden VWAP giriş, `0.07 × p × (1 − p)`
ücret varsayımı. Veri keşif (9–23 Eylül, açıklayıcı) ve test (24 Eylül –
8 Ekim, karar) yarılarına bölündü. Kriter: test yarısında 1× ücretle işlem
başına ortalama net PnL'in bootstrap %95 GA alt sınırı > 0.

**Sonuç.** Geçmedi. Test yarısı: 1735 işlem, edge −0.0031 (Wilson %95
[−0.0169, 0.0087]), işlem başına net PnL −0.079 $, %95 GA alt sınırı
−0.21 $. Rapor: [`final_report/20261009T111938Z/`](final_report/20261009T111938Z/report.txt).

**Bulgu — kalibrasyon.** Kazanma oranı her giriş fiyatı kovasında giriş
fiyatına eşit: dört kovanın hepsinde, iki yarıda da edge'in %95 aralığı
sıfırı içeriyor (kovalar açıklayıcıdır). Ücret sonrası ortalama net PnL
iki yarıda da negatif ölçüldü.

İkinci test penceresi (9–22 Ekim) analiz edilmedi — K-38'e göre yalnızca
test yarısı geçerse gerekiyordu.
