"""Dogrulamayi gecemeyen satirlari data/rejected/ altina yazan yardimci.

Duzen data/raw/ ile birebir ayni desendedir: runner=<id>/date=<d>/. Round ve
heartbeat kayitlarini yazan iki runner (longjob, cron) kendi dizinine yazar;
outcome kayitlarini ureten uzlastirici de "reconciler" adiyla ucuncu bir
yazar olarak ayni desene girer. Boylece iki runner hicbir zaman ayni dosyaya
dokunmaz.

Reddedilen kaydin orijinal icerigi (raw_line) hicbir sekilde yeniden
serilestirilmez veya ayristirilmaz - oldugu gibi string olarak saklanir.
Kayit zaten dogrulamayi gecemedi; yeniden serilestirmek hatanin kendisini
bozabilir.

Bu dosyalar gun devrinde gzip'lenmez, arsivlenmez - az sayida olmalari
beklenir ve gorunur kalmalari istenir (toplayici/gun-devri isi bu PR'in
kapsaminda degil).
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_BASE_DIR = Path("data/rejected")


def write_rejected(
    raw_line: str,
    errors: list[str],
    *,
    runner_id: str,
    record_type: str | None = None,
    job_id: str | None = None,
    ts: int | None = None,
    base_dir: Path = DEFAULT_BASE_DIR,
) -> Path:
    """Reddedilen bir satiri data/rejected/runner=<id>/date=<d>/rejected.jsonl'a ekler.

    raw_line: reddedilen kaydin orijinal, degistirilmemis string hali.
    errors: validate()'in dondurdugu hata mesajlari.
    runner_id: "longjob" | "cron" | "reconciler" - hangi yazar reddetti.
    record_type: tahmin edilebiliyorsa "round" | "outcome" | "heartbeat".
    job_id: reddeden job'un kimligi, biliniyorsa.
    ts: epoch ms; verilmezse yazma anindaki yerel zaman kullanilir.

    Returns: yazilan dosyanin yolu.
    """
    ts = ts if ts is not None else int(time.time() * 1000)
    date_str = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

    dest_dir = Path(base_dir) / f"runner={runner_id}" / f"date={date_str}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / "rejected.jsonl"

    entry = {
        "ts": ts,
        "runner_id": runner_id,
        "job_id": job_id,
        "record_type": record_type,
        "errors": errors,
        "raw_line": raw_line,
    }

    with dest_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    return dest_path
