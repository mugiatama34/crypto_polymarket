"""longjob'un run'lar arasi devrettigi durum.

Turetilmis veridir, SCHEMA.md kapsaminda DEGIL -- bu yuzden `data/`
altina degil `state/`e yazilir. Silinip yeniden kurulabilir: dosya
yoksa/bozuksa longjob bir sonraki 300s-hizali round'dan baslar; en kotu
ihtimalle kucuk bir bosluk olusur ve bu heartbeat/coverage'ta zaten
gorunur olur (docs/decisions.md K-06). Ham gozlem veya sonuc kaydi
kaybi anlamina gelmez.
"""

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

DEFAULT_STATE_PATH = Path("state/longjob.json")


@dataclass
class LongjobState:
    last_processed_round_epoch_s: Optional[int] = None
    updated_at_ms: Optional[int] = None


def load_state(path: Path = DEFAULT_STATE_PATH) -> LongjobState:
    if not path.exists():
        return LongjobState()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return LongjobState()
    return LongjobState(
        last_processed_round_epoch_s=data.get("last_processed_round_epoch_s"),
        updated_at_ms=data.get("updated_at_ms"),
    )


def save_state(state: LongjobState, path: Path = DEFAULT_STATE_PATH) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = asdict(state)
    payload["_note"] = (
        "turetilmis durum, SCHEMA.md kapsaminda degil; dosya silinirse "
        "longjob bir sonraki round'dan yeniden kurulur"
    )
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path
