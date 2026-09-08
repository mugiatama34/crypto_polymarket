"""Heartbeat kaydedici: job_start/job_end/tick/error (SCHEMA.md bolum 6).

`tick` en az 60 saniyede bir yazilmali (K-06 -- bosluk da veridir).
`due_for_tick()` runner'in ana donguisunde bu garantiyi kontrol etmesi
icindir.
"""

import time
from pathlib import Path
from typing import Callable, Optional

from . import writer

TICK_INTERVAL_SEC = 60


class HeartbeatWriter:
    def __init__(
        self,
        *,
        runner_id: str,
        job_id: str,
        now_ms_fn: Optional[Callable[[], int]] = None,
        base_dir: Optional[Path] = None,
    ):
        self.runner_id = runner_id
        self.job_id = job_id
        self._now_ms = now_ms_fn or (lambda: int(time.time() * 1000))
        self._base_dir = base_dir
        self.last_tick_ms: Optional[int] = None

    def _write(self, event: str, *, rounds_seen=None, rounds_missed=None, detail=None) -> Path:
        record = {
            "schema_version": 1,
            "runner_id": self.runner_id,
            "job_id": self.job_id,
            "event": event,
            "ts": self._now_ms(),
            "rounds_seen": rounds_seen,
            "rounds_missed": rounds_missed,
            "detail": detail,
        }
        kwargs = {}
        if self._base_dir is not None:
            kwargs["base_dir"] = self._base_dir
        path = writer.write_heartbeat(record, **kwargs)
        if event in ("job_start", "tick"):
            self.last_tick_ms = record["ts"]
        return path

    def job_start(self, detail: Optional[str] = None) -> Path:
        return self._write("job_start", detail=detail)

    def tick(self, detail: Optional[str] = None) -> Path:
        return self._write("tick", detail=detail)

    def error(self, detail: str) -> Path:
        return self._write("error", detail=detail)

    def job_end(self, *, rounds_seen: int, rounds_missed: int, detail: Optional[str] = None) -> Path:
        return self._write("job_end", rounds_seen=rounds_seen, rounds_missed=rounds_missed, detail=detail)

    def seconds_since_last_tick(self) -> float:
        if self.last_tick_ms is None:
            return float("inf")
        return (self._now_ms() - self.last_tick_ms) / 1000.0

    def due_for_tick(self) -> bool:
        return self.seconds_since_last_tick() >= TICK_INTERVAL_SEC
