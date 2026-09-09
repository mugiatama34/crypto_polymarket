"""longjob orkestrasyonu: kesif -> abonelik -> offset ornekleme -> yazim ->
commit -> 6 saatte temiz kapanis.

Bu dosyada karar mantigi, cron runner'i veya uzlastirici YOK (kapsam
kilidi, bkz. CLAUDE.md). Yalnizca longjob.
"""

import asyncio
import logging
import uuid
from pathlib import Path
from typing import Optional

from . import exchange_probe, gamma_client, git_commit, sampler, writer
from .clock import RealClock
from .heartbeat import TICK_INTERVAL_SEC, HeartbeatWriter
from .round_calendar import OFFSETS_SEC, ROUND_SECONDS, next_round_start_epoch_s, offset_target_ts_ms, round_slug
from .state import load_state, save_state

logger = logging.getLogger(__name__)

DEFAULT_JOB_DURATION_SEC = 6 * 60 * 60
DEFAULT_SHUTDOWN_MARGIN_SEC = 2 * 60
DEFAULT_COMMIT_INTERVAL_SEC = 15 * 60


class LongjobRunner:
    def __init__(
        self,
        *,
        http_client,
        rtds_client,
        clob_ws_client,
        runner_id: str = "longjob",
        job_id: Optional[str] = None,
        clock=None,
        state_path: Path = Path("state/longjob.json"),
        raw_base_dir: Path = writer.DEFAULT_RAW_BASE_DIR,
        coverage_base_dir: Path = writer.DEFAULT_COVERAGE_BASE_DIR,
        rejected_base_dir: Optional[Path] = None,
        repo_dir: Path = Path("."),
        git_branch: Optional[str] = None,
        job_duration_sec: int = DEFAULT_JOB_DURATION_SEC,
        shutdown_margin_sec: int = DEFAULT_SHUTDOWN_MARGIN_SEC,
        commit_interval_sec: int = DEFAULT_COMMIT_INTERVAL_SEC,
    ):
        self.runner_id = runner_id
        self.job_id = job_id or f"longjob-{uuid.uuid4().hex[:12]}"
        self.clock = clock or RealClock()
        self.http_client = http_client
        self.rtds_client = rtds_client
        self.clob_ws_client = clob_ws_client
        self.state_path = state_path
        self.raw_base_dir = raw_base_dir
        self.coverage_base_dir = coverage_base_dir
        self.rejected_base_dir = rejected_base_dir
        self.repo_dir = repo_dir
        self.git_branch = git_branch
        self.job_duration_sec = job_duration_sec
        self.shutdown_margin_sec = shutdown_margin_sec
        self.commit_interval_sec = commit_interval_sec

        self.heartbeat = HeartbeatWriter(
            runner_id=runner_id,
            job_id=self.job_id,
            now_ms_fn=self.clock.now_ms,
            base_dir=coverage_base_dir,
        )
        self._chosen_exchange: Optional[str] = None
        self.rounds_seen = 0
        self.rounds_missed = 0
        self.discovery_slug_hits = 0
        self.discovery_listing_hits = 0

    async def _sleep_with_heartbeat(self, target_ms: int) -> None:
        """`target_ms`'e kadar bekler ama en fazla TICK_INTERVAL_SEC'lik
        parcalar halinde -- boylece uzun beklemelerde de tick garantisi
        (SCHEMA.md bolum 6) bozulmaz."""
        while self.clock.now_ms() < target_ms:
            chunk_target = min(target_ms, self.clock.now_ms() + TICK_INTERVAL_SEC * 1000)
            await self.clock.sleep_until_ms(chunk_target)
            self._maybe_tick()

    def _maybe_tick(self) -> None:
        if self.heartbeat.due_for_tick():
            self.heartbeat.tick()
        self._drain_rtds_alerts()

    def _drain_rtds_alerts(self) -> None:
        """RTDS sessizlik uyarilari/zorla-yeniden-baglanmalari
        (bkz. RTDSClient._watchdog_loop, docs/decisions.md K-23)
        kendi basina heartbeat'e erisemiyor -- runner her tick'te
        kuyruktan cekip yazar (K-06: bosluk da veridir, sessiz gecilmez)."""
        drain = getattr(self.rtds_client, "drain_alerts", None)
        if drain is None:
            return
        for alert in drain():
            self.heartbeat.error(alert)

    async def _on_ws_disconnect(self, name: str, duration_ms: int, error: Optional[str]) -> None:
        """RTDS/CLOB WS baglantisi koptu ve yeniden kuruldu -- baglanti
        hatasinda da, RTDS'in kendi sessizlik yeniden-baglanmasinda da
        (error alaninda `rtds_silence:<topic>` gorunur) heartbeat'e
        yazilir; boslukta sessiz gecilmez (K-06)."""
        detail = f"{name} yeniden baglandi: {duration_ms}ms bosluk"
        if error:
            detail += f" (sebep: {error})"
        logger.warning(detail)
        self.heartbeat.error(detail)

    def _maybe_commit(self, last_commit_ms: int, *, force: bool = False) -> int:
        now_ms = self.clock.now_ms()
        if not force and (now_ms - last_commit_ms) < self.commit_interval_sec * 1000:
            return last_commit_ms
        try:
            committed = git_commit.commit_paths(
                [self.raw_base_dir, self.coverage_base_dir, self.state_path.parent],
                message=f"longjob {self.job_id}: veri guncelleme",
                repo_dir=self.repo_dir,
            )
            if committed:
                git_commit.push_with_retry(repo_dir=self.repo_dir, branch=self.git_branch)
        except git_commit.GitCommitError as exc:
            self.heartbeat.error(f"git commit/push basarisiz: {exc}")
        return now_ms

    async def _run_round(self, market) -> dict:
        observations = []
        raw = [{"endpoint": f"gamma_event_{market.discovery_method}", "payload": market.raw}]

        for offset_sec in OFFSETS_SEC:
            target_ms = offset_target_ts_ms(market.close_ts_ms, offset_sec)
            await self._sleep_with_heartbeat(target_ms)

            ws_obs, ws_raw = await sampler.build_ws_observation(
                offset_sec=offset_sec,
                close_ts_ms=market.close_ts_ms,
                token_ids=market.token_ids,
                rtds_client=self.rtds_client,
                clob_ws_client=self.clob_ws_client,
                now_ms_fn=self.clock.now_ms,
            )
            rest_obs, rest_raw = await sampler.build_rest_observation(
                offset_sec=offset_sec,
                close_ts_ms=market.close_ts_ms,
                token_ids=market.token_ids,
                http_client=self.http_client,
                exchange=self._chosen_exchange,
                now_ms_fn=self.clock.now_ms,
            )
            observations.append(ws_obs)
            observations.append(rest_obs)
            raw.extend(ws_raw)
            raw.extend(rest_raw)
            self._maybe_tick()

        statuses = {o["status"] for o in observations}
        if statuses == {"ok"}:
            status = "complete"
        elif statuses == {"missed"}:
            status = "missed"
        else:
            status = "partial"

        return {
            "schema_version": 1,
            "runner_id": self.runner_id,
            "job_id": self.job_id,
            "data_lane": "forward_paper",
            "round_id": market.round_id,
            "condition_id": market.condition_id,
            "token_ids": market.token_ids,
            "open_ts": market.open_ts_ms,
            "close_ts": market.close_ts_ms,
            "observations": observations,
            "decision": None,
            "status": status,
            "raw": raw,
        }

    async def _process_one_round(self, round_start_s: int) -> None:
        market = None
        try:
            market = await gamma_client.discover_round_market(self.http_client, round_start_s)
        except Exception as exc:  # noqa: BLE001 -- ag/parse hatasi round'u dusurmez, missed sayilir
            self.heartbeat.error(f"gamma fetch basarisiz round={round_slug(round_start_s)}: {exc}")

        if market is None:
            self.rounds_missed += 1
            self.heartbeat.error(f"market bulunamadi, round atlandi: {round_slug(round_start_s)}")
            return

        if market.discovery_method == "listing":
            self.discovery_listing_hits += 1
        else:
            self.discovery_slug_hits += 1

        await self.clob_ws_client.subscribe([market.token_ids["up"], market.token_ids["down"]])
        round_record = await self._run_round(market)
        writer.write_round(round_record, base_dir=self.raw_base_dir, rejected_base_dir=self.rejected_base_dir)
        self.rounds_seen += 1

    async def run(self) -> None:
        set_rtds_on_disconnect = getattr(self.rtds_client, "set_on_disconnect", None)
        if set_rtds_on_disconnect is not None:
            set_rtds_on_disconnect(lambda duration_ms, error: self._on_ws_disconnect("RTDS", duration_ms, error))
        set_clob_ws_on_disconnect = getattr(self.clob_ws_client, "set_on_disconnect", None)
        if set_clob_ws_on_disconnect is not None:
            set_clob_ws_on_disconnect(
                lambda duration_ms, error: self._on_ws_disconnect("CLOB WS", duration_ms, error)
            )

        probe_result = await exchange_probe.probe_exchanges(self.http_client)
        self._chosen_exchange = probe_result.exchange
        blocked = [a.exchange for a in probe_result.attempts if not a.ok]
        detail = f"exchange={self._chosen_exchange}"
        if blocked:
            detail += f" blocked={','.join(blocked)}"
        self.heartbeat.job_start(detail=detail)

        rtds_task = asyncio.create_task(self.rtds_client.run())
        clob_ws_task = asyncio.create_task(self.clob_ws_client.run())

        state = load_state(self.state_path)
        if state.last_processed_round_epoch_s is not None:
            round_start_s = next_round_start_epoch_s(state.last_processed_round_epoch_s)
        else:
            round_start_s = next_round_start_epoch_s(self.clock.now_ms() / 1000)

        job_start_ms = self.clock.now_ms()
        deadline_ms = job_start_ms + self.job_duration_sec * 1000
        last_commit_ms = job_start_ms

        while (deadline_ms - self.clock.now_ms()) >= self.shutdown_margin_sec * 1000:
            await self._process_one_round(round_start_s)

            state.last_processed_round_epoch_s = round_start_s
            state.updated_at_ms = self.clock.now_ms()
            save_state(state, self.state_path)

            last_commit_ms = self._maybe_commit(last_commit_ms)

            round_start_s += ROUND_SECONDS
            await self._sleep_with_heartbeat(round_start_s * 1000)

        self.rtds_client.stop()
        self.clob_ws_client.stop()
        await asyncio.gather(rtds_task, clob_ws_task, return_exceptions=True)

        self._maybe_commit(last_commit_ms, force=True)
        self.heartbeat.job_end(
            rounds_seen=self.rounds_seen,
            rounds_missed=self.rounds_missed,
            discovery_slug_hits=self.discovery_slug_hits,
            discovery_listing_hits=self.discovery_listing_hits,
            # K-25/K-29: rtds_client test double'larinda bu sayaclar
            # olmayabilir (bkz. _drain_rtds_alerts'teki ayni getattr
            # deseni) -- boyle durumda 0 yazilir.
            rtds_dropped_not_json=getattr(self.rtds_client, "dropped_not_json", 0),
            rtds_dropped_unknown_symbol=getattr(self.rtds_client, "dropped_unknown_symbol", 0),
            rtds_dropped_unknown_shape=getattr(self.rtds_client, "dropped_unknown_shape", 0),
            detail="6 saat siniri yaklasti, temiz kapanis",
        )
