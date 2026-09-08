"""Uc kayit tipi icin gecerli ornek kayitlar. Testler bunlari deepcopy'leyip bozar."""

VALID_ROUND = {
    "schema_version": 1,
    "runner_id": "longjob",
    "job_id": "job-abc123",
    "data_lane": "forward_paper",
    "round_id": "btc-updown-5m-1717000000",
    "condition_id": "0xabc",
    "token_ids": {"up": "111", "down": "222"},
    "open_ts": 1717000000000,
    "close_ts": 1717000300000,
    "observations": [
        {
            "offset_sec": 240,
            "offset_actual_sec": 239.8,
            "venue_ts": 1717000060000,
            "response_ts": 1717000060050,
            "runner_ts": 1717000060010,
            "latency_ms": 40,
            "transport": "ws",
            "book": {
                "up": {
                    "best_bid": 0.51,
                    "best_ask": 0.53,
                    "bid_size": 120.0,
                    "ask_size": 95.0,
                    "spread": 0.02,
                    "bids_top5": [[0.51, 120.0], [0.50, 80.0], [0.49, 50.0]],
                    "asks_top5": [[0.53, 95.0], [0.54, 60.0]],
                    "mid": 0.52,
                },
                "down": {
                    "best_bid": 0.46,
                    "best_ask": 0.48,
                    "bid_size": 110.0,
                    "ask_size": 100.0,
                    "spread": 0.02,
                    "bids_top5": [[0.46, 110.0]],
                    "asks_top5": [[0.48, 100.0], [0.49, 70.0], [0.50, 40.0]],
                    "mid": 0.47,
                },
            },
            "btc_binance": {
                "value": 67123.4,
                "source": "rtds_binance",
                "feed_ts": 1717000060000,
            },
            "btc_oracle": {
                "value": 67120.1,
                "source": "rtds_chainlink",
                "feed_ts": 1717000059800,
            },
            "status": "ok",
            "error": None,
        }
    ],
    "decision": {
        "ts": 1717000180000,
        "action": "skip",
        "side": None,
        "rule_id": "spread_too_wide",
        "rule_inputs": {"spread": 0.02, "max_spread": 0.01},
        "entry_price_assumed": None,
        "entry_price_basis": "best_ask",
        "size_shares": None,
        "capital_at_risk": None,
        "cost_model": {
            "fee": 0.0,
            "spread_cost": 0.02,
            "slippage_assumed": 0.0,
            "formula_id": "cost_v1",
        },
    },
    "status": "partial",
    "raw": [{"endpoint": "gamma", "payload": {"foo": "bar"}}],
}

VALID_OUTCOME = {
    "schema_version": 1,
    "round_id": "btc-updown-5m-1717000000",
    "resolved_ts": 1717000305000,
    "outcome": "up",
    "resolution_source": "chainlink",
    "open_price": 67100.0,
    "close_price": 67210.5,
    "raw": {"source": "data-api", "payload": {}},
}

VALID_HEARTBEAT = {
    "schema_version": 1,
    "runner_id": "cron",
    "job_id": "job-xyz789",
    "event": "tick",
    "ts": 1717000060000,
    "rounds_seen": None,
    "rounds_missed": None,
    "detail": None,
}
