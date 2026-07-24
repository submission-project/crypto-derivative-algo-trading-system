from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram


# ───────────────────────────── Order State ─────────────────────────────

ORDER_TRANSITIONS_TOTAL = Counter(
    "takora_order_transitions_total",
    "Total number of order state transitions.",
    ["from_status", "to_status", "source"],
)

ORDER_TRANSITION_FAILURES_TOTAL = Counter(
    "takora_order_transition_failures_total",
    "Total number of failed order state transitions.",
    ["reason"],
)

ORDER_TRANSITION_LATENCY_SECONDS = Histogram(
    "takora_order_transition_latency_seconds",
    "Latency of order state transitions.",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)


# ───────────────────────────── Redis Projection ─────────────────────────────

REDIS_PROJECTION_UPSERT_TOTAL = Counter(
    "takora_redis_projection_upsert_total",
    "Redis order projection upsert results.",
    ["result"],  # applied | stale_ignored | failed
)

REDIS_PROJECTION_REBUILD_TOTAL = Counter(
    "takora_redis_projection_rebuild_total",
    "Redis order projection rebuild runs.",
    ["result"],  # success | partial_failure | failed
)

REDIS_PROJECTION_REBUILD_ROWS_TOTAL = Counter(
    "takora_redis_projection_rebuild_rows_total",
    "Rows processed during Redis projection rebuild.",
    ["result"],  # rebuilt | skipped | failed
)


# ───────────────────────────── Recovery Worker ─────────────────────────────

RECOVERY_SCANS_TOTAL = Counter(
    "takora_recovery_scans_total",
    "Total number of RecoveryWorker scans.",
)

RECOVERY_TARGETS_TOTAL = Counter(
    "takora_recovery_targets_total",
    "Total number of orders selected by RecoveryWorker.",
    ["status"],
)

RECOVERY_RESULTS_TOTAL = Counter(
    "takora_recovery_results_total",
    "RecoveryWorker results.",
    ["result"],  # recovered | no_result | failed | skipped
)

RECOVERY_PENDING_GAUGE = Gauge(
    "takora_recovery_pending_orders",
    "Current number of orders in recovery index.",
)


# ───────────────────────────── Reconciliation Worker ─────────────────────────────

RECONCILIATION_SCANS_TOTAL = Counter(
    "takora_reconciliation_scans_total",
    "Total number of ReconciliationWorker scans.",
)

RECONCILIATION_MISMATCH_TOTAL = Counter(
    "takora_reconciliation_mismatch_total",
    "Reconciliation mismatches detected.",
    ["kind"],
    # kind:
    # pg_missing_in_redis
    # redis_extra_vs_pg
    # exchange_extra_vs_pg
    # pg_missing_from_exchange_open
)

RECONCILIATION_ACTION_TOTAL = Counter(
    "takora_reconciliation_action_total",
    "Reconciliation repair actions.",
    ["action", "result"],
    # action:
    # refresh_projection
    # delete_stale_projection
    # cancel_external_orphan
    # apply_snapshot
    # all_orders_lookup
    # get_order_lookup
)

RECONCILIATION_SCAN_LATENCY_SECONDS = Histogram(
    "takora_reconciliation_scan_latency_seconds",
    "Latency of one reconciliation scan.",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)


# ───────────────────────────── Outbox Publisher ─────────────────────────────

OUTBOX_CLAIMED_TOTAL = Counter(
    "takora_outbox_claimed_total",
    "Total number of outbox events claimed.",
)

OUTBOX_PUBLISHED_TOTAL = Counter(
    "takora_outbox_published_total",
    "Total number of outbox events successfully published.",
    ["event_type"],
)

OUTBOX_FAILED_TOTAL = Counter(
    "takora_outbox_failed_total",
    "Total number of outbox publish failures.",
    ["event_type"],
)

OUTBOX_UNPUBLISHED_GAUGE = Gauge(
    "takora_outbox_unpublished_events",
    "Current number of unpublished outbox events.",
)

OUTBOX_PUBLISH_BATCH_LATENCY_SECONDS = Histogram(
    "takora_outbox_publish_batch_latency_seconds",
    "Latency of one outbox publish batch.",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)


# ───────────────────────────── Fill / Execution Log ─────────────────────────────

FILL_EVENTS_TOTAL = Counter(
    "takora_fill_events_total",
    "Total number of real fill events observed.",
    ["exchange", "market_type", "symbol"],
)

FILL_DUPLICATE_SKIPPED_TOTAL = Counter(
    "takora_fill_duplicate_skipped_total",
    "Total number of duplicate fill events skipped by dedup guard.",
    ["exchange", "market_type", "symbol"],
)

FILL_QUESTDB_SAVE_TOTAL = Counter(
    "takora_fill_questdb_save_total",
    "QuestDB fill save result.",
    ["result"],  # success | failed
)

FILL_DEDUP_KEY_DELETE_TOTAL = Counter(
    "takora_fill_dedup_key_delete_total",
    "Result of deleting Redis fill dedup key after QuestDB failure.",
    ["result"],  # success | failed
)


# ───────────────────────────── User Data Stream ─────────────────────────────

USER_DATA_STREAM_CONNECTED = Gauge(
    "takora_user_data_stream_connected",
    "Whether User Data Stream listener is connected. 1=connected, 0=disconnected.",
)

USER_DATA_STREAM_EVENTS_TOTAL = Counter(
    "takora_user_data_stream_events_total",
    "Total number of user data stream events.",
    ["event_type"],
)

USER_DATA_STREAM_ORDER_EVENTS_TOTAL = Counter(
    "takora_user_data_stream_order_events_total",
    "Total number of ORDER_TRADE_UPDATE events.",
    ["execution_type", "order_status"],
)


# ───────────────────────────── Binance Adapter / API ─────────────────────────────

BINANCE_API_ERRORS_TOTAL = Counter(
    "takora_binance_api_errors_total",
    "Total number of Binance API errors.",
    ["error_type", "code"],
)

BINANCE_API_REQUESTS_TOTAL = Counter(
    "takora_binance_api_requests_total",
    "Total number of Binance API requests.",
    ["endpoint", "result"],
)

BINANCE_API_LATENCY_SECONDS = Histogram(
    "takora_binance_api_latency_seconds",
    "Latency of Binance API calls.",
    ["endpoint"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)