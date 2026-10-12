"""How often to run ingestion so a dataset meets its freshness SLA."""

from contractline.core.diff import compute_duration_seconds

# (interval in seconds, cron). Ingestion runs at the slowest interval that is
# at most half the SLA, so one late or failed run can still be caught up.
_PRESETS = (
    (15 * 60, "*/15 * * * *"),
    (30 * 60, "*/30 * * * *"),
    (3600, "0 * * * *"),
    (2 * 3600, "0 */2 * * *"),
    (6 * 3600, "0 */6 * * *"),
    (12 * 3600, "0 */12 * * *"),
    (24 * 3600, "0 0 * * *"),
)


def build_ingest_schedule(freshness: str) -> str:
    budget = compute_duration_seconds(freshness) / 2
    chosen = _PRESETS[0][1]
    for seconds, cron in _PRESETS:
        if seconds <= budget:
            chosen = cron
    return chosen
