"""
Income anomaly detection.

Identifies income payments that arrived significantly earlier than their
usual day relative to the payday, causing them to fall into the wrong
pay period (e.g. BT paying on 22 Dec instead of the usual ~28 Dec,
landing it in November's period rather than December's).

Detection strategy:
  1. Group all non-payday income by amount cluster (±10% tolerance).
  2. For each cluster with ≥ 4 occurrences, calculate the typical
     days_into_period (how many days after the payday the income arrives).
  3. Flag transactions where days_into_period is a strong outlier
     (above Q3 + 3×IQR and > 14 days) — these arrived much later than
     usual, meaning they landed in the period BEFORE their expected one.
  4. Confirm by checking whether the NEXT period is missing that income.
"""

import aiosqlite


async def get_anomalies(db: aiosqlite.Connection) -> list[dict]:
    """Return detected (and not dismissed) income anomalies."""
    cursor = await db.execute("SELECT value FROM settings WHERE key = 'pay_day_keyword'")
    row = await cursor.fetchone()
    pay_kw = row[0] if row else ""

    # All non-payday income with period context
    cursor = await db.execute(
        """SELECT t.id, t.date, t.description, t.amount,
                  pp.id        AS period_id,
                  pp.label     AS period_label,
                  pp.start_date,
                  pp.end_date,
                  CAST(julianday(t.date) - julianday(pp.start_date) AS INTEGER)
                               AS days_into_period
           FROM transactions t
           JOIN pay_periods pp
             ON t.date >= pp.start_date AND t.date < pp.end_date
           WHERE t.amount > 0
             AND UPPER(t.description) NOT LIKE '%' || UPPER(?) || '%'
           ORDER BY t.amount, t.date""",
        (pay_kw,),
    )
    all_income = [dict(r) for r in await cursor.fetchall()]
    if len(all_income) < 4:
        return []

    # All periods ordered by start_date for next-period lookup
    cursor = await db.execute("SELECT * FROM pay_periods ORDER BY start_date")
    all_periods = [dict(r) for r in await cursor.fetchall()]
    period_by_id = {p["id"]: p for p in all_periods}
    period_by_start = {p["start_date"]: p for p in all_periods}

    # Dismissed transaction IDs
    cursor = await db.execute("SELECT transaction_id FROM anomaly_dismissals")
    dismissed = {r[0] for r in await cursor.fetchall()}

    anomalies = []
    for cluster in _cluster_by_amount(all_income, tolerance=0.10):
        if len(cluster) < 4:
            continue

        # Sort cluster by date for chronological context
        cluster_by_date = sorted(cluster, key=lambda x: x["date"])
        days_sorted = sorted(t["days_into_period"] for t in cluster_by_date)
        n = len(days_sorted)

        q1 = days_sorted[n // 4]
        q3 = days_sorted[3 * n // 4]
        iqr = q3 - q1
        # Strong outlier fence: must be both statistically extreme AND > 14 days
        fence = max(q3 + 3 * max(iqr, 3), 14)

        # Most-recent description (name can change year to year)
        most_recent_desc = cluster_by_date[-1]["description"]
        unique_descs = list(dict.fromkeys(t["description"] for t in cluster_by_date))

        for tx in cluster:
            if tx["days_into_period"] <= fence:
                continue
            if tx["id"] in dismissed:
                continue

            period = period_by_id.get(tx["period_id"])
            if not period:
                continue

            # The next period starts where this one ends
            next_period = period_by_start.get(period["end_date"])
            if not next_period:
                continue

            # Confirm: is this income MISSING from the next period?
            next_start = next_period["start_date"]
            next_end = next_period["end_date"]
            in_next = any(
                next_start <= t["date"] < next_end
                and abs(t["amount"] - tx["amount"]) / max(tx["amount"], 0.01) < 0.15
                for t in cluster_by_date
            )

            anomalies.append({
                "transaction_id": tx["id"],
                "date": tx["date"],
                "description": most_recent_desc,
                "all_descriptions": unique_descs[:5],
                "amount": tx["amount"],
                "actual_period_id": tx["period_id"],
                "actual_period_label": period["label"],
                "expected_period_id": next_period["id"],
                "expected_period_label": next_period["label"],
                "days_into_period": tx["days_into_period"],
                "typical_days_min": q1,
                "typical_days_max": q3,
                "missing_from_expected": not in_next,
                "dismissed": False,
            })

    return anomalies


def _cluster_by_amount(transactions: list[dict], tolerance: float = 0.10) -> list[list[dict]]:
    """Group transactions where every member's amount is within ±tolerance of
    the cluster's running median."""
    if not transactions:
        return []
    sorted_txs = sorted(transactions, key=lambda x: x["amount"])
    clusters: list[list[dict]] = []
    current = [sorted_txs[0]]

    for tx in sorted_txs[1:]:
        mid = current[len(current) // 2]["amount"]
        if tx["amount"] <= mid * (1 + tolerance * 2):
            current.append(tx)
        else:
            clusters.append(current)
            current = [tx]
    clusters.append(current)
    return clusters
