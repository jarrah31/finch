import json
from datetime import date, timedelta
import aiosqlite


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _period_days(start: str, end: str) -> int:
    """Return number of days in a period (end is exclusive)."""
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    return max((e - s).days, 1)


def _days_elapsed(start: str) -> int:
    s = date.fromisoformat(start)
    today = date.today()
    return max((today - s).days, 0)


def _days_remaining(start: str, end: str) -> int:
    e = date.fromisoformat(end)
    today = date.today()
    if e.year >= 9999:
        # Open-ended current period — estimate from average period length
        s = date.fromisoformat(start)
        assumed_end = s + timedelta(days=31)
        return max((assumed_end - today).days, 0)
    return max((e - today).days, 0)


def _normalize_monthly(amount: float, period: str) -> float:
    """Convert a subscription amount to its monthly equivalent (amount is negative)."""
    if period == "weekly":
        return amount * 52 / 12
    if period == "yearly":
        return amount / 12
    return amount  # monthly


# ---------------------------------------------------------------------------
# Legacy functions (kept for existing chart endpoints)
# ---------------------------------------------------------------------------

async def get_period_summary(db: aiosqlite.Connection, period_id: int) -> dict:
    """Get full monthly analysis for a pay period."""
    cursor = await db.execute("SELECT * FROM pay_periods WHERE id = ?", (period_id,))
    period = dict(await cursor.fetchone())

    start = period["start_date"]
    end = period["end_date"]

    # Category breakdown — join to rules to get real is_subscription flag
    cursor = await db.execute(
        """SELECT c.name, COALESCE(p.name, '') as parent_name,
                  COALESCE(cr.is_subscription, 0) as is_subscription,
                  SUM(t.amount) as total, COUNT(t.id) as tx_count
           FROM transactions t
           JOIN categories c ON t.category_id = c.id
           LEFT JOIN categories p ON c.parent_id = p.id
           LEFT JOIN categorization_rules cr ON t.matched_rule_id = cr.id
           WHERE t.date >= ? AND t.date < ?
             AND t.amount < 0
           GROUP BY c.id, COALESCE(cr.is_subscription, 0)
           ORDER BY COALESCE(p.name, c.name), c.name""",
        (start, end),
    )
    categories = []
    for row in await cursor.fetchall():
        d = dict(row)
        if d["parent_name"]:
            d["name"] = d["parent_name"] + " > " + d["name"]
        categories.append(d)

    # Incomings by category
    cursor = await db.execute(
        """SELECT COALESCE(c.name, 'Uncategorized') as name, SUM(t.amount) as total
           FROM transactions t
           LEFT JOIN categories c ON t.category_id = c.id
           WHERE t.date >= ? AND t.date < ?
             AND t.amount > 0
           GROUP BY c.id
           ORDER BY total DESC""",
        (start, end),
    )
    incomings = [dict(row) for row in await cursor.fetchall()]

    total_incoming = sum(row["total"] for row in incomings)

    # Split income into primary (keyword-matched) vs other — mirrors Analysis page
    cursor = await db.execute("SELECT value FROM settings WHERE key = 'income_keywords'")
    kw_row = await cursor.fetchone()
    income_kw = json.loads(kw_row[0]) if kw_row and kw_row[0] else []
    if income_kw:
        kw_match   = "(" + " OR ".join("UPPER(description) LIKE '%'||UPPER(?)||'%'" for _ in income_kw) + ")"
        kw_no_match = "(" + " AND ".join("UPPER(description) NOT LIKE '%'||UPPER(?)||'%'" for _ in income_kw) + ")"
    else:
        kw_match, kw_no_match = "1=1", "1=0"

    cursor = await db.execute(
        f"SELECT COALESCE(SUM(amount), 0) FROM transactions"
        f" WHERE date >= ? AND date < ? AND amount > 0 AND {kw_match}",
        (start, end, *income_kw),
    )
    primary_income = (await cursor.fetchone())[0]

    cursor = await db.execute(
        f"SELECT COALESCE(SUM(amount), 0) FROM transactions"
        f" WHERE date >= ? AND date < ? AND amount > 0 AND {kw_no_match}",
        (start, end, *income_kw),
    )
    other_income = (await cursor.fetchone())[0]

    cursor = await db.execute(
        f"SELECT date, description, amount FROM transactions"
        f" WHERE date >= ? AND date < ? AND amount > 0 AND {kw_match}"
        f" ORDER BY date DESC",
        (start, end, *income_kw),
    )
    income_transactions = [dict(r) for r in await cursor.fetchall()]

    cursor = await db.execute(
        f"SELECT date, description, amount FROM transactions"
        f" WHERE date >= ? AND date < ? AND amount > 0 AND {kw_no_match}"
        f" ORDER BY date DESC",
        (start, end, *income_kw),
    )
    other_income_transactions = [dict(r) for r in await cursor.fetchall()]
    subs_outgoing = sum(row["total"] for row in categories if row["is_subscription"])
    non_sub_outgoing = sum(row["total"] for row in categories if not row["is_subscription"])
    category_outgoing = subs_outgoing + non_sub_outgoing

    cursor = await db.execute(
        """SELECT COALESCE(SUM(amount), 0) as total
           FROM transactions
           WHERE date >= ? AND date < ?
             AND amount < 0
             AND category_id IS NULL""",
        (start, end),
    )
    non_category_outgoing = (await cursor.fetchone())[0]
    total_outgoing = category_outgoing + non_category_outgoing
    surplus = total_incoming + total_outgoing

    cursor = await db.execute(
        """SELECT COALESCE(SUM(t.amount), 0) as total
           FROM transactions t
           JOIN categories c ON t.category_id = c.id
           WHERE t.date >= ? AND t.date < ?
             AND LOWER(c.name) = 'food'""",
        (start, end),
    )
    food_outgoing = (await cursor.fetchone())[0]

    cursor = await db.execute(
        """SELECT COALESCE(SUM(t.amount), 0) as total
           FROM transactions t
           JOIN categories c ON t.category_id = c.id
           WHERE t.date >= ? AND t.date < ?
             AND LOWER(c.name) = 'penny'""",
        (start, end),
    )
    penny_outgoing = (await cursor.fetchone())[0]

    remaining_subs = await _calc_remaining_subs(db, period_id, subs_outgoing)
    remaining_non_sub = await _calc_remaining_non_sub(db, period_id, non_sub_outgoing)

    # Group all spending by top-level category for tooltip summaries
    from collections import defaultdict
    _parent_totals: dict[str, float] = defaultdict(float)
    for cat in categories:
        parent = cat["name"].split(" > ")[0]
        _parent_totals[parent] += cat["total"]
    top_level_categories = sorted(
        [{"name": k, "total": v} for k, v in _parent_totals.items()],
        key=lambda x: x["total"],  # most negative first
    )

    return {
        "period": period,
        "categories": categories,
        "incomings": incomings,
        "total_incoming": total_incoming,
        "primary_income": primary_income,
        "other_income": other_income,
        "income_transactions": income_transactions,
        "other_income_transactions": other_income_transactions,
        "subs_outgoing": subs_outgoing,
        "non_sub_outgoing": non_sub_outgoing,
        "category_outgoing": category_outgoing,
        "non_category_outgoing": non_category_outgoing,
        "total_outgoing": total_outgoing,
        "surplus": surplus,
        "food_outgoing": food_outgoing,
        "penny_outgoing": penny_outgoing,
        "remaining_subs": remaining_subs,
        "remaining_non_sub": remaining_non_sub,
        "top_level_categories": top_level_categories,
    }


async def _calc_remaining_subs(db: aiosqlite.Connection, current_period_id: int, current_subs: float) -> float:
    cursor = await db.execute(
        "SELECT id, start_date, end_date FROM pay_periods WHERE id < ? ORDER BY id DESC LIMIT 1",
        (current_period_id,),
    )
    prev = await cursor.fetchone()
    if not prev:
        return 0.0

    cursor = await db.execute(
        """SELECT COALESCE(SUM(t.amount), 0)
           FROM transactions t
           WHERE t.date >= ? AND t.date < ?
             AND t.amount < 0
             AND t.matched_rule_id IS NOT NULL
             AND EXISTS (
                 SELECT 1 FROM categorization_rules cr
                 WHERE cr.id = t.matched_rule_id AND cr.is_subscription = 1
             )""",
        (prev["start_date"], prev["end_date"]),
    )
    prev_subs = (await cursor.fetchone())[0]
    return prev_subs - current_subs


async def _calc_remaining_non_sub(db: aiosqlite.Connection, current_period_id: int, current_non_sub: float) -> float:
    cursor = await db.execute(
        "SELECT id, start_date, end_date FROM pay_periods WHERE id < ? ORDER BY id DESC LIMIT 1",
        (current_period_id,),
    )
    prev = await cursor.fetchone()
    if not prev:
        return 0.0

    cursor = await db.execute(
        """SELECT COALESCE(SUM(t.amount), 0)
           FROM transactions t
           WHERE t.date >= ? AND t.date < ?
             AND t.amount < 0
             AND (
                 t.matched_rule_id IS NULL
                 OR NOT EXISTS (
                     SELECT 1 FROM categorization_rules cr
                     WHERE cr.id = t.matched_rule_id AND cr.is_subscription = 1
                 )
             )""",
        (prev["start_date"], prev["end_date"]),
    )
    prev_non_sub = (await cursor.fetchone())[0]
    return prev_non_sub - current_non_sub


async def get_multi_period_analysis(db: aiosqlite.Connection, limit: int = 12) -> list[dict]:
    cursor = await db.execute(
        "SELECT * FROM pay_periods WHERE end_date != '9999-12-31' ORDER BY start_date DESC LIMIT ?",
        (limit,),
    )
    periods = [dict(row) for row in await cursor.fetchall()]
    periods.reverse()

    results = []
    for period in periods:
        cursor = await db.execute(
            """SELECT c.name, COALESCE(cr.is_subscription, 0) as is_subscription, SUM(t.amount) as total
               FROM transactions t
               JOIN categories c ON t.category_id = c.id
               LEFT JOIN categorization_rules cr ON t.matched_rule_id = cr.id
               WHERE t.date >= ? AND t.date < ?
                 AND t.amount < 0
               GROUP BY c.id, COALESCE(cr.is_subscription, 0)
               ORDER BY c.name""",
            (period["start_date"], period["end_date"]),
        )
        cats = [dict(row) for row in await cursor.fetchall()]

        cursor = await db.execute(
            """SELECT COALESCE(SUM(amount), 0) FROM transactions
               WHERE date >= ? AND date < ? AND amount > 0""",
            (period["start_date"], period["end_date"]),
        )
        income = (await cursor.fetchone())[0]

        cursor = await db.execute(
            """SELECT COALESCE(SUM(amount), 0) FROM transactions
               WHERE date >= ? AND date < ? AND amount < 0""",
            (period["start_date"], period["end_date"]),
        )
        outgoing = (await cursor.fetchone())[0]

        results.append({
            "period": period,
            "categories": cats,
            "income": income,
            "outgoing": outgoing,
            "surplus": income + outgoing,
        })

    return results


async def get_spending_by_category(db: aiosqlite.Connection, period_id: int | None = None, last_n: int = 6) -> dict:
    if period_id:
        cursor = await db.execute("SELECT * FROM pay_periods WHERE id = ?", (period_id,))
        period = await cursor.fetchone()
        cursor = await db.execute(
            """SELECT c.name, ABS(SUM(t.amount)) as total
               FROM transactions t
               JOIN categories c ON t.category_id = c.id
               WHERE t.date >= ? AND t.date < ?
                 AND t.amount < 0
               GROUP BY c.id
               ORDER BY total DESC""",
            (period["start_date"], period["end_date"]),
        )
        return {"labels": [], "values": [], "rows": [dict(r) for r in await cursor.fetchall()]}

    cursor = await db.execute(
        "SELECT * FROM pay_periods ORDER BY start_date DESC LIMIT ?", (last_n,)
    )
    periods = list(reversed([dict(r) for r in await cursor.fetchall()]))

    all_categories = set()
    period_data = {}
    for p in periods:
        cursor = await db.execute(
            """SELECT c.name, ABS(SUM(t.amount)) as total
               FROM transactions t
               JOIN categories c ON t.category_id = c.id
               WHERE t.date >= ? AND t.date < ? AND t.amount < 0
               GROUP BY c.id""",
            (p["start_date"], p["end_date"]),
        )
        rows = {r["name"]: r["total"] for r in await cursor.fetchall()}
        period_data[p["label"]] = rows
        all_categories.update(rows.keys())

    return {
        "periods": [p["label"] for p in periods],
        "categories": sorted(all_categories),
        "data": period_data,
    }


# ---------------------------------------------------------------------------
# New analysis functions — Overview tab
# ---------------------------------------------------------------------------

async def get_overview(db: aiosqlite.Connection, period_id: int = None, start_date: str = None, end_date: str = None) -> dict:
    is_custom = start_date is not None and end_date is not None
    if is_custom:
        start, end = start_date, end_date
        period = {"id": None, "start_date": start, "end_date": end}
    else:
        cursor = await db.execute("SELECT * FROM pay_periods WHERE id = ?", (period_id,))
        period = dict(await cursor.fetchone())
        start, end = period["start_date"], period["end_date"]

    # Load income keywords so we can split primary vs other income
    cursor = await db.execute("SELECT value FROM settings WHERE key = 'income_keywords'")
    kw_row = await cursor.fetchone()
    income_kw = json.loads(kw_row[0]) if kw_row and kw_row[0] else []
    if income_kw:
        kw_match   = "(" + " OR ".join("UPPER(description) LIKE '%'||UPPER(?)||'%'" for _ in income_kw) + ")"
        kw_no_match = "(" + " AND ".join("UPPER(description) NOT LIKE '%'||UPPER(?)||'%'" for _ in income_kw) + ")"
    else:
        kw_match, kw_no_match = "1=1", "1=0"  # no keywords → all income counts as primary

    # Outgoing
    cursor = await db.execute(
        "SELECT COALESCE(SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END), 0)"
        " FROM transactions WHERE date >= ? AND date < ?",
        (start, end),
    )
    outgoing = (await cursor.fetchone())[0]

    # Primary income (keyword-matched transactions only)
    cursor = await db.execute(
        f"SELECT COALESCE(SUM(amount), 0) FROM transactions"
        f" WHERE date >= ? AND date < ? AND amount > 0 AND {kw_match}",
        (start, end, *income_kw),
    )
    income = (await cursor.fetchone())[0]

    # Other income (positive transactions not matching any income keyword)
    cursor = await db.execute(
        f"SELECT COALESCE(SUM(amount), 0) FROM transactions"
        f" WHERE date >= ? AND date < ? AND amount > 0 AND {kw_no_match}",
        (start, end, *income_kw),
    )
    other_income = (await cursor.fetchone())[0]

    surplus = income + other_income + outgoing  # total income (primary + other) minus spending

    # Subs vs discretionary
    cursor = await db.execute(
        """SELECT COALESCE(SUM(CASE WHEN cr.is_subscription = 1 THEN t.amount ELSE 0 END), 0) as subs,
                  COALESCE(SUM(CASE WHEN COALESCE(cr.is_subscription, 0) = 0 THEN t.amount ELSE 0 END), 0) as disc
           FROM transactions t
           LEFT JOIN categorization_rules cr ON t.matched_rule_id = cr.id
           WHERE t.date >= ? AND t.date < ? AND t.amount < 0""",
        (start, end),
    )
    sd = dict(await cursor.fetchone())
    subs_total, discretionary_total = sd["subs"], sd["disc"]

    # Transaction count
    cursor = await db.execute(
        "SELECT COUNT(*) FROM transactions WHERE date >= ? AND date < ?", (start, end)
    )
    tx_count = (await cursor.fetchone())[0]

    # Previous period for deltas (single-period only)
    prev_income = prev_outgoing = prev_surplus = None
    prev = None
    if not is_custom:
        cursor = await db.execute(
            "SELECT * FROM pay_periods WHERE id < ? ORDER BY id DESC LIMIT 1", (period_id,)
        )
        prev = await cursor.fetchone()
        if prev:
            cursor = await db.execute(
                "SELECT COALESCE(SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END), 0)"
                " FROM transactions WHERE date >= ? AND date < ?",
                (prev["start_date"], prev["end_date"]),
            )
            prev_outgoing = (await cursor.fetchone())[0]
            cursor = await db.execute(
                f"SELECT COALESCE(SUM(amount), 0) FROM transactions"
                f" WHERE date >= ? AND date < ? AND amount > 0 AND {kw_match}",
                (prev["start_date"], prev["end_date"], *income_kw),
            )
            prev_income = (await cursor.fetchone())[0]
            cursor = await db.execute(
                f"SELECT COALESCE(SUM(amount), 0) FROM transactions"
                f" WHERE date >= ? AND date < ? AND amount > 0 AND {kw_no_match}",
                (prev["start_date"], prev["end_date"], *income_kw),
            )
            prev_other_income = (await cursor.fetchone())[0]
            prev_surplus = prev_income + prev_other_income + prev_outgoing

    # Days progress
    elapsed = _days_elapsed(start)
    remaining = _days_remaining(start, end)
    period_length = elapsed + remaining
    progress_pct = round(elapsed / period_length * 100) if period_length else 0

    # Daily allowance (single-period only)
    remaining_balance = income + other_income + outgoing
    if is_custom:
        daily_allowance = 0
    else:
        prev_subs = 0.0
        if prev:
            cursor = await db.execute(
                """SELECT COALESCE(SUM(t.amount), 0) FROM transactions t
                   WHERE t.date >= ? AND t.date < ? AND t.amount < 0
                     AND t.matched_rule_id IS NOT NULL
                     AND EXISTS (SELECT 1 FROM categorization_rules cr WHERE cr.id = t.matched_rule_id AND cr.is_subscription = 1)""",
                (prev["start_date"], prev["end_date"]),
            )
            prev_subs = (await cursor.fetchone())[0]
        daily_allowance = (remaining_balance + (prev_subs - subs_total)) / remaining if remaining > 0 else 0

    # Sorted parent-category list for consistent palette index (matches Trends chart)
    cursor = await db.execute(
        "SELECT name FROM categories WHERE parent_id IS NULL ORDER BY name"
    )
    all_parent_cats = [r[0] for r in await cursor.fetchall()]
    cat_colour_idx = {name: i for i, name in enumerate(all_parent_cats)}

    # Donut chart: top-level category totals (aggregate subcats under parent)
    cursor = await db.execute(
        """SELECT COALESCE(p.name, c.name) as name,
                  COALESCE(p.color, c.color) as color,
                  ABS(SUM(t.amount)) as total,
                  MAX(COALESCE(cr.is_subscription, 0)) as is_subscription
           FROM transactions t
           JOIN categories c ON t.category_id = c.id
           LEFT JOIN categories p ON c.parent_id = p.id
           LEFT JOIN categorization_rules cr ON t.matched_rule_id = cr.id
           WHERE t.date >= ? AND t.date < ? AND t.amount < 0
           GROUP BY COALESCE(p.id, c.id)
           ORDER BY total DESC""",
        (start, end),
    )
    donut_rows = [dict(r) for r in await cursor.fetchall()]
    # Attach alphabetical colour index so front-end palette matches Trends chart
    for row in donut_rows:
        row["color_index"] = cat_colour_idx.get(row["name"], len(all_parent_cats))

    # Uncategorized slice
    cursor = await db.execute(
        "SELECT ABS(COALESCE(SUM(amount), 0)) FROM transactions WHERE date >= ? AND date < ? AND amount < 0 AND category_id IS NULL",
        (start, end),
    )
    uncat = (await cursor.fetchone())[0]
    if uncat > 0:
        donut_rows.append({"name": "Uncategorized", "color": None, "color_index": len(all_parent_cats), "total": uncat})

    # Income transactions split by primary (keyword) vs other (for tooltip)
    cursor = await db.execute(
        f"SELECT date, description, amount FROM transactions"
        f" WHERE date >= ? AND date < ? AND amount > 0 AND {kw_match}"
        f" ORDER BY date DESC",
        (start, end, *income_kw),
    )
    income_txs = [dict(r) for r in await cursor.fetchall()]

    cursor = await db.execute(
        f"SELECT date, description, amount FROM transactions"
        f" WHERE date >= ? AND date < ? AND amount > 0 AND {kw_no_match}"
        f" ORDER BY date DESC",
        (start, end, *income_kw),
    )
    other_income_txs = [dict(r) for r in await cursor.fetchall()]

    # Remaining subs / non-sub vs last period (single-period only)
    if is_custom:
        remaining_subs = 0.0
        remaining_non_sub = 0.0
    else:
        remaining_subs = await _calc_remaining_subs(db, period_id, subs_total)
        remaining_non_sub = await _calc_remaining_non_sub(db, period_id, discretionary_total)

    # Food / Penny highlights for non-sub card
    cursor = await db.execute(
        "SELECT COALESCE(SUM(t.amount), 0) FROM transactions t"
        " JOIN categories c ON t.category_id = c.id"
        " WHERE t.date >= ? AND t.date < ? AND LOWER(c.name) = 'food'",
        (start, end),
    )
    food_outgoing = (await cursor.fetchone())[0]

    cursor = await db.execute(
        "SELECT COALESCE(SUM(t.amount), 0) FROM transactions t"
        " JOIN categories c ON t.category_id = c.id"
        " WHERE t.date >= ? AND t.date < ? AND LOWER(c.name) = 'penny'",
        (start, end),
    )
    penny_outgoing = (await cursor.fetchone())[0]

    # Uncategorized spending (for non-sub tooltip)
    cursor = await db.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM transactions"
        " WHERE date >= ? AND date < ? AND amount < 0 AND category_id IS NULL",
        (start, end),
    )
    non_category_outgoing = (await cursor.fetchone())[0]

    # Upcoming subscriptions (next 7 days)
    upcoming_subscriptions = await get_upcoming_subscriptions(db, lookahead_days=7)

    # Recent transactions (last 15)
    cursor = await db.execute(
        """SELECT t.date, t.description, t.amount,
                  COALESCE(p.name || ' › ' || c.name, c.name) as category_name
           FROM transactions t
           LEFT JOIN categories c ON t.category_id = c.id
           LEFT JOIN categories p ON c.parent_id = p.id
           ORDER BY t.date DESC, t.id DESC LIMIT 15""",
    )
    recent_transactions = [dict(r) for r in await cursor.fetchall()]

    return {
        "period": period,
        "income": income,
        "outgoing": outgoing,
        "surplus": surplus,
        "subs_total": subs_total,
        "discretionary_total": discretionary_total,
        "tx_count": tx_count,
        "prev_income": prev_income,
        "prev_outgoing": prev_outgoing,
        "prev_surplus": prev_surplus,
        "days_elapsed": elapsed,
        "days_remaining": remaining,
        "progress_pct": progress_pct,
        "remaining_balance": remaining_balance,
        "daily_allowance": daily_allowance,
        "donut": donut_rows,
        "income_transactions": income_txs,
        "other_income": other_income,
        "other_income_transactions": other_income_txs,
        "remaining_subs": remaining_subs,
        "remaining_non_sub": remaining_non_sub,
        "food_outgoing": food_outgoing,
        "penny_outgoing": penny_outgoing,
        "non_category_outgoing": non_category_outgoing,
        "upcoming_subscriptions": upcoming_subscriptions,
        "recent_transactions": recent_transactions,
    }


# ---------------------------------------------------------------------------
# Spending breakdown tab
# ---------------------------------------------------------------------------

async def get_breakdown(db: aiosqlite.Connection, period_id: int = None, start_date: str = None, end_date: str = None) -> dict:
    is_custom = start_date is not None and end_date is not None
    if is_custom:
        start, end = start_date, end_date
        period = {"id": None, "start_date": start, "end_date": end}
    else:
        cursor = await db.execute("SELECT * FROM pay_periods WHERE id = ?", (period_id,))
        period = dict(await cursor.fetchone())
        start, end = period["start_date"], period["end_date"]

    # Total outgoing for % calc
    cursor = await db.execute(
        "SELECT ABS(COALESCE(SUM(amount), 0)) FROM transactions WHERE date >= ? AND date < ? AND amount < 0",
        (start, end),
    )
    total_out = (await cursor.fetchone())[0] or 1  # avoid div/0

    # All categories with amounts
    cursor = await db.execute(
        """SELECT c.id, c.name, c.color, c.parent_id,
                  COALESCE(p.name, '') as parent_name,
                  COALESCE(p.color, c.color) as parent_color,
                  COALESCE(p.id, c.id) as top_id,
                  COALESCE(cr.is_subscription, 0) as is_subscription,
                  ABS(SUM(t.amount)) as total, COUNT(t.id) as tx_count
           FROM transactions t
           JOIN categories c ON t.category_id = c.id
           LEFT JOIN categories p ON c.parent_id = p.id
           LEFT JOIN categorization_rules cr ON t.matched_rule_id = cr.id
           WHERE t.date >= ? AND t.date < ? AND t.amount < 0
           GROUP BY c.id
           ORDER BY ABS(SUM(t.amount)) DESC""",
        (start, end),
    )
    rows = [dict(r) for r in await cursor.fetchall()]

    # Build hierarchy: group subcategories under parents
    parents = {}
    for r in rows:
        r["pct"] = round(r["total"] / total_out * 100, 1)
        if r["parent_id"] is None:
            # top-level
            if r["id"] not in parents:
                parents[r["id"]] = {**r, "children": []}
            else:
                parents[r["id"]].update({k: v for k, v in r.items() if k != "children"})
        else:
            # subcategory — add to parent entry (creating a stub if needed)
            pid = r["parent_id"]
            if pid not in parents:
                parents[pid] = {
                    "id": pid, "name": r["parent_name"], "color": r["parent_color"],
                    "parent_id": None, "total": 0, "tx_count": 0, "pct": 0,
                    "is_subscription": 0, "children": []
                }
            parents[pid]["children"].append(r)
            parents[pid]["total"] = parents[pid].get("total", 0) + r["total"]
            parents[pid]["tx_count"] = parents[pid].get("tx_count", 0) + r["tx_count"]

    # Resolve colour names (mirrors JS catColorName: explicit color wins, else index in id-sorted top-level list)
    CAT_PALETTE = ['blue','emerald','violet','rose','amber','teal','orange','sky','lime','pink']
    cursor = await db.execute("SELECT id FROM categories WHERE parent_id IS NULL ORDER BY id")
    top_ids = [r[0] for r in await cursor.fetchall()]

    def color_name(cat_id, explicit_color):
        if explicit_color:
            return explicit_color
        try:
            return CAT_PALETTE[top_ids.index(cat_id) % len(CAT_PALETTE)]
        except ValueError:
            return 'blue'

    # Recalc parent totals / pct (they may have been summed from children)
    hierarchy = []
    for p in parents.values():
        p["pct"] = round(p["total"] / total_out * 100, 1)
        p["color_name"] = color_name(p["id"], p.get("color"))
        p["children"].sort(key=lambda x: x["total"], reverse=True)
        hierarchy.append(p)
    hierarchy.sort(key=lambda x: x["total"], reverse=True)

    # Uncategorized
    cursor = await db.execute(
        "SELECT ABS(COALESCE(SUM(amount), 0)), COUNT(*) FROM transactions WHERE date >= ? AND date < ? AND amount < 0 AND category_id IS NULL",
        (start, end),
    )
    uc = await cursor.fetchone()
    if uc[0] > 0:
        hierarchy.append({
            "id": None, "name": "Uncategorized", "color": None,
            "total": uc[0], "tx_count": uc[1],
            "pct": round(uc[0] / total_out * 100, 1),
            "is_subscription": 0, "children": []
        })

    # Tags breakdown
    cursor = await db.execute(
        """SELECT je.value as tag, ABS(SUM(t.amount)) as total, COUNT(t.id) as tx_count
           FROM transactions t
           JOIN categorization_rules cr ON t.matched_rule_id = cr.id
           CROSS JOIN json_each(cr.tags) je
           WHERE t.date >= ? AND t.date < ? AND t.amount < 0
             AND cr.tags IS NOT NULL AND cr.tags != '[]'
           GROUP BY je.value
           ORDER BY total DESC""",
        (start, end),
    )
    tags = [dict(r) for r in await cursor.fetchall()]

    # Subs vs discretionary totals
    cursor = await db.execute(
        """SELECT COALESCE(SUM(CASE WHEN cr.is_subscription = 1 THEN ABS(t.amount) ELSE 0 END), 0) as subs,
                  COALESCE(SUM(CASE WHEN COALESCE(cr.is_subscription, 0) = 0 THEN ABS(t.amount) ELSE 0 END), 0) as disc
           FROM transactions t
           LEFT JOIN categorization_rules cr ON t.matched_rule_id = cr.id
           WHERE t.date >= ? AND t.date < ? AND t.amount < 0""",
        (start, end),
    )
    sd = dict(await cursor.fetchone())

    return {
        "period": period,
        "hierarchy": hierarchy,
        "tags": tags,
        "subs_total": sd["subs"],
        "discretionary_total": sd["disc"],
        "total_outgoing": total_out,
    }


# ---------------------------------------------------------------------------
# Subscriptions tab
# ---------------------------------------------------------------------------

async def get_subscriptions(db: aiosqlite.Connection, period_id: int = None, start_date: str = None, end_date: str = None) -> dict:
    is_custom = start_date is not None and end_date is not None
    if is_custom:
        start, end = start_date, end_date
        period = {"id": None, "start_date": start, "end_date": end}
        prev_start = prev_end = start  # no prev period for custom ranges
    else:
        cursor = await db.execute("SELECT * FROM pay_periods WHERE id = ?", (period_id,))
        period = dict(await cursor.fetchone())
        start, end = period["start_date"], period["end_date"]

        cursor = await db.execute(
            "SELECT * FROM pay_periods WHERE id < ? ORDER BY id DESC LIMIT 1", (period_id,)
        )
        prev = await cursor.fetchone()
        prev_start = prev["start_date"] if prev else start
        prev_end = prev["end_date"] if prev else start

    # All subscription rules
    cursor = await db.execute(
        """SELECT cr.id, cr.keyword, cr.keywords, cr.subscription_period, cr.tags,
                  c.name as category_name, COALESCE(p.name, '') as parent_name,
                  (SELECT t.amount FROM transactions t
                   WHERE t.matched_rule_id = cr.id AND t.amount < 0
                   ORDER BY t.date DESC LIMIT 1) as latest_amount,
                  (SELECT t.date FROM transactions t
                   WHERE t.matched_rule_id = cr.id AND t.amount < 0
                   ORDER BY t.date DESC LIMIT 1) as latest_date,
                  (SELECT t.date FROM transactions t
                   WHERE t.matched_rule_id = cr.id
                     AND t.date >= ? AND t.date < ?
                   ORDER BY t.date DESC LIMIT 1) as actual_period_date,
                  (SELECT COUNT(*) FROM transactions t
                   WHERE t.matched_rule_id = cr.id
                     AND t.date >= ? AND t.date < ?) as paid_this_period,
                  (SELECT COUNT(*) FROM transactions t
                   WHERE t.matched_rule_id = cr.id
                     AND t.date >= ? AND t.date < ?) as paid_prev_period,
                  COALESCE(p.id, c.id) as root_cat_id,
                  COALESCE(p.color, c.color) as explicit_color
           FROM categorization_rules cr
           JOIN categories c ON cr.category_id = c.id
           LEFT JOIN categories p ON c.parent_id = p.id
           WHERE cr.is_subscription = 1
           ORDER BY cr.subscription_period, COALESCE(p.name, ''), c.name""",
        (start, end, start, end, prev_start, prev_end),
    )

    # Resolve category colour names using same palette logic as get_breakdown.
    CAT_PALETTE = ['blue', 'emerald', 'violet', 'rose', 'amber', 'teal', 'orange', 'sky', 'lime', 'pink']
    cur2 = await db.execute("SELECT id FROM categories WHERE parent_id IS NULL ORDER BY id")
    top_ids = [r[0] for r in await cur2.fetchall()]

    def _sub_color_name(root_id, explicit):
        if explicit:
            return explicit
        try:
            return CAT_PALETTE[top_ids.index(root_id) % len(CAT_PALETTE)]
        except ValueError:
            return 'blue'

    period_start = date.fromisoformat(start)
    period_end   = date.fromisoformat(end)
    subs = []
    monthly_total = 0.0
    for row in await cursor.fetchall():
        d = dict(row)
        d["tags"] = json.loads(d["tags"]) if d["tags"] else []
        d["display_name"] = (d["parent_name"] + " › " + d["category_name"]) if d["parent_name"] else d["category_name"]
        d["color_name"] = _sub_color_name(d["root_cat_id"], d.get("explicit_color"))
        amt = d["latest_amount"] or 0.0
        d["monthly_equiv"] = _normalize_monthly(amt, d["subscription_period"] or "monthly")

        # Inactive detection: no transaction in last 60 days relative to today.
        # 60 days = clearly missed at least one monthly billing cycle.
        today_dt = date.today()
        if d["latest_date"]:
            days_since = (today_dt - date.fromisoformat(d["latest_date"])).days
            d["is_inactive"] = days_since > 60
            d["days_since_last"] = days_since
        else:
            d["is_inactive"] = True
            d["days_since_last"] = None

        # Only count active subscriptions toward the monthly total.
        if not d["is_inactive"]:
            monthly_total += d["monthly_equiv"]

        # Determine the date for this period: actual transaction date if paid,
        # otherwise predict forward from latest historical date.
        if d["actual_period_date"]:
            d["period_date"] = d["actual_period_date"]
            d["period_date_predicted"] = False
        elif d["latest_date"]:
            last = date.fromisoformat(d["latest_date"])
            sub_period = d["subscription_period"] or "monthly"
            pred = _add_sub_period(last, sub_period)
            while pred < period_start:
                pred = _add_sub_period(pred, sub_period)
            # Only include prediction if it falls within the period window
            d["period_date"] = pred.isoformat() if pred < period_end else None
            d["period_date_predicted"] = True
        else:
            d["period_date"] = None
            d["period_date_predicted"] = True

        subs.append(d)

    # Group by period
    groups = {"monthly": [], "yearly": [], "weekly": []}
    for s in subs:
        key = s.get("subscription_period") or "monthly"
        groups.setdefault(key, []).append(s)

    # Upcoming = paid in prev period but not yet this period
    upcoming = [s for s in subs if s["paid_prev_period"] > 0 and s["paid_this_period"] == 0]

    return {
        "period": period,
        "subscriptions": subs,
        "groups": groups,
        "monthly_total": monthly_total,
        "annual_total": monthly_total * 12,
        "active_count": len(subs),
        "upcoming": upcoming,
    }


# ---------------------------------------------------------------------------
# Dashboard: upcoming subscriptions (next N days)
# ---------------------------------------------------------------------------

import calendar as _calendar


def _add_sub_period(dt: date, period: str) -> date:
    """Advance a date by one subscription period."""
    if period == "weekly":
        return dt + timedelta(weeks=1)
    elif period == "yearly":
        try:
            return dt.replace(year=dt.year + 1)
        except ValueError:                          # Feb 29 on non-leap year
            return dt.replace(year=dt.year + 1, day=28)
    else:                                           # monthly (default)
        m = dt.month + 1
        y = dt.year
        if m > 12:
            m, y = 1, y + 1
        day = min(dt.day, _calendar.monthrange(y, m)[1])
        return dt.replace(year=y, month=m, day=day)


async def get_upcoming_subscriptions(
    db: aiosqlite.Connection, lookahead_days: int = 7
) -> list[dict]:
    """Return subscription rules whose next predicted payment falls within
    the next *lookahead_days* days (inclusive of today)."""
    today = date.today()
    horizon = today + timedelta(days=lookahead_days)

    cursor = await db.execute(
        """SELECT cr.id, cr.subscription_period,
                  c.name  AS category_name,
                  COALESCE(p.name, '') AS parent_name,
                  (SELECT t.amount FROM transactions t
                   WHERE t.matched_rule_id = cr.id AND t.amount < 0
                   ORDER BY t.date DESC LIMIT 1) AS latest_amount,
                  (SELECT t.date FROM transactions t
                   WHERE t.matched_rule_id = cr.id AND t.amount < 0
                   ORDER BY t.date DESC LIMIT 1) AS latest_date
           FROM categorization_rules cr
           JOIN categories c ON cr.category_id = c.id
           LEFT JOIN categories p ON c.parent_id = p.id
           WHERE cr.is_subscription = 1"""
    )

    upcoming = []
    for row in await cursor.fetchall():
        d = dict(row)
        if not d["latest_date"]:
            continue

        last = date.fromisoformat(d["latest_date"])
        period = d["subscription_period"] or "monthly"

        # Walk forward one period at a time until we're past today
        next_dt = _add_sub_period(last, period)
        while next_dt < today:
            next_dt = _add_sub_period(next_dt, period)

        if next_dt > horizon:
            continue

        days = (next_dt - today).days
        day_n = next_dt.day
        suffix = (
            "th" if 11 <= day_n <= 13 else
            {1: "st", 2: "nd", 3: "rd"}.get(day_n % 10, "th")
        )
        if days == 0:
            due_label = "Due today"
        elif days == 1:
            due_label = "Due tomorrow"
        else:
            due_label = f"Due {next_dt.strftime('%a')} {day_n}{suffix}"  # e.g. "Due Mon 30th"

        d["predicted_next_date"] = next_dt.isoformat()
        d["days_until"] = days
        d["due_label"] = due_label
        d["display_name"] = (
            d["parent_name"] + " › " + d["category_name"]
            if d["parent_name"]
            else d["category_name"]
        )
        upcoming.append(d)

    upcoming.sort(key=lambda x: (x["days_until"], x["display_name"]))
    return upcoming


# ---------------------------------------------------------------------------
# Trends tab
# ---------------------------------------------------------------------------

async def get_trends(
    db: aiosqlite.Connection,
    num_periods: int = 12,
    period_id: int = None,
    start_date: str = None,
    end_date: str = None,
) -> dict:
    if start_date and end_date:
        # Custom range: fetch ASC directly — no reversal needed
        cursor = await db.execute(
            "SELECT * FROM pay_periods WHERE start_date >= ? AND start_date < ? ORDER BY start_date ASC",
            (start_date, end_date),
        )
        periods = [dict(r) for r in await cursor.fetchall()]
    elif period_id:
        # Anchor to selected period — fetch DESC then reverse to get ASC
        cursor = await db.execute(
            "SELECT * FROM pay_periods WHERE id <= ? ORDER BY start_date DESC LIMIT ?",
            (period_id, num_periods),
        )
        periods = list(reversed([dict(r) for r in await cursor.fetchall()]))
    else:
        cursor = await db.execute(
            "SELECT * FROM pay_periods WHERE end_date != '9999-12-31' ORDER BY start_date DESC LIMIT ?",
            (num_periods,),
        )
        periods = list(reversed([dict(r) for r in await cursor.fetchall()]))

    income_series = []
    outgoing_series = []
    surplus_series = []
    subs_series = []
    disc_series = []
    labels = []
    all_parents: set[str] = set()
    all_children: dict[str, set[str]] = {}  # parent_name -> set of child names
    period_cat_data: list[dict] = []  # one entry per period

    for p in periods:
        s, e = p["start_date"], p["end_date"]
        labels.append(p["label"])

        cursor = await db.execute(
            "SELECT COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) as inc,"
            "       COALESCE(SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END), 0) as out"
            " FROM transactions WHERE date >= ? AND date < ?",
            (s, e),
        )
        r = dict(await cursor.fetchone())
        income_series.append(round(r["inc"], 2))
        outgoing_series.append(round(abs(r["out"]), 2))
        surplus_series.append(round(r["inc"] + r["out"], 2))

        cursor = await db.execute(
            """SELECT COALESCE(SUM(CASE WHEN cr.is_subscription = 1 THEN ABS(t.amount) ELSE 0 END), 0) as subs,
                      COALESCE(SUM(CASE WHEN COALESCE(cr.is_subscription, 0) = 0 THEN ABS(t.amount) ELSE 0 END), 0) as disc
               FROM transactions t
               LEFT JOIN categorization_rules cr ON t.matched_rule_id = cr.id
               WHERE t.date >= ? AND t.date < ? AND t.amount < 0""",
            (s, e),
        )
        sd = dict(await cursor.fetchone())
        subs_series.append(round(sd["subs"], 2))
        disc_series.append(round(sd["disc"], 2))

        # Fetch per-leaf-category spending so we can build parent totals + child breakdowns
        cursor = await db.execute(
            """SELECT COALESCE(p2.name, c.name) as parent_name,
                      c.name as child_name,
                      c.parent_id as c_parent_id,
                      ABS(SUM(t.amount)) as total
               FROM transactions t
               JOIN categories c ON t.category_id = c.id
               LEFT JOIN categories p2 ON c.parent_id = p2.id
               WHERE t.date >= ? AND t.date < ? AND t.amount < 0
               GROUP BY c.id""",
            (s, e),
        )
        period_cats: dict = {}
        for row in await cursor.fetchall():
            pname = row["parent_name"]
            cname = row["child_name"]
            is_sub = row["c_parent_id"] is not None
            amt = round(row["total"], 2)
            if pname not in period_cats:
                period_cats[pname] = {"total": 0.0, "children": {}}
            period_cats[pname]["total"] = round(period_cats[pname]["total"] + amt, 2)
            if is_sub:
                period_cats[pname]["children"][cname] = amt
        period_cat_data.append(period_cats)

    # Collect all parent and child names across all periods
    for pcd in period_cat_data:
        for pname, pdata in pcd.items():
            all_parents.add(pname)
            if pdata["children"]:
                all_children.setdefault(pname, set()).update(pdata["children"].keys())

    # Build nested time-series structure
    categories_out: dict = {}
    for pname in sorted(all_parents):
        child_names = sorted(all_children.get(pname, set()))
        totals: list[float] = []
        children: dict[str, list[float]] = {c: [] for c in child_names}
        for pcd in period_cat_data:
            pdata = pcd.get(pname, {"total": 0.0, "children": {}})
            totals.append(round(pdata["total"], 2))
            for cname in child_names:
                children[cname].append(round(pdata["children"].get(cname, 0.0), 2))
        categories_out[pname] = {"total": totals, "children": children}

    return {
        "labels": labels,
        "income": income_series,
        "outgoing": outgoing_series,
        "surplus": surplus_series,
        "subs": subs_series,
        "discretionary": disc_series,
        "categories": categories_out,
    }


# ---------------------------------------------------------------------------
# Forecast tab
# ---------------------------------------------------------------------------

async def get_forecast(db: aiosqlite.Connection, history_periods: int = 6) -> dict:
    # Get last N completed periods
    cursor = await db.execute(
        "SELECT * FROM pay_periods WHERE end_date != '9999-12-31' ORDER BY start_date DESC LIMIT ?",
        (history_periods,),
    )
    hist_periods = list(reversed([dict(r) for r in await cursor.fetchall()]))

    if not hist_periods:
        return {"months": []}

    # Average income over history
    incomes = []
    disc_totals = []
    for p in hist_periods:
        cursor = await db.execute(
            "SELECT COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) as inc FROM transactions WHERE date >= ? AND date < ?",
            (p["start_date"], p["end_date"]),
        )
        incomes.append((await cursor.fetchone())[0])

        cursor = await db.execute(
            """SELECT ABS(COALESCE(SUM(t.amount), 0)) FROM transactions t
               LEFT JOIN categorization_rules cr ON t.matched_rule_id = cr.id
               WHERE t.date >= ? AND t.date < ? AND t.amount < 0
                 AND COALESCE(cr.is_subscription, 0) = 0""",
            (p["start_date"], p["end_date"]),
        )
        disc_totals.append((await cursor.fetchone())[0])

    avg_income = sum(incomes) / len(incomes) if incomes else 0
    avg_disc = sum(disc_totals) / len(disc_totals) if disc_totals else 0

    # Latest subscription monthly total (from all active sub rules)
    cursor = await db.execute(
        """SELECT cr.subscription_period,
                  (SELECT t.amount FROM transactions t
                   WHERE t.matched_rule_id = cr.id AND t.amount < 0
                   ORDER BY t.date DESC LIMIT 1) as latest_amount
           FROM categorization_rules cr
           WHERE cr.is_subscription = 1"""
    )
    sub_monthly = 0.0
    for row in await cursor.fetchall():
        amt = row["latest_amount"] or 0.0
        sub_monthly += abs(_normalize_monthly(amt, row["subscription_period"] or "monthly"))

    # Generate next 3 months
    last_period = hist_periods[-1]
    last_end = date.fromisoformat(last_period["end_date"])

    months = []
    for i in range(3):
        month_start = last_end + timedelta(days=i * 31) if i == 0 else date(
            last_end.year + ((last_end.month + i - 1) // 12),
            ((last_end.month + i - 1) % 12) + 1,
            1,
        )
        label = month_start.strftime("%b %Y")
        pred_total = sub_monthly + avg_disc
        pred_surplus = avg_income - pred_total
        months.append({
            "label": label,
            "predicted_subs": round(sub_monthly, 2),
            "predicted_discretionary": round(avg_disc, 2),
            "predicted_total": round(pred_total, 2),
            "predicted_income": round(avg_income, 2),
            "predicted_surplus": round(pred_surplus, 2),
        })

    return {"months": months, "avg_income": round(avg_income, 2), "sub_monthly": round(sub_monthly, 2)}


# ---------------------------------------------------------------------------
# Budget Runway tab
# ---------------------------------------------------------------------------

async def get_runway(db: aiosqlite.Connection, period_id: int) -> dict:
    cursor = await db.execute("SELECT * FROM pay_periods WHERE id = ?", (period_id,))
    period = dict(await cursor.fetchone())
    start, end = period["start_date"], period["end_date"]

    # Current period spending
    cursor = await db.execute(
        "SELECT COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) as income,"
        "       COALESCE(SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END), 0) as outgoing"
        " FROM transactions WHERE date >= ? AND date < ?",
        (start, end),
    )
    r = dict(await cursor.fetchone())
    income_so_far = r["income"]
    spent_so_far = r["outgoing"]  # negative

    # Subs paid this period
    cursor = await db.execute(
        """SELECT COALESCE(SUM(t.amount), 0) FROM transactions t
           WHERE t.date >= ? AND t.date < ? AND t.amount < 0
             AND t.matched_rule_id IS NOT NULL
             AND EXISTS (SELECT 1 FROM categorization_rules cr WHERE cr.id = t.matched_rule_id AND cr.is_subscription = 1)""",
        (start, end),
    )
    subs_paid = (await cursor.fetchone())[0]  # negative

    # Discretionary paid this period
    cursor = await db.execute(
        """SELECT COALESCE(SUM(t.amount), 0) FROM transactions t
           LEFT JOIN categorization_rules cr ON t.matched_rule_id = cr.id
           WHERE t.date >= ? AND t.date < ? AND t.amount < 0
             AND COALESCE(cr.is_subscription, 0) = 0""",
        (start, end),
    )
    disc_spent = (await cursor.fetchone())[0]  # negative

    # Expected subs from previous period
    cursor = await db.execute(
        "SELECT * FROM pay_periods WHERE id < ? ORDER BY id DESC LIMIT 1", (period_id,)
    )
    prev = await cursor.fetchone()
    subs_expected = 0.0
    if prev:
        cursor = await db.execute(
            """SELECT COALESCE(SUM(t.amount), 0) FROM transactions t
               WHERE t.date >= ? AND t.date < ? AND t.amount < 0
                 AND t.matched_rule_id IS NOT NULL
                 AND EXISTS (SELECT 1 FROM categorization_rules cr WHERE cr.id = t.matched_rule_id AND cr.is_subscription = 1)""",
            (prev["start_date"], prev["end_date"]),
        )
        subs_expected = (await cursor.fetchone())[0]  # negative

    subs_remaining = subs_expected - subs_paid  # both negative; result is negative remainder

    # Average daily discretionary from last 3 completed periods
    cursor = await db.execute(
        "SELECT * FROM pay_periods WHERE end_date != '9999-12-31' AND id < ? ORDER BY id DESC LIMIT 3",
        (period_id,),
    )
    hist = [dict(r) for r in await cursor.fetchall()]
    avg_daily_disc = 0.0
    if hist:
        daily_rates = []
        for h in hist:
            cursor = await db.execute(
                """SELECT COALESCE(SUM(t.amount), 0) FROM transactions t
                   LEFT JOIN categorization_rules cr ON t.matched_rule_id = cr.id
                   WHERE t.date >= ? AND t.date < ? AND t.amount < 0
                     AND COALESCE(cr.is_subscription, 0) = 0""",
                (h["start_date"], h["end_date"]),
            )
            disc = (await cursor.fetchone())[0]  # negative
            days = _period_days(h["start_date"], h["end_date"])
            daily_rates.append(disc / days)  # negative per day
        avg_daily_disc = sum(daily_rates) / len(daily_rates)

    elapsed = _days_elapsed(start)
    remaining = _days_remaining(start, end)
    period_length = elapsed + remaining
    progress_pct = round(elapsed / period_length * 100) if period_length else 0

    est_remaining_disc = avg_daily_disc * remaining  # negative

    pay_amount = period.get("pay_amount") or income_so_far
    remaining_balance = income_so_far + spent_so_far  # what's in the account
    projected_end = remaining_balance + subs_remaining + est_remaining_disc

    daily_allowance = 0.0
    if remaining > 0:
        spendable = remaining_balance + subs_remaining  # remaining after known subs
        daily_allowance = spendable / remaining

    # Subscription checklist: all sub rules, mark paid/unpaid this period
    cursor = await db.execute(
        """SELECT cr.id, cr.subscription_period, cr.tags,
                  c.name as category_name, COALESCE(p.name, '') as parent_name,
                  (SELECT t.amount FROM transactions t
                   WHERE t.matched_rule_id = cr.id AND t.amount < 0
                   ORDER BY t.date DESC LIMIT 1) as latest_amount,
                  (SELECT COUNT(*) FROM transactions t
                   WHERE t.matched_rule_id = cr.id
                     AND t.date >= ? AND t.date < ?) as paid_this_period,
                  (SELECT COUNT(*) FROM transactions t
                   WHERE t.matched_rule_id = cr.id
                     AND t.date >= ? AND t.date < ?) as paid_prev_period
           FROM categorization_rules cr
           JOIN categories c ON cr.category_id = c.id
           LEFT JOIN categories p ON c.parent_id = p.id
           WHERE cr.is_subscription = 1
           ORDER BY COALESCE(p.name, ''), c.name""",
        (start, end,
         prev["start_date"] if prev else start,
         prev["end_date"] if prev else start),
    )
    checklist = []
    for row in await cursor.fetchall():
        d = dict(row)
        d["display_name"] = (d["parent_name"] + " › " + d["category_name"]) if d["parent_name"] else d["category_name"]
        d["paid"] = d["paid_this_period"] > 0
        d["expected"] = d["paid_prev_period"] > 0 or d["paid_this_period"] > 0
        checklist.append(d)

    return {
        "period": period,
        "income_so_far": income_so_far,
        "spent_so_far": spent_so_far,
        "subs_paid": subs_paid,
        "disc_spent": disc_spent,
        "subs_expected": subs_expected,
        "subs_remaining": subs_remaining,
        "est_remaining_disc": est_remaining_disc,
        "remaining_balance": remaining_balance,
        "projected_end": projected_end,
        "daily_allowance": daily_allowance,
        "avg_daily_disc": avg_daily_disc,
        "elapsed": elapsed,
        "remaining": remaining,
        "progress_pct": progress_pct,
        "checklist": checklist,
    }
