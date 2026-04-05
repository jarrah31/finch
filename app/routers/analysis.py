from fastapi import APIRouter, HTTPException, Body, Query
from typing import Optional
from ..database import get_db
from ..services.analysis import (
    get_period_summary, get_multi_period_analysis, get_spending_by_category,
    get_overview, get_breakdown, get_subscriptions, get_trends, get_forecast, get_runway,
)
from ..services.anomalies import get_anomalies

router = APIRouter()


@router.get("/api/pay-periods")
async def list_pay_periods():
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM pay_periods ORDER BY start_date DESC"
        )
        return [dict(row) for row in await cursor.fetchall()]
    finally:
        await db.close()


@router.get("/api/pay-periods/{period_id}/summary")
async def period_summary(period_id: int):
    db = await get_db()
    try:
        return await get_period_summary(db, period_id)
    finally:
        await db.close()


@router.get("/api/charts/spending-trend")
async def spending_trend(last_n: int = 12):
    db = await get_db()
    try:
        return await get_multi_period_analysis(db, last_n)
    finally:
        await db.close()


@router.get("/api/charts/spending-by-category")
async def spending_by_category(period_id: Optional[int] = None, last_n: int = 6):
    db = await get_db()
    try:
        return await get_spending_by_category(db, period_id, last_n)
    finally:
        await db.close()


@router.get("/api/analysis/overview")
async def analysis_overview(
    period_id: Optional[int] = None,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    db = await get_db()
    try:
        return await get_overview(db, period_id=period_id, start_date=start_date, end_date=end_date)
    finally:
        await db.close()


@router.get("/api/analysis/breakdown")
async def analysis_breakdown(
    period_id: Optional[int] = None,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    db = await get_db()
    try:
        return await get_breakdown(db, period_id=period_id, start_date=start_date, end_date=end_date)
    finally:
        await db.close()


@router.get("/api/analysis/subscriptions")
async def analysis_subscriptions(
    period_id: Optional[int] = None,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    db = await get_db()
    try:
        return await get_subscriptions(db, period_id=period_id, start_date=start_date, end_date=end_date)
    finally:
        await db.close()


@router.get("/api/analysis/trends")
async def analysis_trends(
    periods: int = 12,
    period_id: Optional[int] = None,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    db = await get_db()
    try:
        return await get_trends(db, num_periods=periods, period_id=period_id, start_date=start_date, end_date=end_date)
    finally:
        await db.close()


@router.get("/api/analysis/forecast")
async def analysis_forecast():
    db = await get_db()
    try:
        return await get_forecast(db)
    finally:
        await db.close()


@router.get("/api/analysis/runway")
async def analysis_runway(period_id: int):
    db = await get_db()
    try:
        return await get_runway(db, period_id)
    finally:
        await db.close()


@router.get("/api/descriptions")
async def unique_descriptions(search: Optional[str] = None, positive: bool = False, amounts: Optional[str] = None, keywords: Optional[str] = None, hide_categorised: bool = False, exclude_amounts: Optional[str] = None, exclude_keywords: Optional[str] = None):
    """Get unique descriptions for the categorization UI."""
    import json as _json
    db = await get_db()
    try:
        conditions = []
        params = []

        if positive:
            conditions.append("amount > 0")

        if hide_categorised:
            conditions.append("category_id IS NULL")

        # Multi-keyword OR search takes priority over single search
        kw_list = []
        if keywords:
            try:
                kw_list = _json.loads(keywords)
            except Exception:
                pass
        if kw_list:
            and_parts = " AND ".join(["UPPER(description) LIKE '%' || UPPER(?) || '%'" for _ in kw_list])
            conditions.append(f"({and_parts})")
            params.extend(kw_list)
        elif search:
            conditions.append("UPPER(description) LIKE '%' || UPPER(?) || '%'")
            params.append(search)

        # Amount filter: keep only transactions where amount matches any of the provided values
        amount_list = []
        if amounts:
            try:
                amount_list = _json.loads(amounts)
            except Exception:
                pass
        if amount_list:
            conditions.append(
                "EXISTS (SELECT 1 FROM json_each(?) je WHERE ABS(amount - je.value) < 0.005)"
            )
            params.append(_json.dumps(amount_list))

        # Exclude amounts: filter OUT transactions matching any excluded amount
        exc_amount_list = []
        if exclude_amounts:
            try:
                exc_amount_list = _json.loads(exclude_amounts)
            except Exception:
                pass
        if exc_amount_list:
            conditions.append(
                "NOT EXISTS (SELECT 1 FROM json_each(?) je WHERE ABS(amount - je.value) < 0.005)"
            )
            params.append(_json.dumps(exc_amount_list))

        # Exclude keywords: filter OUT transactions containing any excluded keyword
        exc_kw_list = []
        if exclude_keywords:
            try:
                exc_kw_list = _json.loads(exclude_keywords)
            except Exception:
                pass
        for ekw in exc_kw_list:
            conditions.append("UPPER(description) NOT LIKE '%' || UPPER(?) || '%'")
            params.append(ekw)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        # Accurate total transaction count (ignores LIMIT)
        count_cursor = await db.execute(
            f"SELECT COUNT(*) as n FROM transactions {where}", params
        )
        total_tx_count = (await count_cursor.fetchone())["n"]

        # Group by description + amount so we can show every distinct amount variant.
        # Limit to 500 desc+amount combos then aggregate into up to 100 descriptions.
        cursor = await db.execute(
            f"""SELECT description, amount,
                       COUNT(*) as tx_count,
                       MIN(date) as min_date, MAX(date) as max_date
               FROM transactions t
               {where}
               GROUP BY description, amount
               ORDER BY MAX(date) DESC, description, COUNT(*) DESC
               LIMIT 500""",
            params,
        )
        rows = [dict(row) for row in await cursor.fetchall()]

        # Aggregate into per-description buckets preserving insertion order
        desc_map: dict = {}
        for row in rows:
            d = row["description"]
            if d not in desc_map:
                desc_map[d] = {
                    "description": d,
                    "tx_count": 0,
                    "min_date": row["min_date"],
                    "max_date": row["max_date"],
                    "amounts": [],
                }
            entry = desc_map[d]
            entry["tx_count"] += row["tx_count"]
            entry["min_date"] = min(entry["min_date"], row["min_date"])
            entry["max_date"] = max(entry["max_date"], row["max_date"])
            entry["amounts"].append({"amount": row["amount"], "count": row["tx_count"]})

        results = list(desc_map.values())[:100]
        return {"results": results, "total_tx_count": total_tx_count}
    finally:
        await db.close()


@router.get("/api/analysis/anomalies")
async def list_anomalies():
    db = await get_db()
    try:
        return await get_anomalies(db)
    finally:
        await db.close()


@router.post("/api/analysis/anomalies/{transaction_id}/dismiss")
async def dismiss_anomaly(transaction_id: int):
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR IGNORE INTO anomaly_dismissals (transaction_id) VALUES (?)",
            (transaction_id,),
        )
        await db.commit()
        return {"ok": True}
    finally:
        await db.close()


@router.post("/api/analysis/anomalies/{transaction_id}/fix")
async def fix_anomaly(transaction_id: int, body: dict = Body(...)):
    """Move a transaction's date to the start of its expected pay period."""
    expected_period_id = body.get("expected_period_id")
    if not expected_period_id:
        return {"ok": False, "error": "expected_period_id required"}
    db = await get_db()
    try:
        # Get the expected period start date
        cursor = await db.execute(
            "SELECT start_date FROM pay_periods WHERE id = ?",
            (expected_period_id,),
        )
        row = await cursor.fetchone()
        if not row:
            return {"ok": False, "error": "Period not found"}
        new_date = row[0]

        # Move the transaction into the expected period
        await db.execute(
            "UPDATE transactions SET date = ? WHERE id = ?",
            (new_date, transaction_id),
        )
        # Also dismiss so it won't reappear after date change
        await db.execute(
            "INSERT OR IGNORE INTO anomaly_dismissals (transaction_id) VALUES (?)",
            (transaction_id,),
        )
        await db.commit()
        return {"ok": True, "new_date": new_date}
    finally:
        await db.close()
