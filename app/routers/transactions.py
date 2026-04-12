import json
from fastapi import APIRouter, Request, UploadFile, File, Query
from fastapi.responses import HTMLResponse
from typing import Optional

from ..database import get_db
from ..models import TransactionCategoryUpdate, MerchantOverrideUpdate
from ..services.csv_import import import_csv
from ..services.categorizer import categorize_uncategorized, recategorize_all
from ..services.pay_periods import recompute_pay_periods
from ..data.merchant_dictionary import lookup_merchant, normalize_description
from ..services.logos import fetch_and_cache_logo
import httpx

router = APIRouter()


@router.post("/api/transactions/import")
async def import_transactions(request: Request, file: UploadFile = File(...)):
    content = (await file.read()).decode("utf-8-sig")
    db = await get_db()
    try:
        # Get date format and column mapping from settings
        cursor = await db.execute(
            "SELECT key, value FROM settings WHERE key IN ('csv_date_format', 'csv_column_mapping')"
        )
        settings_rows = {r[0]: r[1] for r in await cursor.fetchall()}
        date_format = settings_rows.get("csv_date_format") or "%d %b %Y"
        mapping_json = settings_rows.get("csv_column_mapping")
        column_mapping = json.loads(mapping_json) if mapping_json else None

        result = await import_csv(db, content, date_format, column_mapping)

        # Auto-categorize new transactions
        categorized = await categorize_uncategorized(db)
        result["categorized"] = categorized

        # Recompute pay periods
        await recompute_pay_periods(db)

        return result
    finally:
        await db.close()


SORT_COLUMNS = {
    "date": "t.date",
    "description": "LOWER(t.description)",
    "amount": "t.amount",
    "type": "LOWER(COALESCE(t.type, ''))",
    "category": "LOWER(COALESCE(c.name, ''))",
}

@router.get("/api/transactions/types")
async def list_transaction_types():
    """Return the distinct non-empty type values present in the transactions table."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT DISTINCT type FROM transactions WHERE type IS NOT NULL AND type != '' ORDER BY type"
        )
        return [row[0] for row in await cursor.fetchall()]
    finally:
        await db.close()


@router.get("/api/transactions")
async def list_transactions(
    account_id: Optional[int] = None,
    category_ids: Optional[str] = None,  # comma-separated; 'uncategorized' is a special value
    tx_types: Optional[str] = None,       # comma-separated type strings
    pay_period_id: Optional[int] = None,
    search: Optional[str] = None,
    search_in: str = "both",
    page: int = 1,
    per_page: int = 50,
    sort_by: str = "date",
    sort_dir: str = "desc",
):
    db = await get_db()
    try:
        conditions = []
        params = []

        if account_id:
            conditions.append("t.account_id = ?")
            params.append(account_id)
        if category_ids:
            parts = [x.strip() for x in category_ids.split(",") if x.strip()]
            include_null = "uncategorized" in parts
            int_ids = [int(x) for x in parts if x != "uncategorized"]
            cat_clauses = []
            if include_null:
                cat_clauses.append("t.category_id IS NULL")
            if int_ids:
                placeholders = ",".join("?" * len(int_ids))
                cat_clauses.append(f"t.category_id IN ({placeholders})")
                params.extend(int_ids)
            if cat_clauses:
                conditions.append("(" + " OR ".join(cat_clauses) + ")")
        if tx_types:
            parts = [x.strip() for x in tx_types.split(",") if x.strip()]
            if parts:
                conditions.append(
                    "UPPER(t.type) IN (" + ",".join(["UPPER(?)"] * len(parts)) + ")"
                )
                params.extend(parts)
        if search:
            words = search.split()
            if search_in == "description":
                for word in words:
                    conditions.append("UPPER(t.description) LIKE '%' || UPPER(?) || '%'")
                    params.append(word)
            elif search_in == "category":
                # Word-boundary match: pad combined name with spaces so "car" won't hit "card"
                cat_combined = "UPPER(' ' || COALESCE(p.name, '') || ' ' || COALESCE(c.name, '') || ' ')"
                for word in words:
                    conditions.append(f"{cat_combined} LIKE '%' || ' ' || UPPER(?) || ' ' || '%'")
                    params.append(word)
            else:  # both
                for word in words:
                    conditions.append(f"(UPPER(t.description) LIKE '%' || UPPER(?) || '%' OR UPPER(' ' || COALESCE(p.name, '') || ' ' || COALESCE(c.name, '') || ' ') LIKE '%' || ' ' || UPPER(?) || ' ' || '%')")
                    params.extend([word, word])
        if pay_period_id:
            conditions.append(
                "t.date >= (SELECT start_date FROM pay_periods WHERE id = ?) "
                "AND t.date < (SELECT end_date FROM pay_periods WHERE id = ?)"
            )
            params.extend([pay_period_id, pay_period_id])

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Count (needs same joins as the fetch for category search to work)
        cursor = await db.execute(
            f"""SELECT COUNT(*) FROM transactions t
                LEFT JOIN categories c ON t.category_id = c.id
                LEFT JOIN categories p ON c.parent_id = p.id
                {where}""",
            params,
        )
        total = (await cursor.fetchone())[0]

        # Fetch page
        offset = (page - 1) * per_page
        cursor = await db.execute(
            f"""SELECT t.*, c.name as category_name, p.name as parent_category_name, a.account_name
                FROM transactions t
                LEFT JOIN categories c ON t.category_id = c.id
                LEFT JOIN categories p ON c.parent_id = p.id
                LEFT JOIN accounts a ON t.account_id = a.id
                {where}
                ORDER BY {SORT_COLUMNS.get(sort_by, 't.date')} {'DESC' if sort_dir == 'desc' else 'ASC'}, t.id {'DESC' if sort_dir == 'desc' else 'ASC'}
                LIMIT ? OFFSET ?""",
            params + [per_page, offset],
        )
        transactions = [dict(row) for row in await cursor.fetchall()]

        # Attach merchant metadata for the logo editor.
        # merchant_domain is stored on the row — no runtime lookup needed.
        # merchant_overridden lets the frontend know whether a manual rule exists.
        description_keys = [normalize_description(tx["description"]) for tx in transactions]
        if description_keys:
            placeholders = ",".join("?" * len(description_keys))
            cursor = await db.execute(
                f"SELECT description_key FROM merchant_overrides WHERE description_key IN ({placeholders})",
                description_keys,
            )
            overridden_keys = {row[0] for row in await cursor.fetchall()}
        else:
            overridden_keys = set()

        for tx in transactions:
            key = normalize_description(tx["description"])
            tx["merchant_description_key"] = key
            tx["merchant_overridden"] = key in overridden_keys
            # merchant_name: look up from dictionary for modal pre-fill (cheap, in-memory)
            if not tx["merchant_overridden"]:
                m = lookup_merchant(tx["description"])
                tx["merchant_name"] = m["name"] if m else None
            else:
                tx["merchant_name"] = None

        return {
            "transactions": transactions,
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page,
        }
    finally:
        await db.close()


@router.put("/api/merchant-overrides")
async def save_merchant_override(data: MerchantOverrideUpdate):
    """
    Save (or clear) a merchant logo domain override.

    Two things happen:
      1. merchant_overrides table is updated so future imports auto-apply the rule.
      2. merchant_domain is written directly to every matching transaction row so
         the assignment survives a DB restore without needing to re-run matching.
    """
    db = await get_db()
    try:
        # 1. Update the overrides table (rulebook for future imports)
        if data.domain:
            await db.execute(
                "INSERT OR REPLACE INTO merchant_overrides (description_key, domain) VALUES (?, ?)",
                (data.description_key, data.domain),
            )
        else:
            await db.execute(
                "DELETE FROM merchant_overrides WHERE description_key = ?",
                (data.description_key,),
            )

        # 2. Apply to all existing matching transactions.
        #    We fetch descriptions and filter in Python because SQLite doesn't know
        #    about normalize_description() — this is fast enough since it runs rarely.
        cursor = await db.execute("SELECT id, description FROM transactions")
        rows = await cursor.fetchall()
        matching_ids = [
            r[0] for r in rows
            if normalize_description(r[1]) == data.description_key
        ]
        if matching_ids:
            placeholders = ",".join("?" * len(matching_ids))
            if data.domain:
                await db.execute(
                    f"UPDATE transactions SET merchant_domain = ? WHERE id IN ({placeholders})",
                    [data.domain] + matching_ids,
                )
            else:
                # Clearing: fall back to dictionary for each matched transaction
                for tx_id in matching_ids:
                    # Find description for this id (already in rows)
                    desc = next(r[1] for r in rows if r[0] == tx_id)
                    m = lookup_merchant(desc)
                    fallback = m["domain"] if m else None
                    await db.execute(
                        "UPDATE transactions SET merchant_domain = ? WHERE id = ?",
                        (fallback, tx_id),
                    )

        await db.commit()

        # 3. If a domain was set, cache its logo locally so future page loads
        #    are served from the local static directory, not the CDN.
        if data.domain:
            pk_cursor = await db.execute(
                "SELECT value FROM settings WHERE key = 'logodev_publishable_key'"
            )
            pk_row = await pk_cursor.fetchone()
            pk = (pk_row[0] or "").strip() if pk_row else ""
            if pk:
                async with httpx.AsyncClient() as client:
                    await fetch_and_cache_logo(data.domain, pk, client)

        return {"ok": True, "transactions_updated": len(matching_ids)}
    finally:
        await db.close()


@router.patch("/api/transactions/{tx_id}")
async def update_transaction_category(tx_id: int, data: TransactionCategoryUpdate):
    db = await get_db()
    try:
        await db.execute(
            "UPDATE transactions SET category_id = ?, manual_category = 1 WHERE id = ?",
            (data.category_id, tx_id),
        )
        await db.commit()
        return {"ok": True}
    finally:
        await db.close()


@router.delete("/api/transactions/{tx_id}")
async def delete_transaction(tx_id: int):
    db = await get_db()
    try:
        await db.execute("DELETE FROM transactions WHERE id = ?", (tx_id,))
        await db.commit()
        return {"ok": True}
    finally:
        await db.close()


@router.post("/api/transactions/recategorize")
async def recategorize():
    db = await get_db()
    try:
        count = await recategorize_all(db)
        return {"categorized": count}
    finally:
        await db.close()
