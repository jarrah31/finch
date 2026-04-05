import json
from datetime import datetime, timedelta
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from ..database import get_db
from ..services.categorizer import get_rules

router = APIRouter()


def _fmt_period_date(d: str, is_end: bool = False) -> str:
    """Format a period boundary date for display.
    For end dates, subtract 1 day so '26 Feb' displays as '25 Feb',
    reflecting the last day transactions are actually included (queries use <).
    """
    if d == "9999-12-31":
        return "present"
    dt = datetime.strptime(d, "%Y-%m-%d")
    if is_end:
        dt -= timedelta(days=1)
    return dt.strftime("%-d %b %Y")


@router.get("/")
async def index():
    return RedirectResponse(url="/analysis", status_code=302)


@router.get("/transactions", response_class=HTMLResponse)
async def transactions_page(request: Request):
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM categories ORDER BY name")
        categories = [dict(row) for row in await cursor.fetchall()]

        cursor = await db.execute("SELECT * FROM accounts ORDER BY account_name")
        accounts = [dict(row) for row in await cursor.fetchall()]

        cursor = await db.execute("SELECT * FROM pay_periods ORDER BY start_date DESC")
        periods = [dict(row) for row in await cursor.fetchall()]

        cursor = await db.execute(
            "SELECT value FROM settings WHERE key = 'logodev_publishable_key'"
        )
        pk_row = await cursor.fetchone()
        logodev_pk = (pk_row[0] or "").strip() if pk_row else ""

        return request.app.state.templates.TemplateResponse(
            "transactions.html",
            {
                "request": request,
                "categories": categories,
                "accounts": accounts,
                "periods": periods,
                "logodev_pk": logodev_pk,
            },
        )
    finally:
        await db.close()


@router.get("/analysis", response_class=HTMLResponse)
async def analysis_page(request: Request):
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM pay_periods ORDER BY start_date DESC")
        periods = [dict(row) for row in await cursor.fetchall()]
        for p in periods:
            p["option_label"] = f"{_fmt_period_date(p['start_date'])} – {_fmt_period_date(p['end_date'], is_end=True)}"

        return request.app.state.templates.TemplateResponse(
            "analysis.html",
            {"request": request, "periods": periods},
        )
    finally:
        await db.close()


@router.get("/rules", response_class=HTMLResponse)
async def rules_page(request: Request):
    db = await get_db()
    try:
        cursor = await db.execute(
            """SELECT c.*, COUNT(cr.id) as rule_count, p.name as parent_name
               FROM categories c
               LEFT JOIN categories p ON p.id = c.parent_id
               LEFT JOIN categorization_rules cr ON cr.category_id = c.id
               GROUP BY c.id
               ORDER BY COALESCE(c.parent_id, c.id), c.parent_id IS NOT NULL, c.name"""
        )
        categories = [dict(row) for row in await cursor.fetchall()]

        rules = await get_rules(db)

        return request.app.state.templates.TemplateResponse(
            "rules.html",
            {"request": request, "categories": categories, "rules": rules},
        )
    finally:
        await db.close()


@router.get("/categories", response_class=HTMLResponse)
async def categories_page(request: Request):
    db = await get_db()
    try:
        cursor = await db.execute(
            """SELECT c.*, COUNT(cr.id) as rule_count, p.name as parent_name
               FROM categories c
               LEFT JOIN categories p ON p.id = c.parent_id
               LEFT JOIN categorization_rules cr ON cr.category_id = c.id
               GROUP BY c.id
               ORDER BY COALESCE(c.parent_id, c.id), c.parent_id IS NOT NULL, c.name"""
        )
        categories = [dict(row) for row in await cursor.fetchall()]

        cursor2 = await db.execute(
            """SELECT id, category_id, keyword, keywords, subscription_period,
                      match_amounts, comment
               FROM categorization_rules
               ORDER BY id"""
        )
        cat_rules = [dict(row) for row in await cursor2.fetchall()]

        return request.app.state.templates.TemplateResponse(
            "categories.html",
            {"request": request, "categories": categories, "cat_rules": cat_rules},
        )
    finally:
        await db.close()


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    db = await get_db()
    try:
        cursor = await db.execute("SELECT key, value FROM settings")
        settings = {row["key"]: row["value"] for row in await cursor.fetchall()}

        # Parse JSON array/object fields
        raw_income = settings.get("income_keywords")
        settings["income_keywords"] = json.loads(raw_income) if raw_income else []

        raw_mapping = settings.get("csv_column_mapping")
        settings["csv_column_mapping"] = json.loads(raw_mapping) if raw_mapping else None

        # Ensure logo.dev keys always present (may be absent on older DBs)
        settings.setdefault("logodev_publishable_key", "")
        settings.setdefault("logodev_secret_key", "")

        cursor = await db.execute("SELECT COUNT(*) as count FROM transactions")
        tx_count = (await cursor.fetchone())["count"]

        cursor = await db.execute("SELECT COUNT(*) as count FROM categories")
        cat_count = (await cursor.fetchone())["count"]

        return request.app.state.templates.TemplateResponse(
            "settings.html",
            {
                "request": request,
                "settings": settings,
                "tx_count": tx_count,
                "cat_count": cat_count,
            },
        )
    finally:
        await db.close()
