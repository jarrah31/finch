import os
import json
import tempfile
import aiosqlite
from pathlib import Path
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from fastapi.responses import FileResponse
import httpx
from ..database import get_db, DB_PATH
from ..models import SettingsUpdate
from ..services.pay_periods import recompute_pay_periods
from ..services.mcc import backfill_mcc_data
from ..services.merchant import backfill_merchant_domains
from ..services.logos import backfill_logos

router = APIRouter()


@router.get("/api/settings")
async def get_settings():
    db = await get_db()
    try:
        cursor = await db.execute("SELECT key, value FROM settings")
        return {row["key"]: row["value"] for row in await cursor.fetchall()}
    finally:
        await db.close()


@router.patch("/api/settings")
async def update_settings(data: SettingsUpdate):
    db = await get_db()
    try:
        recompute = False
        if data.pay_day_keyword is not None:
            await db.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('pay_day_keyword', ?)",
                (data.pay_day_keyword,),
            )
            recompute = True
        if data.csv_date_format is not None:
            await db.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('csv_date_format', ?)",
                (data.csv_date_format,),
            )
        if data.currency_symbol is not None:
            await db.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('currency_symbol', ?)",
                (data.currency_symbol,),
            )
        if data.income_keywords is not None:
            await db.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('income_keywords', ?)",
                (json.dumps(data.income_keywords),),
            )
        if data.logodev_publishable_key is not None:
            await db.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('logodev_publishable_key', ?)",
                (data.logodev_publishable_key,),
            )
        if data.logodev_secret_key is not None:
            await db.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('logodev_secret_key', ?)",
                (data.logodev_secret_key,),
            )
        if data.csv_column_mapping is not None:
            await db.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('csv_column_mapping', ?)",
                (json.dumps(data.csv_column_mapping),),
            )
        await db.commit()

        if recompute:
            await recompute_pay_periods(db)

        return {"ok": True}
    finally:
        await db.close()


@router.get("/api/export/database")
async def export_database():
    return FileResponse(
        str(DB_PATH),
        media_type="application/x-sqlite3",
        filename="finance.db",
    )


@router.post("/api/import/database")
async def import_database(file: UploadFile = File(...)):
    content = await file.read()

    # Validate SQLite magic bytes
    if not content.startswith(b"SQLite format 3\x00"):
        raise HTTPException(status_code=400, detail="Not a valid SQLite database file")

    # Write to a temp file in the same directory so os.replace is atomic
    tmp_path = None
    try:
        fd, tmp_str = tempfile.mkstemp(suffix=".db", dir=str(DB_PATH.parent))
        tmp_path = Path(tmp_str)
        os.close(fd)
        tmp_path.write_bytes(content)

        # Validate the uploaded DB has expected tables
        test_db = await aiosqlite.connect(str(tmp_path))
        try:
            cursor = await test_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('transactions','categories','categorization_rules','settings')"
            )
            tables = {row[0] for row in await cursor.fetchall()}
        finally:
            await test_db.close()

        required = {"transactions", "categories", "categorization_rules"}
        if not required.issubset(tables):
            raise HTTPException(
                status_code=400,
                detail="File does not appear to be a valid Finch database backup",
            )

        # Atomically replace the live database
        os.replace(str(tmp_path), str(DB_PATH))
        tmp_path = None  # no cleanup needed – file was moved
        return {"ok": True}

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Import failed: {exc}")
    finally:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


@router.post("/api/logodev/test")
async def test_logodev_keys():
    """
    Test both Logo.dev keys:
      - Secret key (sk_…): calls the Brand Search API for "amazon"
      - Publishable key (pk_…): returned so the browser can test CDN image load

    Returns { ok, pk, sk_ok, domain, logo_url, error }
    """
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT key, value FROM settings WHERE key IN ('logodev_publishable_key', 'logodev_secret_key')"
        )
        settings = {row["key"]: row["value"] for row in await cursor.fetchall()}
    finally:
        await db.close()

    pk = (settings.get("logodev_publishable_key") or "").strip()
    sk = (settings.get("logodev_secret_key") or "").strip()

    if not pk and not sk:
        raise HTTPException(status_code=400, detail="No Logo.dev keys configured — save them first.")

    result: dict = {"ok": False, "pk": pk, "sk_ok": False, "domain": None, "logo_url": None, "error": None}

    # Test secret key via Brand Search API
    if sk:
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                resp = await client.get(
                    "https://api.logo.dev/search",
                    params={"q": "amazon", "token": sk},
                    headers={"Authorization": f"Bearer {sk}"},
                )
            if resp.status_code == 200:
                hits = resp.json()
                if hits:
                    result["sk_ok"] = True
                    result["domain"] = hits[0].get("domain")
                else:
                    result["error"] = "Secret key accepted but search returned no results"
            elif resp.status_code in (401, 403):
                result["error"] = "Secret key rejected (401/403) — check the sk_… value"
            else:
                result["error"] = f"Brand Search API returned HTTP {resp.status_code}"
        except httpx.TimeoutException:
            result["error"] = "Request timed out — check your internet connection"
        except Exception as exc:
            result["error"] = f"Request failed: {exc}"

    # Build CDN logo URL using publishable key + domain from search
    domain = result["domain"] or "amazon.co.uk"
    if pk:
        result["logo_url"] = f"https://img.logo.dev/{domain}?token={pk}&size=64&format=png"

    result["ok"] = result["sk_ok"] or bool(pk)
    return result


@router.get("/api/logodev/search")
async def search_logodev(q: str = Query(..., min_length=1)):
    """
    Search the Logo.dev Brand Search API for matching brands.
    Returns [{name, domain}] up to 10 results.
    """
    db = await get_db()
    try:
        cursor = await db.execute("SELECT value FROM settings WHERE key = 'logodev_secret_key'")
        row = await cursor.fetchone()
        sk = (row[0] or "").strip() if row else ""
    finally:
        await db.close()

    if not sk:
        raise HTTPException(status_code=400, detail="Logo.dev secret key not configured — add it in Settings.")

    try:
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.get(
                "https://api.logo.dev/search",
                params={"q": q, "token": sk},
                headers={"Authorization": f"Bearer {sk}"},
            )
        if resp.status_code == 200:
            hits = resp.json()
            return {
                "results": [
                    {"name": h.get("name", ""), "domain": h.get("domain", "")}
                    for h in hits[:10]
                    if h.get("domain")
                ]
            }
        elif resp.status_code in (401, 403):
            raise HTTPException(status_code=400, detail="Secret key rejected — check the sk_… value in Settings.")
        else:
            raise HTTPException(status_code=502, detail=f"Logo.dev API returned HTTP {resp.status_code}")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Logo.dev API timed out")


@router.post("/api/mcc/backfill")
async def run_mcc_backfill():
    """
    One-time migration: extract MCC prefixes from existing transaction descriptions
    and store them in the mcc_code column (stripping the prefix from description).
    Safe to run multiple times — only processes rows where mcc_code IS NULL.
    """
    db = await get_db()
    try:
        result = await backfill_mcc_data(db)
        return result
    finally:
        await db.close()


@router.post("/api/merchant/backfill")
async def run_merchant_backfill():
    """
    Populate merchant_domain on existing transactions that don't have one yet.
    Applies saved manual overrides first, then falls back to the dictionary.
    Safe to run multiple times — only processes rows where merchant_domain IS NULL.
    """
    db = await get_db()
    try:
        result = await backfill_merchant_domains(db)
        return result
    finally:
        await db.close()


@router.post("/api/logos/backfill")
async def run_logos_backfill():
    """
    Download and cache logos locally for every distinct merchant_domain in
    the transactions table.  Already-cached files are skipped.
    Requires logodev_publishable_key to be saved in settings.
    """
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT value FROM settings WHERE key = 'logodev_publishable_key'"
        )
        row = await cursor.fetchone()
        pk = (row[0] or "").strip() if row else ""
        if not pk:
            return {"error": "logodev_publishable_key not configured", "fetched": 0, "skipped": 0, "failed": 0}
        result = await backfill_logos(db, pk)
        return result
    finally:
        await db.close()


@router.delete("/api/reset")
async def reset_all_data():
    db = await get_db()
    try:
        await db.execute("DELETE FROM transactions")
        await db.execute("DELETE FROM categorization_rules")
        await db.execute("DELETE FROM categories")
        await db.execute("DELETE FROM pay_periods")
        await db.execute("DELETE FROM accounts")
        await db.commit()
        return {"ok": True}
    finally:
        await db.close()
