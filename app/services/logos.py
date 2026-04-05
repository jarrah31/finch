"""
Local logo caching service.

Logos are downloaded from Logo.dev's CDN once and stored in
app/static/logos/{domain}.png so they are served from the local server.
The CDN URL is used only as a fallback when no local file exists.
"""
from pathlib import Path

import aiosqlite
import httpx

LOGOS_DIR = Path(__file__).parent.parent / "static" / "logos"


def logo_is_cached(domain: str) -> bool:
    return (LOGOS_DIR / f"{domain}.png").exists()


async def fetch_and_cache_logo(
    domain: str, pk: str, client: httpx.AsyncClient
) -> bool:
    """
    Fetch the logo for *domain* from Logo.dev and save it locally.
    Returns True on success, False if the fetch failed or the response
    was not an image (e.g. a 1×1 placeholder).
    Skips the network call if a cached file already exists.
    """
    path = LOGOS_DIR / f"{domain}.png"
    if path.exists():
        return True

    url = f"https://img.logo.dev/{domain}?token={pk}&size=64&format=png"
    try:
        resp = await client.get(url, timeout=10.0, follow_redirects=True)
        content_type = resp.headers.get("content-type", "")
        if resp.status_code == 200 and content_type.startswith("image/"):
            LOGOS_DIR.mkdir(parents=True, exist_ok=True)
            path.write_bytes(resp.content)
            return True
    except Exception:
        pass
    return False


async def backfill_logos(db: aiosqlite.Connection, pk: str) -> dict:
    """
    Download and cache logos for every distinct merchant_domain stored on
    transactions.  Already-cached domains are skipped.
    Returns {"fetched": N, "skipped": N, "failed": N}.
    """
    LOGOS_DIR.mkdir(parents=True, exist_ok=True)

    cursor = await db.execute(
        "SELECT DISTINCT merchant_domain FROM transactions"
        " WHERE merchant_domain IS NOT NULL ORDER BY merchant_domain"
    )
    domains = [row[0] for row in await cursor.fetchall()]

    fetched = skipped = failed = 0
    async with httpx.AsyncClient() as client:
        for domain in domains:
            if logo_is_cached(domain):
                skipped += 1
                continue
            ok = await fetch_and_cache_logo(domain, pk, client)
            if ok:
                fetched += 1
            else:
                failed += 1

    return {"fetched": fetched, "skipped": skipped, "failed": failed}
