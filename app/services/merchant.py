"""
Merchant domain resolution and backfill.
"""
import aiosqlite
from ..data.merchant_dictionary import lookup_merchant, normalize_description


async def backfill_merchant_domains(db: aiosqlite.Connection) -> dict:
    """
    Populate merchant_domain on every transaction that doesn't have one yet.

    Priority order:
      1. Manual override from merchant_overrides table
      2. Automatic match from the merchant dictionary

    Returns {"updated": N, "skipped": M}
    """
    # Load all saved overrides once
    cursor = await db.execute("SELECT description_key, domain FROM merchant_overrides")
    override_map: dict[str, str] = {r[0]: r[1] for r in await cursor.fetchall()}

    # Only process rows that have no merchant_domain set
    cursor = await db.execute(
        "SELECT id, description FROM transactions WHERE merchant_domain IS NULL"
    )
    rows = await cursor.fetchall()

    updated = 0
    for row in rows:
        tx_id, description = row[0], row[1]
        key = normalize_description(description)
        if key in override_map:
            domain = override_map[key]
        else:
            m = lookup_merchant(description)
            domain = m["domain"] if m else None

        if domain:
            await db.execute(
                "UPDATE transactions SET merchant_domain = ? WHERE id = ?",
                (domain, tx_id),
            )
            updated += 1

    await db.commit()
    return {"updated": updated, "skipped": len(rows) - updated}
