"""
MCC (Merchant Category Code) service.

Responsibilities:
  - Load the embedded mcc_codes.json lookup table once at import time
  - Extract a 4-digit MCC prefix from a raw transaction description
  - Map an MCC code to a human-readable category name
  - Auto-create (or find) an MCC-sourced category row in the DB
  - Apply an MCC tag to a transaction when a rule has already matched
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

import aiosqlite

# ---------------------------------------------------------------------------
# Load MCC lookup table (once, at module import)
# ---------------------------------------------------------------------------

_DATA_FILE = Path(__file__).parent.parent / "data" / "mcc_codes.json"

# Build {mcc_code: friendly_description} from the JSON
_MCC_MAP: dict[str, str] = {}

try:
    _raw = json.loads(_DATA_FILE.read_text())
    for entry in _raw:
        code = str(entry.get("mcc", "")).zfill(4)
        # Prefer edited_description, fall back to combined_description
        desc = (
            entry.get("edited_description")
            or entry.get("combined_description")
            or ""
        ).strip()
        if code and desc:
            _MCC_MAP[code] = desc
except Exception:
    pass  # If the file is missing, enrichment is simply unavailable

# ---------------------------------------------------------------------------
# Regexes for prefix detection
# ---------------------------------------------------------------------------

# Matches any leading 4-digit block: "5411 SAINSBURYS" or "5386 15MAR26 ..."
_MCC_RE = re.compile(r"^(\d{4})\s+(.*)", re.DOTALL)

# Detects a bank-format date immediately after the 4-digit prefix.
# Format: DDMMMYY (e.g. "15MAR26", "03JAN25") — used by several UK banks
# to embed an internal transaction reference code before the merchant name.
# When this pattern is present the 4-digit prefix is a bank code, not an MCC.
_BANK_DATE_RE = re.compile(r"^\d{2}[A-Z]{3}\d{2}\b")


def extract_mcc(description: str) -> tuple[Optional[str], str]:
    """
    Strip any leading 4-digit bank/MCC prefix from *description* and return
    ``(mcc_code, cleaned_description)``.

    Two cases are handled:

    1. **Bank-format prefix** — the 4-digit code is immediately followed by a
       date in ``DDMMMYY`` format (e.g. ``"5386 15MAR26 CD , SCREWFIX..."``).
       These are proprietary bank reference codes, not MCC codes.  The prefix
       is stripped so the description is cleaner, but ``mcc_code`` is returned
       as ``None`` (no category inference).

    2. **Real MCC prefix** — the 4-digit code is a known ISO 18245 category
       code *outside* the 3000–3999 range (which contains company-specific
       airline/hotel entries that cause false positives).  Both the prefix is
       stripped *and* the MCC code is returned for category assignment.

    If neither condition matches the description is returned unchanged.

    Examples
    --------
    >>> extract_mcc("5386 15MAR26 CD , SCREWFIX DIRECT , GB")
    (None, '15MAR26 CD , SCREWFIX DIRECT , GB')
    >>> extract_mcc("5411 SAINSBURYS S/MKT")
    ('5411', 'SAINSBURYS S/MKT')
    >>> extract_mcc("DIRECT DEBIT PAYMENT")
    (None, 'DIRECT DEBIT PAYMENT')
    """
    m = _MCC_RE.match(description.strip())
    if not m:
        return None, description

    code, rest = m.group(1), m.group(2).strip()

    # Case 1: bank-format prefix (DDMMMYY date follows) — strip but no MCC
    if _BANK_DATE_RE.match(rest):
        return None, rest

    # Case 2: real MCC category code (known, not the company-specific 3xxx range)
    if code in _MCC_MAP and not ("3000" <= code <= "3999"):
        return code, rest

    # Unknown prefix that isn't bank-format — leave description unchanged
    return None, description


def mcc_label(mcc_code: str) -> Optional[str]:
    """Return the human-readable label for an MCC code, or None if unknown."""
    return _MCC_MAP.get(str(mcc_code).zfill(4))


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

async def get_or_create_mcc_category(
    db: aiosqlite.Connection, mcc_code: str
) -> Optional[int]:
    """
    Return the category_id for the MCC-derived category, creating it if
    it doesn't already exist.  Returns None if the MCC code is unknown.
    """
    label = mcc_label(mcc_code)
    if not label:
        return None

    # Shorten very long MCC descriptions for display
    name = _shorten_mcc_label(label)

    cursor = await db.execute(
        "SELECT id FROM categories WHERE name = ? AND parent_id IS NULL",
        (name,),
    )
    row = await cursor.fetchone()
    if row:
        return row[0]

    # Create it
    cursor = await db.execute(
        "INSERT INTO categories (name, source) VALUES (?, 'mcc')",
        (name,),
    )
    await db.commit()
    return cursor.lastrowid


async def apply_mcc_tag(
    db: aiosqlite.Connection, transaction_id: int, mcc_code: str
) -> None:
    """
    When a transaction already has a rule-matched category, store the MCC
    category name as a tag instead of replacing the category.
    """
    label = mcc_label(mcc_code)
    if not label:
        return

    tag_name = _shorten_mcc_label(label)

    # Find or create the tag
    cursor = await db.execute(
        "SELECT id FROM tags WHERE name = ?", (tag_name,)
    )
    row = await cursor.fetchone()
    if row:
        tag_id = row[0]
    else:
        cursor = await db.execute(
            "INSERT INTO tags (name, source) VALUES (?, 'mcc')",
            (tag_name,),
        )
        await db.commit()
        tag_id = cursor.lastrowid

    # Attach to transaction (ignore duplicate)
    await db.execute(
        "INSERT OR IGNORE INTO transaction_tags (transaction_id, tag_id) VALUES (?, ?)",
        (transaction_id, tag_id),
    )
    await db.commit()


# ---------------------------------------------------------------------------
# Backfill helpers
# ---------------------------------------------------------------------------


async def backfill_mcc_data(db: aiosqlite.Connection) -> dict:
    """
    One-time migration for existing transactions imported before MCC extraction
    was added to the CSV importer.

    For each transaction whose mcc_code column is NULL, call extract_mcc():
      - If the description changed (prefix was stripped), update it.
      - If a real MCC code was found, store it in mcc_code.
      - Bank-format prefixes (DDMMMYY date follows) are stripped for cleanliness
        but mcc_code is left NULL (they are not real MCC codes).

    Safe to run multiple times — only touches rows where mcc_code IS NULL.

    Returns counts: {"updated": N, "skipped": M}
    """
    cursor = await db.execute(
        "SELECT id, description FROM transactions WHERE mcc_code IS NULL"
    )
    rows = await cursor.fetchall()

    updated = 0
    for row in rows:
        tx_id = row[0]
        description = row[1]
        code, cleaned = extract_mcc(description)
        if cleaned != description:
            # Description was changed (prefix stripped) — always write the cleaned version.
            # code may be None (bank prefix) or a real MCC string.
            await db.execute(
                "UPDATE transactions SET description = ?, mcc_code = ? WHERE id = ?",
                (cleaned, code, tx_id),
            )
            updated += 1

    await db.commit()
    return {"updated": updated, "skipped": len(rows) - updated}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Noise phrases to strip from long MCC labels for compact display
_STRIP_PHRASES = [
    " – Sales and Installation",
    " – Sales, Service, Installation",
    ", Supermarkets",
    ", Restaurants",
    " (Not Elsewhere Classified)",
    " (Automated Fuel Dispensers)",
    " and Eating Places",
]


def _shorten_mcc_label(label: str) -> str:
    """Trim verbose MCC descriptions to a cleaner short name."""
    result = label
    for phrase in _STRIP_PHRASES:
        result = result.replace(phrase, "")
    # Collapse multiple whitespace
    result = re.sub(r"\s{2,}", " ", result).strip()
    # Truncate at first em-dash or double-dash if still long
    for sep in (" – ", " - ", ", "):
        if len(result) > 40 and sep in result:
            result = result.split(sep)[0].strip()
            break
    return result
