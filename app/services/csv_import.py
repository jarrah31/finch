import csv
import hashlib
import io
from datetime import datetime
from typing import Optional

import aiosqlite

from .mcc import extract_mcc
from ..data.merchant_dictionary import lookup_merchant, normalize_description


def parse_date(date_str: str, fmt: str = "%d %b %Y") -> str:
    """Parse date string to ISO 8601 format."""
    return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")


def compute_hash(date: str, description: str, amount: float, account_number: str) -> str:
    """Compute SHA-256 hash for duplicate detection."""
    raw = f"{date}|{description}|{amount}|{account_number}"
    return hashlib.sha256(raw.encode()).hexdigest()


async def import_csv(
    db: aiosqlite.Connection,
    file_content: str,
    date_format: str = "%d %b %Y",
    column_mapping: Optional[dict] = None,
) -> dict:
    """Import transactions from CSV content. Returns counts of inserted/skipped/errors.

    column_mapping (optional) maps app field names to CSV column headers:
      date, description, amount, debit, credit, type, balance,
      account_name, account_number
    When debit/credit are set, value = credit - debit.  Falls back to 'amount'
    column (default 'Value') when neither debit nor credit is mapped.
    """
    inserted = 0
    skipped = 0
    errors = []
    inserted_dates: list[str] = []

    def _col(field: str, default: str) -> str:
        """Return the mapped column header for a field, or the default."""
        if column_mapping:
            v = column_mapping.get(field)
            if v:
                return v
        return default

    # Load all saved merchant overrides up-front so we can apply them during import
    cursor = await db.execute("SELECT description_key, domain FROM merchant_overrides")
    override_map: dict[str, str] = {r[0]: r[1] for r in await cursor.fetchall()}

    reader = csv.DictReader(io.StringIO(file_content))

    for i, row in enumerate(reader, start=2):
        try:
            date_str = row[_col("date", "Date")].strip()
            tx_type = (row.get(_col("type", "Type")) or "").strip()
            description = row[_col("description", "Description")].strip()
            # Remove quotes that may wrap description
            if description.startswith('"') and description.endswith('"'):
                description = description[1:-1]

            # Amount: split debit/credit columns take priority over single amount column
            debit_col = column_mapping.get("debit") if column_mapping else None
            credit_col = column_mapping.get("credit") if column_mapping else None
            if debit_col or credit_col:
                debit_raw = (row.get(debit_col) or "").strip() if debit_col else ""
                credit_raw = (row.get(credit_col) or "").strip() if credit_col else ""
                debit = float(debit_raw.replace(",", "")) if debit_raw else 0.0
                credit = float(credit_raw.replace(",", "")) if credit_raw else 0.0
                value = credit - debit
            else:
                value = float(row[_col("amount", "Value")].replace(",", ""))

            balance_str = (row.get(_col("balance", "Balance")) or "").strip()
            balance = float(balance_str.replace(",", "")) if balance_str else None
            account_name = (row.get(_col("account_name", "Account Name")) or "Unknown").strip()
            account_number = (row.get(_col("account_number", "Account Number")) or "unknown").strip()

            iso_date = parse_date(date_str, date_format)

            # Compute hash BEFORE MCC extraction so it always matches the raw CSV description
            import_hash = compute_hash(iso_date, description, value, account_number)

            # Extract MCC prefix (e.g. "5411 SAINSBURYS" → mcc="5411", desc="SAINSBURYS")
            mcc_code, description = extract_mcc(description)

            # Resolve merchant domain: saved override first, then dictionary
            desc_key = normalize_description(description)
            if desc_key in override_map:
                merchant_domain = override_map[desc_key]
            else:
                m = lookup_merchant(description)
                merchant_domain = m["domain"] if m else None

            # Get or create account
            cursor = await db.execute(
                "SELECT id FROM accounts WHERE account_number = ?",
                (account_number,),
            )
            account_row = await cursor.fetchone()
            if account_row:
                account_id = account_row[0]
            else:
                cursor = await db.execute(
                    "INSERT INTO accounts (account_name, account_number) VALUES (?, ?)",
                    (account_name, account_number),
                )
                account_id = cursor.lastrowid

            try:
                await db.execute(
                    """INSERT OR IGNORE INTO transactions
                       (account_id, date, type, description, amount, balance, mcc_code, merchant_domain, import_hash)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (account_id, iso_date, tx_type, description, value, balance, mcc_code, merchant_domain, import_hash),
                )
                if db.total_changes:
                    # Check if the row was actually inserted (not ignored)
                    cursor = await db.execute(
                        "SELECT id FROM transactions WHERE import_hash = ?",
                        (import_hash,),
                    )
                    row_check = await cursor.fetchone()
                    if row_check:
                        inserted += 1
                        inserted_dates.append(iso_date)
                    else:
                        skipped += 1
                else:
                    skipped += 1
            except aiosqlite.IntegrityError:
                skipped += 1

        except Exception as e:
            errors.append(f"Row {i}: {str(e)}")

    await db.commit()
    return {
        "inserted": inserted,
        "skipped": skipped,
        "errors": errors,
        "min_date": min(inserted_dates) if inserted_dates else None,
        "max_date": max(inserted_dates) if inserted_dates else None,
    }
